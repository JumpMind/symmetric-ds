/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.service.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.io.ThresholdFileWriter;
import org.jumpmind.symmetric.load.IBatchListener;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.load.IDataLoader;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.IDataLoaderStatistics;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.RegistrationNotOpenException;
import org.jumpmind.symmetric.service.RegistrationRequiredException;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticNameConstants;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.transport.TransportException;
import org.jumpmind.symmetric.transport.file.FileIncomingTransport;
import org.jumpmind.symmetric.transport.internal.InternalIncomingTransport;
import org.jumpmind.symmetric.util.AppUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;

public class DataLoaderService extends AbstractService implements IDataLoaderService, BeanFactoryAware {

    private IDbDialect dbDialect;

    private IIncomingBatchService incomingBatchService;

    private ITransportManager transportManager;

    private BeanFactory beanFactory;

    private List<IDataLoaderFilter> filters;

    private IStatisticManager statisticManager;

    private INodeService nodeService;

    private Map<String, IColumnFilter> columnFilters = new HashMap<String, IColumnFilter>();

    private List<IBatchListener> batchListeners;

    /**
     * Connect to the remote node and pull data. The acknowledgment of commit/error status is sent separately after the
     * data is processed.
     */
    public boolean loadData(Node remote, Node local) throws IOException {
        boolean wasWorkDone = false;
        try {
            List<IncomingBatch> list = loadDataAndReturnBatches(transportManager.getPullTransport(remote, local));
            if (list.size() > 0) {
                sendAck(remote, local, list);
                wasWorkDone = true;
            }
        } catch (RegistrationRequiredException e) {
            log.warn("RegistrationLost");
            loadData(transportManager.getRegisterTransport(local));
            nodeService.findIdentity(false);
            wasWorkDone = true;
        } catch (MalformedURLException e) {
            log.error("URLConnectingFailure", e.getMessage());
        }
        return wasWorkDone;
    }

    /**
     * Try a configured number of times to get the ACK through.
     */
    private void sendAck(Node remote, Node local, List<IncomingBatch> list) throws IOException {
        Exception error = null;
        boolean sendAck = false;
        int numberOfStatusSendRetries = parameterService.getInt(ParameterConstants.DATA_LOADER_NUM_OF_ACK_RETRIES);
        for (int i = 0; i < numberOfStatusSendRetries && !sendAck; i++) {
            try {
                sendAck = transportManager.sendAcknowledgement(remote, list, local);
            } catch (IOException ex) {
                log.warn("AckSendingFailed", (i + 1), ex.getMessage());
                error = ex;
            } catch (RuntimeException ex) {
                log.warn("AckSendingFailed", (i + 1), ex.getMessage());
                error = ex;
            }
            if (!sendAck) {
                if (i < numberOfStatusSendRetries - 1) {
                    AppUtils.sleep(parameterService.getLong(ParameterConstants.DATA_LOADER_TIME_BETWEEN_ACK_RETRIES));
                } else if (error instanceof RuntimeException) {
                    throw (RuntimeException) error;
                } else if (error instanceof IOException) {
                    throw (IOException) error;
                }
            }
        }
    }

    public IDataLoader openDataLoader(BufferedReader reader) throws IOException {
        IDataLoader dataLoader = (IDataLoader) beanFactory.getBean(Constants.DATALOADER);
        dataLoader.open(reader, filters, columnFilters);
        return dataLoader;
    }

    public IDataLoaderStatistics loadDataBatch(String batchData) throws IOException {
        BufferedReader reader = new BufferedReader(new StringReader(batchData));
        IDataLoader dataLoader = openDataLoader(reader);
        IDataLoaderStatistics stats = null;
        try {
            while (dataLoader.hasNext()) {
                dataLoader.load();
                IncomingBatch history = new IncomingBatch(dataLoader.getContext());
                history.setValues(dataLoader.getStatistics(), true);
                fireBatchComplete(dataLoader, history);
            }
        } finally {
            stats = dataLoader.getStatistics();
            dataLoader.close();
        }
        return stats;
    }

    /**
     * Load database from input stream and return a list of batch statuses. This is used for a pull request that
     * responds with data, and the acknowledgment is sent later.
     */
    protected List<IncomingBatch> loadDataAndReturnBatches(IIncomingTransport transport) throws IOException {

        List<IncomingBatch> list = new ArrayList<IncomingBatch>();
        IncomingBatch batch = null;
        IDataLoader dataLoader = null;
        try {
            long totalNetworkMillis = System.currentTimeMillis();
            if (parameterService.is(ParameterConstants.STREAM_TO_FILE_ENABLED)) {
                transport = writeToFile(transport);
                totalNetworkMillis = System.currentTimeMillis() - totalNetworkMillis;
            }
            dataLoader = openDataLoader(transport.open());
            while (dataLoader.hasNext()) {
                batch = new IncomingBatch(dataLoader.getContext());
                list.add(batch);
                loadBatch(dataLoader, batch);
                batch = null;
            }

            if (parameterService.is(ParameterConstants.STREAM_TO_FILE_ENABLED)) {
                estimateNetworkMillis(list, totalNetworkMillis);
            }

            for (IncomingBatch incomingBatch : list) {
                if (incomingBatch.isPersistable()) {
                    incomingBatchService.updateIncomingBatch(incomingBatch);
                }
            }

        } catch (RegistrationRequiredException ex) {
            throw ex;
        } catch (ConnectException ex) {
            statisticManager.getStatistic(StatisticNameConstants.INCOMING_TRANSPORT_CONNECT_ERROR_COUNT).increment();
            throw ex;
        } catch (UnknownHostException ex) {
            log.warn("TransportFailedConnectionUnavailable", ex.getMessage());
            statisticManager.getStatistic(StatisticNameConstants.INCOMING_TRANSPORT_CONNECT_ERROR_COUNT).increment();
            throw ex;
        } catch (RegistrationNotOpenException ex) {
            log.warn("RegistrationFailed");
        } catch (ConnectionRejectedException ex) {
            log.warn("TransportFailedConnectionBusy");
            statisticManager.getStatistic(StatisticNameConstants.INCOMING_TRANSPORT_REJECTED_COUNT).increment();
            throw ex;
        } catch (AuthenticationException ex) {
            log.warn("AuthenticationFailed");
        } catch (Throwable e) {
            if (dataLoader != null && dataLoader.getContext().getBatchId() > 0) {
                batch = new IncomingBatch(dataLoader.getContext());
            }
            if (dataLoader != null && batch != null) {
                if (e instanceof IOException || e instanceof TransportException) {
                    log.warn("BatchLoadingFailed", batch.getNodeBatchId(), e.getMessage());
                    batch.setSqlMessage(e.getMessage());
                    statisticManager.getStatistic(StatisticNameConstants.INCOMING_TRANSPORT_ERROR_COUNT).increment();
                } else {
                    log.error("BatchLoadingFailed", batch.getNodeBatchId(), e.getMessage(), e);
                    SQLException se = unwrapSqlException(e);
                    if (se != null) {
                        statisticManager.getStatistic(StatisticNameConstants.INCOMING_DATABASE_ERROR_COUNT).increment();
                        batch.setSqlState(se.getSQLState());
                        batch.setSqlCode(se.getErrorCode());
                        batch.setSqlMessage(se.getMessage());
                    } else {
                        batch.setSqlMessage(e.getMessage());
                        statisticManager.getStatistic(StatisticNameConstants.INCOMING_OTHER_ERROR_COUNT).increment();
                    }
                }
                batch.setValues(dataLoader.getStatistics(), false);
                handleBatchError(batch);
            } else {
                if (e instanceof IOException) {
                    log.error("BatchReadingFailed", e.getMessage());
                } else {
                    log.error("BatchParsingFailed", e);
                }
            }
        } finally {
            if (dataLoader != null) {
                dataLoader.close();
            }
            transport.close();
            recordStatistics(list);
        }
        return list;
    }

    protected void estimateNetworkMillis(List<IncomingBatch> list, long totalNetworkMillis) {
        long totalNumberOfBytes = 0;
        for (IncomingBatch incomingBatch : list) {
            totalNumberOfBytes += incomingBatch.getByteCount();
        }
        for (IncomingBatch incomingBatch : list) {
            if (totalNumberOfBytes > 0) {
                double ratio = (double) incomingBatch.getByteCount() / (double) totalNumberOfBytes;
                incomingBatch.setNetworkMillis((long) (totalNetworkMillis * ratio));
            }
        }
    }

    protected IIncomingTransport writeToFile(IIncomingTransport transport) throws IOException {
        ThresholdFileWriter writer = null;
        try {
            writer = new ThresholdFileWriter(parameterService.getLong(ParameterConstants.STREAM_TO_FILE_THRESHOLD),
                    "load");
            IOUtils.copy(transport.open(), writer);
        } finally {
            IOUtils.closeQuietly(writer);
            transport.close();
        }
        return new FileIncomingTransport(writer);
    }

    private void recordStatistics(List<IncomingBatch> list) {
        if (list != null) {
            statisticManager.getStatistic(StatisticNameConstants.INCOMING_BATCH_COUNT).add(list.size());
            for (IncomingBatch incomingBatch : list) {
                statisticManager.getStatistic(StatisticNameConstants.INCOMING_MS_PER_ROW).add(
                        incomingBatch.getDatabaseMillis(), incomingBatch.getStatementCount());
                statisticManager.getStatistic(StatisticNameConstants.INCOMING_BATCH_COUNT).increment();
                if (org.jumpmind.symmetric.model.IncomingBatch.Status.SK.equals(incomingBatch.getStatus())) {
                    statisticManager.getStatistic(StatisticNameConstants.INCOMING_SKIP_BATCH_COUNT).increment();
                }
            }
        }
    }

    public boolean loadData(IIncomingTransport transport) throws IOException {
        boolean inError = false;
        List<IncomingBatch> list = loadDataAndReturnBatches(transport);
        if (list != null && list.size() > 0) {
            for (IncomingBatch incomingBatch : list) {
                inError |= incomingBatch.getStatus() != org.jumpmind.symmetric.model.IncomingBatch.Status.OK;
            }
        } else {
            inError = true;
        }
        return !inError;
    }

    private void fireEarlyCommit(IDataLoader loader, IncomingBatch batch) {
        if (batchListeners != null) {
            long ts = System.currentTimeMillis();
            for (IBatchListener listener : batchListeners) {
                listener.earlyCommit(loader, batch);
            }
            // update the filter milliseconds so batch listeners are also
            // included
            batch.setFilterMillis(batch.getFilterMillis() + (System.currentTimeMillis() - ts));
        }
    }

    private void fireBatchComplete(IDataLoader loader, IncomingBatch batch) {
        if (batchListeners != null) {
            long ts = System.currentTimeMillis();
            for (IBatchListener listener : batchListeners) {
                listener.batchComplete(loader, batch);
            }
            // update the filter milliseconds so batch listeners are also
            // included
            batch.setFilterMillis(batch.getFilterMillis() + (System.currentTimeMillis() - ts));
        }
    }

    private void fireBatchCommitted(IDataLoader loader, IncomingBatch batch) {
        if (batchListeners != null) {
            long ts = System.currentTimeMillis();
            for (IBatchListener listener : batchListeners) {
                listener.batchCommitted(loader, batch);
            }
            // update the filter milliseconds so batch listeners are also
            // included
            batch.setFilterMillis(batch.getFilterMillis() + (System.currentTimeMillis() - ts));
        }
    }

    private void fireBatchRolledback(IDataLoader loader, IncomingBatch batch) {
        if (batchListeners != null) {
            long ts = System.currentTimeMillis();
            for (IBatchListener listener : batchListeners) {
                listener.batchRolledback(loader, batch);
            }
            // update the filter milliseconds so batch listeners are also
            // included
            batch.setFilterMillis(batch.getFilterMillis() + (System.currentTimeMillis() - ts));
        }
    }

    protected void handleBatchError(final IncomingBatch status) {
        try {
            if (!status.isRetry()) {
                status.setStatus(IncomingBatch.Status.ER);
                incomingBatchService.insertIncomingBatch(status);
            }
        } catch (Exception e) {
            log.error("BatchStatusRecordFailed", status.getNodeBatchId());
        }
    }

    /**
     * Load database from input stream and write acknowledgment to output stream. This is used for a "push" request with
     * a response of an acknowledgment.
     * 
     * @param in
     * @param out
     * @throws IOException
     */
    public void loadData(InputStream in, OutputStream out) throws IOException {
        List<IncomingBatch> list = loadDataAndReturnBatches(new InternalIncomingTransport(in));
        transportManager.writeAcknowledgement(out, list);
    }

    public void setDataLoaderFilters(List<IDataLoaderFilter> filters) {
        this.filters = filters;
    }

    public void addDataLoaderFilter(IDataLoaderFilter filter) {
        if (filters == null) {
            filters = new ArrayList<IDataLoaderFilter>();
        }
        filters.add(filter);
    }

    public void removeDataLoaderFilter(IDataLoaderFilter filter) {
        filters.remove(filter);
    }

    public void setTransportManager(ITransportManager remoteService) {
        this.transportManager = remoteService;
    }

    public void setIncomingBatchService(IIncomingBatchService incomingBatchService) {
        this.incomingBatchService = incomingBatchService;
    }

    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void addColumnFilter(String tableName, IColumnFilter filter) {
        this.columnFilters.put(tableName, filter);
    }

    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }

    public void addBatchListener(IBatchListener batchListener) {
        if (this.batchListeners == null) {
            this.batchListeners = new ArrayList<IBatchListener>();
        }
        this.batchListeners.add(batchListener);
    }

    public void setBatchListeners(List<IBatchListener> batchListeners) {
        this.batchListeners = batchListeners;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    private enum LoadStatus {
        CONTINUE, DONE
    }

    protected void loadBatch(final IDataLoader dataLoader, final IncomingBatch batch) {
        try {
            TransactionalLoadDelegate loadDelegate = new TransactionalLoadDelegate(batch, dataLoader);
            LoadStatus loadStatus = loadDelegate.getLoadStatus();
            do {
                newTransactionTemplate.execute(loadDelegate);
                loadStatus = loadDelegate.getLoadStatus();
                if (loadStatus == LoadStatus.CONTINUE) {
                    statisticManager.getStatistic(StatisticNameConstants.INCOMING_MAX_ROWS_COMMITTED).increment();
                    // Chances are if SymmetricDS is configured to commit early in a batch we want to give other threads
                    // a chance to do work and access the database.
                    AppUtils.sleep(5);
                }
            } while (LoadStatus.CONTINUE == loadStatus);
            fireBatchCommitted(dataLoader, batch);
        } catch (RuntimeException ex) {
            fireBatchRolledback(dataLoader, batch);
            throw ex;
        }
    }

    class TransactionalLoadDelegate implements TransactionCallback {
        IncomingBatch batch;
        IDataLoader dataLoader;
        LoadStatus loadStatus = LoadStatus.DONE;

        public TransactionalLoadDelegate(IncomingBatch status, IDataLoader dataLoader) {
            this.batch = status;
            this.dataLoader = dataLoader;
        }

        public Object doInTransaction(TransactionStatus txStatus) {
            try {
                boolean done = true;
                dbDialect.disableSyncTriggers(dataLoader.getContext().getNodeId());
                if (this.loadStatus == LoadStatus.CONTINUE || incomingBatchService.acquireIncomingBatch(batch)) {
                    done = dataLoader.load();
                } else {
                    dataLoader.skip();
                }
                batch.setValues(dataLoader.getStatistics(), true);
                if (done) {
                    fireBatchComplete(dataLoader, batch);
                    this.loadStatus = LoadStatus.DONE;
                } else {
                    fireEarlyCommit(dataLoader, batch);
                    this.loadStatus = LoadStatus.CONTINUE;
                }
                return this.loadStatus;
            } catch (IOException e) {
                throw new TransportException(e);
            } finally {
                dbDialect.enableSyncTriggers();
            }
        }

        public LoadStatus getLoadStatus() {
            return loadStatus;
        }

    }

}
