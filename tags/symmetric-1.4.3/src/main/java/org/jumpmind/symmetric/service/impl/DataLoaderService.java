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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ErrorConstants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.IBatchListener;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.load.IDataLoader;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.IDataLoaderStatistics;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatchHistory;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.IncomingBatchHistory.Status;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.RegistrationNotOpenException;
import org.jumpmind.symmetric.service.RegistrationRequiredException;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticName;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.transport.TransportException;
import org.jumpmind.symmetric.transport.internal.InternalIncomingTransport;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

public class DataLoaderService extends AbstractService implements IDataLoaderService, BeanFactoryAware {

    protected static final Log logger = LogFactory.getLog(DataLoaderService.class);

    protected IDbDialect dbDialect;

    protected IIncomingBatchService incomingBatchService;

    protected ITransportManager transportManager;

    protected TransactionTemplate transactionTemplate;

    protected BeanFactory beanFactory;

    protected List<IDataLoaderFilter> filters;

    protected IStatisticManager statisticManager;
    
    protected INodeService nodeService;

    protected Map<String, IColumnFilter> columnFilters = new HashMap<String, IColumnFilter>();

    protected List<IBatchListener> batchListeners;

    /**
     * Connect to the remote node and pull data. The acknowledgment of
     * commit/error status is sent separately after the data is processed.
     */
    public boolean loadData(Node remote, Node local) throws IOException {
        boolean wasWorkDone = false;
        try {
            List<IncomingBatchHistory> list = loadDataAndReturnBatches(transportManager.getPullTransport(remote, local));
            if (list.size() > 0) {
                sendAck(remote, local, list);
                wasWorkDone = true;
            }
        } catch (RegistrationRequiredException e) {
            logger.warn("Registration was lost, attempting to re-register");
            loadData(transportManager.getRegisterTransport(local));
            nodeService.findIdentity(false);
            wasWorkDone = true;
        } catch (MalformedURLException e) {
            logger.error("Could not connect to the " + remote + " node's transport because of a bad URL: "
                    + e.getMessage());
        }
        return wasWorkDone;
    }

    /**
     * Try a configured number of times to get the ACK through.
     */
    private void sendAck(Node remote, Node local, List<IncomingBatchHistory> list) throws IOException {
        Exception error = null;
        boolean sendAck = false;
        int numberOfStatusSendRetries = parameterService.getInt(ParameterConstants.DATA_LOADER_NUM_OF_ACK_RETRIES);
        for (int i = 0; i < numberOfStatusSendRetries && !sendAck; i++) {
            try {
                sendAck = transportManager.sendAcknowledgement(remote, list, local);
            } catch (IOException ex) {
                logger.warn("Ack was not sent successfully on try number " + i + 1 + ". " + ex.getMessage());
                error = ex;
            } catch (RuntimeException ex) {
                logger.warn("Ack was not sent successfully on try number " + i + 1 + ". " + ex.getMessage());
                error = ex;
            }
            if (!sendAck) {
                if (i < numberOfStatusSendRetries - 1) {
                    sleepBetweenFailedAcks();
                } else if (error instanceof RuntimeException) {
                    throw (RuntimeException) error;
                } else if (error instanceof IOException) {
                    throw (IOException) error;
                }
            }
        }
    }

    private final void sleepBetweenFailedAcks() {
        try {
            Thread.sleep(parameterService.getLong(ParameterConstants.DATA_LOADER_TIME_BETWEEN_ACK_RETRIES));
        } catch (InterruptedException e) {
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
                IncomingBatchHistory history = new IncomingBatchHistory(dataLoader.getContext());
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
     * Load database from input stream and return a list of batch statuses. This
     * is used for a pull request that responds with data, and the
     * acknowledgment is sent later.
     * 
     * @param in
     */
    protected List<IncomingBatchHistory> loadDataAndReturnBatches(IIncomingTransport transport) {

        List<IncomingBatchHistory> list = new ArrayList<IncomingBatchHistory>();
        IncomingBatch status = null;
        IncomingBatchHistory history = null;
        IDataLoader dataLoader = null;
        try {
            dataLoader = openDataLoader(transport.open());
            while (dataLoader.hasNext()) {
                status = new IncomingBatch(dataLoader.getContext());
                history = new IncomingBatchHistory(dataLoader.getContext());
                list.add(history);
                loadBatch(dataLoader, status, history);
                status = null;
            }
        } catch (RegistrationRequiredException ex) {
            throw ex;
        } catch (ConnectException ex) {
            logger.warn(ErrorConstants.COULD_NOT_CONNECT_TO_TRANSPORT);
            statisticManager.getStatistic(StatisticName.INCOMING_TRANSPORT_CONNECT_ERROR_COUNT).increment();
        } catch (UnknownHostException ex) {
            logger.warn(ErrorConstants.COULD_NOT_CONNECT_TO_TRANSPORT + " Unknown host name of " + ex.getMessage());
            statisticManager.getStatistic(StatisticName.INCOMING_TRANSPORT_CONNECT_ERROR_COUNT).increment();
        } catch (RegistrationNotOpenException ex) {
            logger.warn(ErrorConstants.REGISTRATION_NOT_OPEN);
        } catch (ConnectionRejectedException ex) {
            logger.warn(ErrorConstants.TRANSPORT_REJECTED_CONNECTION);
            statisticManager.getStatistic(StatisticName.INCOMING_TRANSPORT_REJECTED_COUNT).increment();
        } catch (AuthenticationException ex) {
            logger.warn(ErrorConstants.NOT_AUTHENTICATED);
        } catch (Throwable e) {
            if (dataLoader != null && status != null) {
                if (e instanceof IOException || e instanceof TransportException) {
                    logger.warn("Failed to load batch " + status.getNodeBatchId() + " because: " + e.getMessage());
                    history.setSqlMessage(e.getMessage());
                    statisticManager.getStatistic(StatisticName.INCOMING_TRANSPORT_ERROR_COUNT).increment();
                } else {
                    logger.error("Failed to load batch " + status.getNodeBatchId(), e);
                    SQLException se = unwrapSqlException(e);
                    if (se != null) {
                        statisticManager.getStatistic(StatisticName.INCOMING_DATABASE_ERROR_COUNT).increment();
                        history.setSqlState(se.getSQLState());
                        history.setSqlCode(se.getErrorCode());
                        history.setSqlMessage(se.getMessage());
                    } else {
                        history.setSqlMessage(e.getMessage());
                        statisticManager.getStatistic(StatisticName.INCOMING_OTHER_ERROR_COUNT).increment();
                    }
                }
                history.setValues(dataLoader.getStatistics(), false);
                handleBatchError(status, history);
            } else {
                if (e instanceof IOException) {
                    logger.error("Failed while reading batch because: " + e.getMessage());
                } else {
                    logger.error("Failed while parsing batch.", e);
                }
            }
        } finally {
            if (dataLoader != null) {
                dataLoader.close();
            }
            recordStatistics(list);
        }
        return list;
    }

    private void recordStatistics(List<IncomingBatchHistory> list) {
        if (list != null) {
            statisticManager.getStatistic(StatisticName.INCOMING_BATCH_COUNT).add(list.size());
            for (IncomingBatchHistory incomingBatchHistory : list) {
                statisticManager.getStatistic(StatisticName.INCOMING_MS_PER_ROW).add(
                        incomingBatchHistory.getDatabaseMillis(), incomingBatchHistory.getStatementCount());
                statisticManager.getStatistic(StatisticName.INCOMING_BATCH_COUNT).increment();
                if (IncomingBatchHistory.Status.SK.equals(incomingBatchHistory.getStatus())) {
                    statisticManager.getStatistic(StatisticName.INCOMING_SKIP_BATCH_COUNT).increment();
                }
            }
        }
    }

    public boolean loadData(IIncomingTransport transport) {
        boolean inError = false;
        List<IncomingBatchHistory> list = loadDataAndReturnBatches(transport);
        if (list != null && list.size() > 0) {
            for (IncomingBatchHistory incomingBatchHistory : list) {
                inError |= incomingBatchHistory.getStatus() != Status.OK;
            }
        } else {
            inError = true;
        }
        return !inError;
    }

    protected void loadBatch(final IDataLoader dataLoader, final IncomingBatch status,
            final IncomingBatchHistory history) {
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus transactionstatus) {
                try {
                    dbDialect.disableSyncTriggers();
                    if (incomingBatchService.acquireIncomingBatch(status)) {
                        dataLoader.load();
                    } else {
                        history.setStatus(IncomingBatchHistory.Status.SK);
                        dataLoader.skip();
                    }
                    history.setValues(dataLoader.getStatistics(), true);
                    fireBatchComplete(dataLoader, history);
                    incomingBatchService.insertIncomingBatchHistory(history);
                } catch (IOException e) {
                    throw new TransportException(e);
                } finally {
                    dbDialect.enableSyncTriggers();
                }
            }
        });
    }

    private void fireBatchComplete(IDataLoader loader, IncomingBatchHistory history) {
        if (batchListeners != null) {
            long ts = System.currentTimeMillis();
            for (IBatchListener listener : batchListeners) {
                listener.batchComplete(loader, history);
            }
            // update the filter milliseconds so batch listeners are also
            // included
            history.setFilterMillis(history.getFilterMillis() + (System.currentTimeMillis() - ts));
        }
    }

    protected void handleBatchError(final IncomingBatch status, final IncomingBatchHistory history) {
        try {
            if (!status.isRetry()) {
                status.setStatus(IncomingBatch.Status.ER);
                incomingBatchService.insertIncomingBatch(status);
            }
        } catch (Exception e) {
            logger.error("Failed to record status of batch " + status.getNodeBatchId());
        }
        try {
            history.setStatus(IncomingBatchHistory.Status.ER);
            incomingBatchService.insertIncomingBatchHistory(history);
        } catch (Exception e) {
            logger.error("Failed to record history of batch " + status.getNodeBatchId());
        }
    }

    /**
     * Load database from input stream and write acknowledgment to output
     * stream. This is used for a "push" request with a response of an
     * acknowledgment.
     * 
     * @param in
     * @param out
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public void loadData(InputStream in, OutputStream out) throws IOException {
        List<IncomingBatchHistory> list = loadDataAndReturnBatches(new InternalIncomingTransport(in));
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

    public void setTransactionTemplate(TransactionTemplate transactionTemplate) {
        this.transactionTemplate = transactionTemplate;
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

}
