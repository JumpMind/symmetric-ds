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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ErrorConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.load.IDataLoader;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatchHistory;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.IncomingBatchHistory.Status;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.RegistrationNotOpenException;
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

    int numberOfStatusSendRetries = 5;

    long timeBetweenStatusSendRetriesMs = 5000;

    protected Map<String, IColumnFilter> columnFilters = new HashMap<String, IColumnFilter>();

    /**
     * Connect to the remote node and pull data. The acknowledgment of
     * commit/error status is sent separately after the data is processed.
     */
    public void loadData(Node remote, Node local) throws IOException {
        List<IncomingBatchHistory> list = loadDataAndReturnBatches(transportManager.getPullTransport(remote, local));
        sendAck(remote, local, list);
    }

    /**
     * Try a configured number of times to get the ACK through.
     */
    private void sendAck(Node remote, Node local, List<IncomingBatchHistory> list) throws IOException {
        Exception error = null;
        boolean sendAck = false;
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
                    sleep();
                } else if (error instanceof RuntimeException) {
                    throw (RuntimeException) error;
                } else if (error instanceof IOException) {
                    throw (IOException) error;
                }
            }
        }
    }

    private final void sleep() {
        try {
            Thread.sleep(timeBetweenStatusSendRetriesMs);
        } catch (InterruptedException e) {
        }
    }

    /**
     * Load database from input stream and return a list of batch statuses. This
     * is used for a pull request that responds with data, and the
     * acknowledgment is sent later.
     * 
     * @param in
     */
    protected List<IncomingBatchHistory> loadDataAndReturnBatches(IIncomingTransport transport) {
        IDataLoader dataLoader = (IDataLoader) beanFactory.getBean(Constants.DATALOADER);
        List<IncomingBatchHistory> list = new ArrayList<IncomingBatchHistory>();
        IncomingBatch status = null;
        IncomingBatchHistory history = null;
        try {
            dataLoader.open(transport.open(), filters, columnFilters);
            while (dataLoader.hasNext()) {
                status = new IncomingBatch(dataLoader.getContext());
                history = new IncomingBatchHistory(dataLoader.getContext());
                list.add(history);
                loadBatch(dataLoader, status, history);
                status = null;
            }
        } catch (ConnectException ex) {
            logger.warn(ErrorConstants.COULD_NOT_CONNECT_TO_TRANSPORT);
        } catch (UnknownHostException ex) {
            logger.warn(ErrorConstants.COULD_NOT_CONNECT_TO_TRANSPORT + " Unknown host name of " + ex.getMessage());
        } catch (RegistrationNotOpenException ex) {
            logger.warn(ErrorConstants.REGISTRATION_NOT_OPEN);
        } catch (ConnectionRejectedException ex) {
            logger.warn(ErrorConstants.TRANSPORT_REJECTED_CONNECTION);
        } catch (AuthenticationException ex) {
            logger.warn(ErrorConstants.NOT_AUTHENTICATED);
        } catch (Exception e) {
            if (status != null) {
                if (e instanceof SocketException || e instanceof TransportException) {
                    logger.warn(e.getMessage());
                } else {
                    logger.error("Failed to load batch " + status.getNodeBatchId(), e);
                }
                history.setValues(dataLoader.getStatistics(), false);
                handleBatchError(status, history);
            } else {
                if (e instanceof IOException) {
                    logger.error("Failed while reading batch.", e);
                } else {
                    logger.error("Failed while parsing batch.", e);
                }
            }
        } finally {
            dataLoader.close();
        }
        return list;
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
                    incomingBatchService.insertIncomingBatchHistory(history);
                } catch (IOException e) {
                    throw new TransportException(e);
                } finally {
                    dbDialect.enableSyncTriggers();
                }
            }
        });
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

    public void setNumberOfStatusSendRetries(int numberOfStatusSendRetries) {
        this.numberOfStatusSendRetries = numberOfStatusSendRetries;
    }

    public void setTimeBetweenStatusSendRetriesMs(long timeBetweenStatusSendRetriesMs) {
        this.timeBetweenStatusSendRetriesMs = timeBetweenStatusSendRetriesMs;
    }

}
