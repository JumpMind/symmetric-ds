/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */
package org.jumpmind.symmetric.service.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.io.ThresholdFileWriter;
import org.jumpmind.symmetric.load.BatchListenerAdapter;
import org.jumpmind.symmetric.load.IBatchListener;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.load.IDataLoader;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.IDataLoaderStatistics;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatch.Status;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.RegistrationNotOpenException;
import org.jumpmind.symmetric.service.RegistrationRequiredException;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.transport.SyncDisabledException;
import org.jumpmind.symmetric.transport.TransportException;
import org.jumpmind.symmetric.transport.file.FileIncomingTransport;
import org.jumpmind.symmetric.transport.http.HttpTransportManager;
import org.jumpmind.symmetric.transport.internal.InternalIncomingTransport;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.symmetric.web.WebConstants;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;

/**
 * @see IDataLoaderService
 */
public class DataLoaderService extends AbstractService implements IDataLoaderService, BeanFactoryAware {

    private IDbDialect dbDialect;

    private IIncomingBatchService incomingBatchService;
    
    private IConfigurationService configurationService;

    private ITransportManager transportManager;

    private BeanFactory beanFactory;

    private List<IDataLoaderFilter> filters;

    private IStatisticManager statisticManager;

    private INodeService nodeService;

    private Map<String, List<IColumnFilter>> columnFilters = new HashMap<String, List<IColumnFilter>>();

    private List<IBatchListener> batchListeners;
    
    public DataLoaderService() {
         addBatchListener(new LoadBatchResultsListener());
    }    

    /**
     * Connect to the remote node and pull data. The acknowledgment of
     * commit/error status is sent separately after the data is processed.
     */
    public RemoteNodeStatus loadDataFromPull(Node remote)
            throws IOException {
        RemoteNodeStatus status = new RemoteNodeStatus(remote != null ? remote.getNodeId() : null);
        loadDataFromPull(remote, status);
        return status;
    }
    
    public void loadDataFromPull(Node remote, RemoteNodeStatus status) 
            throws IOException {
        try {
            Node local = nodeService.findIdentity();
            if (local == null) {
                local = new Node(this.parameterService, dbDialect);
            }
            NodeSecurity localSecurity = nodeService.findNodeSecurity(local.getNodeId());
            IIncomingTransport transport = null;
            if (remote != null && localSecurity != null) {
                Map<String, String> requestProperties = new HashMap<String, String>();
                ChannelMap suspendIgnoreChannels = configurationService
                        .getSuspendIgnoreChannelLists();
                requestProperties.put(WebConstants.SUSPENDED_CHANNELS,
                        suspendIgnoreChannels.getSuspendChannelsAsString());
                requestProperties.put(WebConstants.IGNORED_CHANNELS,
                        suspendIgnoreChannels.getIgnoreChannelsAsString());
                transport = transportManager.getPullTransport(remote, local,
                        localSecurity.getNodePassword(), requestProperties,
                        parameterService.getRegistrationUrl());
            } else {
                transport = transportManager.getRegisterTransport(local, parameterService.getRegistrationUrl());
                remote = new Node();
                remote.setSyncUrl(parameterService.getRegistrationUrl());
            }

            List<IncomingBatch> list = loadDataAndReturnBatches(transport);
            if (list.size() > 0) {
                status.updateIncomingStatus(list);
                local = nodeService.findIdentity();
                localSecurity = nodeService.findNodeSecurity(local.getNodeId());
                if (StringUtils.isNotBlank(transport.getRedirectionUrl())) {
                    // we were redirected for the pull, we need to redirect for the ack
                    String url = transport.getRedirectionUrl();
                    url = url.replace(HttpTransportManager.buildRegistrationUrl("", local), "");
                    remote.setSyncUrl(url);
                }
                sendAck(remote, local, localSecurity, list);
            }

        } catch (RegistrationRequiredException e) {
            log.warn("RegistrationLost");
            loadDataFromPull(null, status);
            nodeService.findIdentity(false);
        } catch (MalformedURLException e) {
            log.error("URLConnectingFailure", remote.getNodeId(), remote.getSyncUrl());
            throw e;
        }
    }

    /**
     * Try a configured number of times to get the ACK through.
     */
    private void sendAck(Node remote, Node local, NodeSecurity localSecurity, List<IncomingBatch> list) throws IOException {
        Exception error = null;
        int sendAck = -1;
        int numberOfStatusSendRetries = parameterService.getInt(ParameterConstants.DATA_LOADER_NUM_OF_ACK_RETRIES);
        for (int i = 0; i < numberOfStatusSendRetries && sendAck != HttpURLConnection.HTTP_OK; i++) {
            try {
                sendAck = transportManager.sendAcknowledgement(remote, list, local, localSecurity.getNodePassword(), parameterService.getRegistrationUrl());
            } catch (IOException ex) {
                log.warn("AckSendingFailed", (i + 1), ex.getMessage());
                error = ex;
            } catch (RuntimeException ex) {
                log.warn("AckSendingFailed", (i + 1), ex.getMessage());
                error = ex;
            }
            if (sendAck != HttpURLConnection.HTTP_OK) {
                if (i < numberOfStatusSendRetries - 1) {
                    AppUtils.sleep(parameterService.getLong(ParameterConstants.DATA_LOADER_TIME_BETWEEN_ACK_RETRIES));
                } else if (error instanceof RuntimeException) {
                    throw (RuntimeException) error;
                } else if (error instanceof IOException) {
                    throw (IOException) error;
                } else {
                    throw new IOException(Integer.toString(sendAck));
                }
            }
        }
    }

    public IDataLoader openDataLoader(BufferedReader reader) throws IOException {
        IDataLoader dataLoader = (IDataLoader) beanFactory.getBean(Constants.DATALOADER);
        dataLoader.open(reader, dataSource, batchListeners, filters, columnFilters);
        return dataLoader;
    }

    public IDataLoaderStatistics loadDataBatch(String batchData) throws IOException {
        BufferedReader reader = new BufferedReader(new StringReader(batchData));
        IDataLoader dataLoader = null;
        IDataLoaderStatistics stats = null;
        try {
            dataLoader = openDataLoader(reader);
            while (dataLoader.hasNext()) {
                dataLoader.load();
            }
        } finally {
            if (dataLoader != null) {
                stats = dataLoader.getStatistics();
                dataLoader.close();
            }
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
            
            IDataLoaderContext context = dataLoader.getContext();            
            while (dataLoader.hasNext()) {
                batch = context.getBatch();
                if (parameterService.is(ParameterConstants.DATA_LOADER_ENABLED) || 
                    (batch.getChannelId() != null && batch.getChannelId().equals(Constants.CHANNEL_CONFIG))) {
                    list.add(batch);
                    loadBatch(dataLoader, batch);
                }
                batch = null;
            }

            if (parameterService.is(ParameterConstants.STREAM_TO_FILE_ENABLED)) {
                estimateNetworkMillis(list, totalNetworkMillis);
            }

            for (IncomingBatch incomingBatch : list) {
                // TODO I wonder if there is a way to avoid the second update?
                incomingBatchService.updateIncomingBatch(incomingBatch);
            }

        } catch (RegistrationRequiredException ex) {
            throw ex;
        } catch (ConnectException ex) {
            throw ex;
        } catch (UnknownHostException ex) {
            log.warn("TransportFailedUnknownHost", ex.getMessage());
            throw ex;
        } catch (RegistrationNotOpenException ex) {
            log.warn("RegistrationFailed");
        } catch (ConnectionRejectedException ex) {
            log.warn("TransportFailedConnectionBusy");
            throw ex;
        } catch (AuthenticationException ex) {
            log.warn("AuthenticationFailed");
        } catch (SyncDisabledException ex) {
            log.warn("SyncDisabled");
            throw ex;
        } catch (Throwable e) {
            if (dataLoader != null && dataLoader.getContext().getBatch() != null && batch == null) {
                batch = dataLoader.getContext().getBatch();
                list.add(batch);
            }
            if (dataLoader != null && batch != null) {
                statisticManager.incrementDataLoadedErrors(batch.getChannelId(), 1);
                if (e instanceof IOException || e instanceof TransportException) {
                    log.warn("BatchLoadingFailed", batch.getNodeBatchId(), e.getMessage());
                    batch.setSqlMessage(e.getMessage());
                } else {
                    log.error("BatchLoadingFailed", e, batch.getNodeBatchId(), e.getMessage());
                    SQLException se = unwrapSqlException(e);
                    if (se != null) {                        
                        batch.setSqlState(se.getSQLState());
                        batch.setSqlCode(se.getErrorCode());
                        batch.setSqlMessage(se.getMessage());
                    } else {
                        batch.setSqlMessage(e.getMessage());
                    }
                }
                batch.setValues(dataLoader.getStatistics(), false);
                handleBatchError(batch);
            } else {
                if (e instanceof IOException) {
                    if (!e.getMessage().startsWith("http")) {
                        log.error("BatchReadingFailed", e.getMessage());
                    } else {
                        log.error("BatchReadingFailed", e.getMessage(), e);
                    }
                } else {
                    log.error("BatchParsingFailed", e);
                }
            }
        } finally {
            if (dataLoader != null) {
                dataLoader.close();
            }
            transport.close();
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

    protected void handleBatchError(final IncomingBatch batch) {
        try {
            if (batch.getStatus() != Status.OK) {
                batch.setStatus(IncomingBatch.Status.ER);
            }
            incomingBatchService.updateIncomingBatch(batch);
        } catch (Exception e) {
            log.error("BatchStatusRecordFailed", batch.getNodeBatchId());
        }
    }

    /**
     * Load database from input stream and write acknowledgment to output stream. This is used for a "push" request with
     * a response of an acknowledgment.
     */
    public void loadData(InputStream in, OutputStream out) throws IOException {
        List<IncomingBatch> list = loadDataAndReturnBatches(new InternalIncomingTransport(in));
        Node local = nodeService.findIdentity();
        NodeSecurity security = nodeService.findNodeSecurity(local.getNodeId());
        transportManager.writeAcknowledgement(out, list, local, security != null ? security.getNodePassword() : null);
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
        List<IColumnFilter> filters = this.columnFilters.get(tableName);
        if (filters == null) {
            filters = new ArrayList<IColumnFilter>();
            this.columnFilters.put(tableName, filters);
        }
        filters.add(filter);
    }
    
    /**
     * @see IDataLoaderService#reRegisterColumnFilter(String[], IColumnFilter)
     */
    public void reRegisterColumnFilter(String[] tableNames, IColumnFilter filter) {
        Set<Entry<String, List<IColumnFilter>>> entries = this.columnFilters.entrySet();
        Iterator<Entry<String, List<IColumnFilter>>> it = entries.iterator();
        while (it.hasNext()) {
            Entry<String, List<IColumnFilter>> entry = it.next();
            if (entry.getValue().contains(filter)) {
                entry.getValue().remove(filter);
            }            
        }
        
        if (tableNames != null) {
            for (String name : tableNames) {
                this.addColumnFilter(name, filter);
            }
        }        
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
    
    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    protected void loadBatch(final IDataLoader dataLoader, final IncomingBatch batch) {
        try {
            if (incomingBatchService.acquireIncomingBatch(batch)) {
                dataLoader.load();
            } else {
                dataLoader.skip();
            }
        } catch (IOException e) {
            throw new TransportException(e);
        } 
    }
    
    class LoadBatchResultsListener extends BatchListenerAdapter {

        public void batchComplete(IDataLoader loader, IncomingBatch batch) {
            loader.getContext().getBatch().setValues(loader.getStatistics(), true);
            batch.setStatus(Status.OK);
            incomingBatchService.updateIncomingBatch(batch);           
        }
        
    }

}