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
 * under the License. 
 */
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
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataProcessor;
import org.jumpmind.symmetric.io.data.IDataProcessorListener;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.reader.ProtocolDataReader;
import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.io.data.transform.TransformTable;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriter;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.IProtocolDataWriterListener;
import org.jumpmind.symmetric.io.data.writer.StagingDataWriter;
import org.jumpmind.symmetric.io.data.writer.TransformDatabaseWriter;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.load.ConfigurationChangedFilter;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatch.Status;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITransformService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.service.RegistrationNotOpenException;
import org.jumpmind.symmetric.service.RegistrationRequiredException;
import org.jumpmind.symmetric.service.impl.TransformService.TransformTableNodeGroupLink;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.transport.SyncDisabledException;
import org.jumpmind.symmetric.transport.TransportException;
import org.jumpmind.symmetric.transport.http.HttpTransportManager;
import org.jumpmind.symmetric.transport.internal.InternalIncomingTransport;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.symmetric.web.WebConstants;

/**
 * Responsible for writing batch data to the database
 * 
 * @see IDataLoaderService
 */
public class DataLoaderService extends AbstractService implements IDataLoaderService {

    private IIncomingBatchService incomingBatchService;

    private IConfigurationService configurationService;

    private ITransportManager transportManager;

    private List<IDatabaseWriterFilter> filters;

    private IStatisticManager statisticManager;

    private INodeService nodeService;

    private ITransformService transformService;

    private IStagingManager stagingManager;

    public DataLoaderService(IParameterService parameterService,
            ISymmetricDialect symmetricDialect, IIncomingBatchService incomingBatchService,
            IConfigurationService configurationService, ITransportManager transportManager,
            IStatisticManager statisticManager, INodeService nodeService,
            ITransformService transformService, ITriggerRouterService triggerRouterService,
            IStagingManager stagingManager) {
        super(parameterService, symmetricDialect);
        this.incomingBatchService = incomingBatchService;
        this.configurationService = configurationService;
        this.transportManager = transportManager;
        this.statisticManager = statisticManager;
        this.nodeService = nodeService;
        this.transformService = transformService;
        this.stagingManager = stagingManager;
        this.filters = new ArrayList<IDatabaseWriterFilter>();
        this.filters.add(new ConfigurationChangedFilter(parameterService, configurationService,
                triggerRouterService, transformService));
    }
    
    public List<IncomingBatch> loadDataBatch(String batchData) throws IOException {
        InternalIncomingTransport transport = new InternalIncomingTransport(new BufferedReader(new StringReader(batchData)));
        return loadDataFromTransport(nodeService.findIdentityNodeId(), transport);
    }

    /**
     * Connect to the remote node and pull data. The acknowledgment of
     * commit/error status is sent separately after the data is processed.
     */
    public RemoteNodeStatus loadDataFromPull(Node remote) throws IOException {
        RemoteNodeStatus status = new RemoteNodeStatus(remote != null ? remote.getNodeId() : null);
        loadDataFromPull(remote, status);
        return status;
    }

    public void loadDataFromPull(Node remote, RemoteNodeStatus status) throws IOException {
        try {
            Node local = nodeService.findIdentity();
            if (local == null) {
                local = new Node(this.parameterService, symmetricDialect);
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
                transport = transportManager.getRegisterTransport(local,
                        parameterService.getRegistrationUrl());
                log.info("Using registration URL of {}", transport.getUrl());
                remote = new Node();
                remote.setSyncUrl(parameterService.getRegistrationUrl());
            }

            List<IncomingBatch> list = loadDataFromTransport(remote.getNodeId(), transport);
            if (list.size() > 0) {
                status.updateIncomingStatus(list);
                local = nodeService.findIdentity();
                if (local != null) {
                    localSecurity = nodeService.findNodeSecurity(local.getNodeId());
                    if (StringUtils.isNotBlank(transport.getRedirectionUrl())) {
                        // we were redirected for the pull, we need to redirect
                        // for the ack
                        String url = transport.getRedirectionUrl();
                        url = url.replace(HttpTransportManager.buildRegistrationUrl("", local), "");
                        remote.setSyncUrl(url);
                    }
                    sendAck(remote, local, localSecurity, list);
                }
            }

        } catch (RegistrationRequiredException e) {
            log.warn("Registration was lost. Attempting to re-register.");
            loadDataFromPull(null, status);
            nodeService.findIdentity(false);
        } catch (MalformedURLException e) {
            log.error("Could not connect to the {} node's transport because of a bad URL: {}",
                    remote.getNodeId(), remote.getSyncUrl());
            throw e;
        }
    }
    
    /**
     * Load database from input stream and write acknowledgment to output
     * stream. This is used for a "push" request with a response of an
     * acknowledgment.
     */
    public void loadDataFromPush(String sourceNodeId, InputStream in, OutputStream out)
            throws IOException {
        List<IncomingBatch> list = loadDataFromTransport(sourceNodeId,
                new InternalIncomingTransport(in));
        Node local = nodeService.findIdentity();
        NodeSecurity security = nodeService.findNodeSecurity(local.getNodeId());
        transportManager.writeAcknowledgement(out, list, local,
                security != null ? security.getNodePassword() : null);
    }

    public void addDatabaseWriterFilter(IDatabaseWriterFilter filter) {
        if (filters == null) {
            filters = new ArrayList<IDatabaseWriterFilter>();
        }
        filters.add(filter);
    }

    public void removeDatabaseWriterFilter(IDatabaseWriterFilter filter) {
        filters.remove(filter);
    }    

    /**
     * Try a configured number of times to get the ACK through.
     */
    protected void sendAck(Node remote, Node local, NodeSecurity localSecurity,
            List<IncomingBatch> list) throws IOException {
        Exception error = null;
        int sendAck = -1;
        int numberOfStatusSendRetries = parameterService
                .getInt(ParameterConstants.DATA_LOADER_NUM_OF_ACK_RETRIES);
        for (int i = 0; i < numberOfStatusSendRetries && sendAck != HttpURLConnection.HTTP_OK; i++) {
            try {
                sendAck = transportManager.sendAcknowledgement(remote, list, local,
                        localSecurity.getNodePassword(), parameterService.getRegistrationUrl());
            } catch (IOException ex) {
                log.warn("Ack was not sent successfully on try number {}.  {}", i + 1,
                        ex.getMessage());
                error = ex;
            } catch (RuntimeException ex) {
                log.warn("Ack was not sent successfully on try number {}.  {}", i + 1,
                        ex.getMessage());
                error = ex;
            }
            if (sendAck != HttpURLConnection.HTTP_OK) {
                if (i < numberOfStatusSendRetries - 1) {
                    AppUtils.sleep(parameterService
                            .getLong(ParameterConstants.DATA_LOADER_TIME_BETWEEN_ACK_RETRIES));
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

    protected IDataWriter prepareDatabaseWriter(String sourceNodeId) {
        DatabaseWriterSettings settings = buildDatabaseWriterSettings();

        TransformTable[] transforms = null;
        if (sourceNodeId != null) {
            List<TransformTableNodeGroupLink> transformsList = transformService.findTransformsFor(
                    new NodeGroupLink(sourceNodeId, nodeService.findIdentityNodeId()),
                    TransformPoint.LOAD, true);
            transforms = transformsList != null ? transformsList
                    .toArray(new TransformTable[transformsList.size()]) : null;
        }

        return new TransformDatabaseWriter(symmetricDialect.getPlatform(), settings, null,
                transforms, filters.toArray(new IDatabaseWriterFilter[filters.size()]));
    }

    /**
     * Load database from input stream and return a list of batch statuses. This
     * is used for a pull request that responds with data, and the
     * acknowledgment is sent later.
     */
    protected List<IncomingBatch> loadDataFromTransport(String sourceNodeId,
            IIncomingTransport transport) throws IOException {
        final ManageIncomingBatchListener listener = new ManageIncomingBatchListener();
        try {

            long totalNetworkMillis = System.currentTimeMillis();
            if (parameterService.is(ParameterConstants.STREAM_TO_FILE_ENABLED)) {
                IDataReader dataReader = new ProtocolDataReader(transport.open());
                IDataWriter dataWriter = new StagingDataWriter(sourceNodeId, Constants.STAGING_CATEGORY_INCOMING,
                        stagingManager, new LoadIntoDatabaseOnArrivalListener(sourceNodeId,
                                listener));
                new DataProcessor(dataReader, dataWriter).process();
                totalNetworkMillis = System.currentTimeMillis() - totalNetworkMillis;
            } else {
                DataProcessor processor = new DataProcessor(
                        new ProtocolDataReader(transport.open()),
                        prepareDatabaseWriter(sourceNodeId), listener);
                processor.process();
            }

            List<IncomingBatch> batchesProcessed = listener.getBatchesProcessed();
            for (IncomingBatch incomingBatch : batchesProcessed) {
                if (incomingBatch.getBatchId() != BatchInfo.VIRTUAL_BATCH_FOR_REGISTRATION
                        && incomingBatchService.updateIncomingBatch(incomingBatch) == 0) {
                    log.error("Failed to update batch {}.  Zero rows returned.",
                            incomingBatch.getBatchId());
                }
            }
        } catch (Exception ex) {
            logAndRethrow(ex);
        } finally {
            transport.close();
        }
        return listener.getBatchesProcessed();
    }

    protected void logAndRethrow(Exception ex) throws IOException {
        if (ex instanceof RegistrationRequiredException) {
            throw (RegistrationRequiredException) ex;
        } else if (ex instanceof ConnectException) {
            throw (ConnectException) ex;
        } else if (ex instanceof UnknownHostException) {
            log.warn("Could not connect to the transport because the host was unknown: {}",
                    ex.getMessage());
            throw (UnknownHostException) ex;
        } else if (ex instanceof RegistrationNotOpenException) {
            log.warn("Registration attempt failed.  Registration was not open for the node.");
        } else if (ex instanceof ConnectionRejectedException) {
            log.warn("The server was too busy to accept the connection");
            throw (ConnectionRejectedException) ex;
        } else if (ex instanceof AuthenticationException) {
            log.warn("Could not authenticate with node");
        } else if (ex instanceof SyncDisabledException) {
            log.warn("Synchronization is disabled on the server node");
            throw (SyncDisabledException) ex;
        } else if (ex instanceof IOException) {
            if (ex.getMessage() != null && !ex.getMessage().startsWith("http")) {
                log.error("Failed while reading batch because: {}", ex.getMessage());
            } else {
                log.error("Failed while reading batch because: {}", ex.getMessage(), ex);
            }
            throw (IOException) ex;
        } else {
            log.error("Failed while parsing batch", ex);
        }
    }

    protected DatabaseWriterSettings buildDatabaseWriterSettings() {
        DatabaseWriterSettings settings = new DatabaseWriterSettings();
        settings.setConflictResolutionDeletes(parameterService
                .is(ParameterConstants.DATA_LOADER_ALLOW_MISSING_DELETE) ? DatabaseWriterSettings.ConflictResolutionDeletes.IGNORE_CONTINUE
                : DatabaseWriterSettings.ConflictResolutionDeletes.ERROR_STOP);
        settings.setConflictResolutionInserts(parameterService
                .is(ParameterConstants.DATA_LOADER_ENABLE_FALLBACK_UPDATE) ? DatabaseWriterSettings.ConflictResolutionInserts.FALLBACK_UPDATE
                : DatabaseWriterSettings.ConflictResolutionInserts.ERROR_STOP);
        settings.setConflictResolutionUpdates(parameterService
                .is(ParameterConstants.DATA_LOADER_ENABLE_FALLBACK_INSERT) ? DatabaseWriterSettings.ConflictResolutionUpdates.FALLBACK_INSERT
                : DatabaseWriterSettings.ConflictResolutionUpdates.ERROR_STOP);
        settings.setMaxRowsBeforeCommit(parameterService
                .getLong(ParameterConstants.DATA_LOADER_MAX_ROWS_BEFORE_COMMIT));
        settings.setTreatDateTimeFieldsAsVarchar(parameterService
                .is(ParameterConstants.DATA_LOADER_TREAT_DATETIME_AS_VARCHAR));
        return settings;
    }

    protected void setTransportManager(ITransportManager transportManager) {
        this.transportManager = transportManager;
    }

    class LoadIntoDatabaseOnArrivalListener implements IProtocolDataWriterListener {

        private IDataWriter databaseWriter;

        private ManageIncomingBatchListener listener;
        
        private long batchStartsToArriveTimeInMs;

        public LoadIntoDatabaseOnArrivalListener(String sourceNodeId,
                ManageIncomingBatchListener listener) {
            this.databaseWriter = prepareDatabaseWriter(sourceNodeId);
            this.listener = listener;
        }

        public void start(Batch batch) {
            batchStartsToArriveTimeInMs = System.currentTimeMillis();
        }

        public void end(Batch batch, IStagedResource resource) {
            if (listener.currentBatch != null) {
                listener.currentBatch.setNetworkMillis(System.currentTimeMillis()-batchStartsToArriveTimeInMs);
            }
            
            DataProcessor processor = new DataProcessor(new ProtocolDataReader(resource),
                    databaseWriter, listener);
            try {
                processor.process();
            } finally {
                resource.setState(State.DONE);
            }
        }
    }

    class ManageIncomingBatchListener implements IDataProcessorListener {

        protected List<IncomingBatch> batchesProcessed = new ArrayList<IncomingBatch>();

        protected IncomingBatch currentBatch;        

        public void beforeBatchEnd(DataContext context) {
            enableSyncTriggers(context);
        }

        public boolean beforeBatchStarted(DataContext context) {
            this.currentBatch = null;
            Batch batch = context.getBatch();
            if (parameterService.is(ParameterConstants.DATA_LOADER_ENABLED)
                    || (batch.getChannelId() != null && batch.getChannelId().equals(
                            Constants.CHANNEL_CONFIG))) {
                IncomingBatch incomingBatch = new IncomingBatch(batch);
                if (incomingBatchService.acquireIncomingBatch(incomingBatch)) {
                    this.batchesProcessed.add(incomingBatch);
                    this.currentBatch = incomingBatch;
                    return true;
                }
            }
            return false;
        }

        public void afterBatchStarted(DataContext context) {
            Batch batch = context.getBatch();
            symmetricDialect.disableSyncTriggers(findTransaction(context), batch.getNodeId());
        }

        protected ISqlTransaction findTransaction(DataContext context) {
            if (context.getWriter() instanceof TransformWriter) {
                IDataWriter targetWriter = ((TransformWriter) context.getWriter())
                        .getTargetWriter();
                if (targetWriter instanceof DatabaseWriter) {
                    return ((DatabaseWriter) targetWriter).getTransaction();
                }
            }
            return null;
        }

        public void batchSuccessful(DataContext context) {
            Batch batch = context.getBatch();
            this.currentBatch.setValues(context.getReader().getStatistics().get(batch), context
                    .getWriter().getStatistics().get(batch), true);
            Status oldStatus = this.currentBatch.getStatus();
            try {
                this.currentBatch.setStatus(Status.OK);
                incomingBatchService.updateIncomingBatch(this.currentBatch);
            } catch (RuntimeException ex) {
                this.currentBatch.setStatus(oldStatus);
                throw ex;
            }
        }

        protected void enableSyncTriggers(DataContext context) {
            try {
                ISqlTransaction transaction = findTransaction(context);
                if (transaction != null) {
                    symmetricDialect.enableSyncTriggers(transaction);
                }
            } catch (Exception ex) {
                log.error(ex.getMessage(), ex);
            }
        }

        public void batchInError(DataContext context, Exception ex) {
            try {
                Batch batch = context.getBatch();
                this.currentBatch.setValues(context.getReader().getStatistics().get(batch), context
                        .getWriter().getStatistics().get(batch), false);
                enableSyncTriggers(context);
                statisticManager.incrementDataLoadedErrors(this.currentBatch.getChannelId(), 1);
                if (ex instanceof IOException || ex instanceof TransportException) {
                    log.warn("Failed to load batch {} because: {}",
                            this.currentBatch.getNodeBatchId(), ex.getMessage());
                    this.currentBatch.setSqlMessage(ex.getMessage());
                } else {
                    log.error("Failed to load batch {} because: {}", new Object[] {
                            this.currentBatch.getNodeBatchId(), ex.getMessage() });
                    log.error(ex.getMessage(), ex);
                    SQLException se = unwrapSqlException(ex);
                    if (se != null) {
                        this.currentBatch.setSqlState(se.getSQLState());
                        this.currentBatch.setSqlCode(se.getErrorCode());
                        this.currentBatch.setSqlMessage(se.getMessage());
                    } else {
                        this.currentBatch.setSqlMessage(ex.getMessage());
                    }
                }

                // If we were in the process of skipping a batch
                // then its status would have been OK. We should not
                // set the status to ER.
                if (this.currentBatch.getStatus() != Status.OK) {
                    this.currentBatch.setStatus(IncomingBatch.Status.ER);
                }
                incomingBatchService.updateIncomingBatch(this.currentBatch);
            } catch (Exception e) {
                log.error("Failed to record status of batch {}",
                        this.currentBatch != null ? this.currentBatch.getNodeBatchId() : context
                                .getBatch().getNodeBatchId());
            }
        }

        public List<IncomingBatch> getBatchesProcessed() {
            return batchesProcessed;
        }

        public IncomingBatch getCurrentBatch() {
            return currentBatch;
        }
    }

}
