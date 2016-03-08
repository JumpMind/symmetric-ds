/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
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

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.UniqueKeyException;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ErrorConstants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.Batch.BatchType;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.DataProcessor;
import org.jumpmind.symmetric.io.data.IDataProcessorListener;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.reader.ProtocolDataReader;
import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.io.data.transform.TransformTable;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.Conflict.DetectConflict;
import org.jumpmind.symmetric.io.data.writer.Conflict.PingBack;
import org.jumpmind.symmetric.io.data.writer.Conflict.ResolveConflict;
import org.jumpmind.symmetric.io.data.writer.ConflictException;
import org.jumpmind.symmetric.io.data.writer.DefaultDatabaseWriter;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.IProtocolDataWriterListener;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.StagingDataWriter;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.load.ConfigurationChangedDatabaseWriterFilter;
import org.jumpmind.symmetric.load.DefaultDataLoaderFactory;
import org.jumpmind.symmetric.load.DynamicDatabaseWriterFilter;
import org.jumpmind.symmetric.load.IDataLoaderFactory;
import org.jumpmind.symmetric.load.ILoadSyncLifecycleListener;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatch.Status;
import org.jumpmind.symmetric.model.IncomingError;
import org.jumpmind.symmetric.model.LoadFilter;
import org.jumpmind.symmetric.model.LoadFilter.LoadFilterType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfoDataWriter;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessInfoKey.ProcessType;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.ILoadFilterService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.ITransformService;
import org.jumpmind.symmetric.service.RegistrationNotOpenException;
import org.jumpmind.symmetric.service.RegistrationRequiredException;
import org.jumpmind.symmetric.service.impl.TransformService.TransformTableNodeGroupLink;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.transport.ServiceUnavailableException;
import org.jumpmind.symmetric.transport.SyncDisabledException;
import org.jumpmind.symmetric.transport.TransportException;
import org.jumpmind.symmetric.transport.http.HttpTransportManager;
import org.jumpmind.symmetric.transport.internal.InternalIncomingTransport;
import org.jumpmind.symmetric.web.WebConstants;
import org.jumpmind.util.LogSuppressor;

/**
 * Responsible for writing batch data to the database
 * 
 * @see IDataLoaderService
 */
public class DataLoaderService extends AbstractService implements IDataLoaderService {

    private IIncomingBatchService incomingBatchService;

    private IConfigurationService configurationService;

    private ITransportManager transportManager;

    private IStatisticManager statisticManager;

    private INodeService nodeService;

    private ITransformService transformService;

    private ILoadFilterService loadFilterService;

    private IStagingManager stagingManager;

    private IExtensionService extensionService;

    private Map<NodeGroupLink, List<ConflictNodeGroupLink>> conflictSettingsCache = new HashMap<NodeGroupLink, List<ConflictNodeGroupLink>>();

    private long lastConflictCacheResetTimeInMs = 0;

    private ISymmetricEngine engine = null;

    private Date lastUpdateTime;
    
    private final LogSuppressor logSuppressor = new LogSuppressor(log);

    public DataLoaderService(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect());
        this.incomingBatchService = engine.getIncomingBatchService();
        this.configurationService = engine.getConfigurationService();
        this.transportManager = engine.getTransportManager();
        this.statisticManager = engine.getStatisticManager();
        this.nodeService = engine.getNodeService();
        this.transformService = engine.getTransformService();
        this.loadFilterService = engine.getLoadFilterService();
        this.stagingManager = engine.getStagingManager();
        this.setSqlMap(new DataLoaderServiceSqlMap(platform, createSqlReplacementTokens()));
        extensionService = engine.getExtensionService();
        extensionService.addExtensionPoint(new DefaultDataLoaderFactory(parameterService));
        extensionService.addExtensionPoint(new ConfigurationChangedDatabaseWriterFilter(engine));
        this.engine = engine;
    }

    public boolean refreshFromDatabase() {
        Date date = sqlTemplate.queryForObject(getSql("selectMaxLastUpdateTime"), Date.class);
        if (date != null) {
            if (lastUpdateTime == null || lastUpdateTime.before(date)) {
                if (lastUpdateTime != null) {
                    log.info("Newer conflict settings were detected");
                }
                lastUpdateTime = date;
                clearCache();
                return true;
            }
        }
        return false;
    }

    public List<String> getAvailableDataLoaderFactories() {
        return new ArrayList<String>(getDataLoaderFactories().keySet());
    }

    protected Map<String, IDataLoaderFactory> getDataLoaderFactories() {
        Map<String, IDataLoaderFactory> dataLoaderFactories = new HashMap<String, IDataLoaderFactory>();
        for (IDataLoaderFactory factory : engine.getExtensionService().getExtensionPointList(
                IDataLoaderFactory.class)) {
            dataLoaderFactories.put(factory.getTypeName(), factory);
        }
        return dataLoaderFactories;
    }

    public List<IncomingBatch> loadDataBatch(String batchData) {
        String nodeId = nodeService.findIdentityNodeId();
        if (StringUtils.isNotBlank(nodeId)) {
            ProcessInfo processInfo = statisticManager.newProcessInfo(new ProcessInfoKey(nodeId,
                    nodeId, ProcessInfoKey.ProcessType.MANUAL_LOAD));
            try {
                InternalIncomingTransport transport = new InternalIncomingTransport(
                        new BufferedReader(new StringReader(batchData)));
                List<IncomingBatch> list = loadDataFromTransport(processInfo,
                        nodeService.findIdentity(), transport);
                processInfo.setStatus(ProcessInfo.Status.OK);
                return list;
            } catch (IOException ex) {
                processInfo.setStatus(ProcessInfo.Status.ERROR);
                throw new IoException();
            } catch (RuntimeException ex) {
                processInfo.setStatus(ProcessInfo.Status.ERROR);
                throw ex;
            }
        } else {
            return new ArrayList<IncomingBatch>(0);
        }
    }

    /**
     * Connect to the remote node and pull data. The acknowledgment of
     * commit/error status is sent separately after the data is processed.
     */
    public RemoteNodeStatus loadDataFromPull(Node remote) throws IOException {
        RemoteNodeStatus status = new RemoteNodeStatus(remote != null ? remote.getNodeId() : null,
                configurationService.getChannels(false));
        loadDataFromPull(remote, status);
        return status;
    }

    public void loadDataFromPull(Node remote, RemoteNodeStatus status) throws IOException {
        Node local = nodeService.findIdentity();
        if (local == null) {
            local = new Node(this.parameterService, symmetricDialect);
        }
        try {
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

            ProcessInfo processInfo = statisticManager.newProcessInfo(new ProcessInfoKey(remote
                    .getNodeId(), local.getNodeId(), ProcessType.PULL_JOB));
            try {
                List<IncomingBatch> list = loadDataFromTransport(processInfo, remote, transport);
                if (list.size() > 0) {
                    processInfo.setStatus(ProcessInfo.Status.ACKING);
                    status.updateIncomingStatus(list);
                    local = nodeService.findIdentity();
                    if (local != null) {
                        localSecurity = nodeService.findNodeSecurity(local.getNodeId());
                        if (StringUtils.isNotBlank(transport.getRedirectionUrl())) {
                            /*
                             * We were redirected for the pull, we need to
                             * redirect for the ack
                             */
                            String url = transport.getRedirectionUrl();
                            url = url.replace(HttpTransportManager.buildRegistrationUrl("", local),
                                    "");
                            remote.setSyncUrl(url);
                        }
                        sendAck(remote, local, localSecurity, list, transportManager);
                    }
                }

                if (containsError(list)) {
                    processInfo.setStatus(ProcessInfo.Status.ERROR);
                } else {
                    processInfo.setStatus(ProcessInfo.Status.OK);
                }
            } catch (RuntimeException e) {
                processInfo.setStatus(ProcessInfo.Status.ERROR);
                throw e;
            } catch (IOException e) {
                processInfo.setStatus(ProcessInfo.Status.ERROR);
                throw e;
            }

        } catch (RegistrationRequiredException e) {
            if (StringUtils.isBlank(remote.getSyncUrl())
                    || remote.getSyncUrl().equals(parameterService.getRegistrationUrl())) {
                log.warn("Node information missing on the server.  Attempting to re-register");
                loadDataFromPull(null, status);
                nodeService.findIdentity(false);
            } else {
                log.warn(
                        "Failed to pull data from node '{}'. It probably is missing a node security record for '{}'.",
                        remote.getNodeId(), local.getNodeId());
            }
        } catch (MalformedURLException e) {
            if (remote != null) {
                log.error("Could not connect to the {} node's transport because of a bad URL: {}",
                        remote.getNodeId(), remote.getSyncUrl());
            } else {
                log.error("", e);
            }
            throw e;
        }
    }

    private boolean containsError(List<IncomingBatch> list) {
        for (IncomingBatch incomingBatch : list) {
            if (incomingBatch.getStatus() == Status.ER) {
                return true;
            }
        }
        return false;
    }

    /**
     * Load database from input stream and write acknowledgment to output
     * stream. This is used for a "push" request with a response of an
     * acknowledgment.
     */
    public void loadDataFromPush(Node sourceNode, InputStream in, OutputStream out)
            throws IOException {
        Node local = nodeService.findIdentity();
        if (local != null) {
            ProcessInfo processInfo = statisticManager.newProcessInfo(new ProcessInfoKey(sourceNode
                    .getNodeId(), local.getNodeId(), ProcessInfoKey.ProcessType.PUSH_HANDLER));
            try {
                List<IncomingBatch> batchList = loadDataFromTransport(processInfo, sourceNode,
                        new InternalIncomingTransport(in));
                logDataReceivedFromPush(sourceNode, batchList);
                NodeSecurity security = nodeService.findNodeSecurity(local.getNodeId());
                processInfo.setStatus(ProcessInfo.Status.ACKING);
                transportManager.writeAcknowledgement(out, sourceNode, batchList, local,
                        security != null ? security.getNodePassword() : null);
                if (containsError(batchList)) {
                    processInfo.setStatus(ProcessInfo.Status.ERROR);
                } else {
                    processInfo.setStatus(ProcessInfo.Status.OK);
                }
            } catch (RuntimeException e) {
                processInfo.setStatus(ProcessInfo.Status.ERROR);
                throw e;
            }
        } else {
            throw new SymmetricException("Could not load data because the node is not registered");
        }
    }

    private void logDataReceivedFromPush(Node sourceNode, List<IncomingBatch> batchList) {
        int okBatchesCount = 0;
        int errorBatchesCount = 0;
        int okDataCount = 0;
        for (IncomingBatch incomingBatch : batchList) {
            if (incomingBatch.getStatus() == Status.OK) {
                okBatchesCount++;
                okDataCount += incomingBatch.getStatementCount();
            } else if (incomingBatch.getStatus() == Status.ER) {
                errorBatchesCount++;
            }
        }
        
        if (okBatchesCount > 0) {
            log.info(
                    "{} data and {} batches loaded during push request from {}.  There were {} batches in error",
                    new Object[] { okDataCount, okBatchesCount, sourceNode.toString(),
                            errorBatchesCount });
        } 
    }

    public List<IncomingBatch> loadDataFromOfflineTransport(Node remote, RemoteNodeStatus status, IIncomingTransport transport) throws IOException {
        Node local = nodeService.findIdentity();
        ProcessInfo processInfo = statisticManager.newProcessInfo(new ProcessInfoKey(remote
                .getNodeId(), local.getNodeId(), ProcessType.OFFLINE_PULL));
        List<IncomingBatch> list = null;
        try {
            list = loadDataFromTransport(processInfo, remote, transport);
            if (list.size() > 0) {
                processInfo.setStatus(ProcessInfo.Status.ACKING);
                status.updateIncomingStatus(list);
            }

            if (containsError(list)) {
                processInfo.setStatus(ProcessInfo.Status.ERROR);
            } else {
                processInfo.setStatus(ProcessInfo.Status.OK);
            }
        } catch (RuntimeException e) {
            processInfo.setStatus(ProcessInfo.Status.ERROR);
            throw e;
        } catch (IOException e) {
            processInfo.setStatus(ProcessInfo.Status.ERROR);
            throw e;
        }
        return list;
    }

    /**
     * Load database from input stream and return a list of batch statuses. This
     * is used for a pull request that responds with data, and the
     * acknowledgment is sent later.
     */
    protected List<IncomingBatch> loadDataFromTransport(final ProcessInfo processInfo,
            final Node sourceNode, IIncomingTransport transport) throws IOException {
        final ManageIncomingBatchListener listener = new ManageIncomingBatchListener();
        final DataContext ctx = new DataContext();
        Throwable error = null;
        try {
            Node targetNode = nodeService.findIdentity();
            ctx.put(Constants.DATA_CONTEXT_ENGINE, engine);
            if (targetNode != null) {
                ctx.put(Constants.DATA_CONTEXT_TARGET_NODE, targetNode);
                ctx.put(Constants.DATA_CONTEXT_TARGET_NODE_ID, targetNode.getNodeId());
                ctx.put(Constants.DATA_CONTEXT_TARGET_NODE_GROUP_ID, targetNode.getNodeGroupId());
                ctx.put(Constants.DATA_CONTEXT_TARGET_NODE_EXTERNAL_ID, targetNode.getExternalId());
            }

            if (sourceNode != null) {
                ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE, sourceNode);
                ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE_ID, sourceNode.getNodeId());
                ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE_GROUP_ID, sourceNode.getNodeGroupId());
                ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE_EXTERNAL_ID, sourceNode.getExternalId());
            }

            for (ILoadSyncLifecycleListener l : extensionService
                    .getExtensionPointList(ILoadSyncLifecycleListener.class)) {
                l.syncStarted(ctx);
            }

            long memoryThresholdInBytes = parameterService
                    .getLong(ParameterConstants.STREAM_TO_FILE_THRESHOLD);
            long totalNetworkMillis = System.currentTimeMillis();
            String targetNodeId = nodeService.findIdentityNodeId();
            if (parameterService.is(ParameterConstants.STREAM_TO_FILE_ENABLED)) {
                processInfo.setStatus(ProcessInfo.Status.TRANSFERRING);
                IDataReader dataReader = new ProtocolDataReader(BatchType.LOAD, targetNodeId,
                        transport.openReader());
                IDataWriter dataWriter = new StagingDataWriter(memoryThresholdInBytes,
                        sourceNode.getNodeId(), Constants.STAGING_CATEGORY_INCOMING,
                        stagingManager, new LoadIntoDatabaseOnArrivalListener(processInfo,
                                sourceNode.getNodeId(), listener));
                new DataProcessor(dataReader, dataWriter, "transfer to stage").process(ctx);
                totalNetworkMillis = System.currentTimeMillis() - totalNetworkMillis;
            } else {
                DataProcessor processor = new DataProcessor(new ProtocolDataReader(BatchType.LOAD,
                        targetNodeId, transport.openReader()), null, listener, "data load") {
                    @Override
                    protected IDataWriter chooseDataWriter(Batch batch) {
                        return buildDataWriter(processInfo, sourceNode.getNodeId(),
                                batch.getChannelId(), batch.getBatchId());
                    }
                };
                processor.process(ctx);
            }

        } catch (Throwable ex) {
            error = ex;
            logAndRethrow(sourceNode, ex);
        } finally {
            transport.close();

            for (ILoadSyncLifecycleListener l : extensionService
                    .getExtensionPointList(ILoadSyncLifecycleListener.class)) {
                l.syncEnded(ctx, listener.getBatchesProcessed(), error);
            }
        }
        return listener.getBatchesProcessed();
    }

    protected void logAndRethrow(Node remoteNode, Throwable ex) throws IOException {
        if (ex instanceof RegistrationRequiredException) {
            throw (RegistrationRequiredException) ex;
        } else if (ex instanceof ConnectException) {
            throw (ConnectException) ex;
        } else if (ex instanceof UnknownHostException) {
            log.warn("Could not connect to the transport because the host was unknown: '{}'",
                    ex.getMessage());
            throw (UnknownHostException) ex;
        } else if (ex instanceof RegistrationNotOpenException) {
            log.warn("Registration attempt failed.  Registration was not open");
        } else if (ex instanceof ConnectionRejectedException) {
            throw (ConnectionRejectedException) ex;
        } else if (ex instanceof ServiceUnavailableException) {
            throw (ServiceUnavailableException) ex;            
        } else if (ex instanceof AuthenticationException) {
            log.warn("Could not authenticate with node '{}'",
                    remoteNode != null ? remoteNode.getNodeId() : "?");
            throw (AuthenticationException) ex;
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
            if (!(ex instanceof ConflictException || ex instanceof SqlException)) {
                log.error("Failed while parsing batch", ex);
            }
        }
    }

    protected IDataWriter buildDataWriter(ProcessInfo processInfo, String sourceNodeId,
            String channelId, long batchId) {
        TransformTable[] transforms = null;
        NodeGroupLink link = null;
        List<ResolvedData> resolvedDatas = new ArrayList<ResolvedData>();
        List<IDatabaseWriterFilter> filters = extensionService
                .getExtensionPointList(IDatabaseWriterFilter.class);
        List<IDatabaseWriterFilter> dynamicFilters = filters;
        List<IDatabaseWriterErrorHandler> errorHandlers = extensionService
                .getExtensionPointList(IDatabaseWriterErrorHandler.class);
        List<IDatabaseWriterErrorHandler> dynamicErrorHandlers = errorHandlers;

        if (sourceNodeId != null) {
            Node sourceNode = nodeService.findNode(sourceNodeId);
            if (sourceNode != null) {
                link = new NodeGroupLink(sourceNode.getNodeGroupId(),
                        parameterService.getNodeGroupId());
            }

            Map<LoadFilterType, Map<String, List<LoadFilter>>> loadFilters = loadFilterService
                    .findLoadFiltersFor(link, true);
            List<DynamicDatabaseWriterFilter> databaseWriterFilters = DynamicDatabaseWriterFilter
                    .getDatabaseWriterFilters(engine, loadFilters);

            if (loadFilters != null && loadFilters.size() > 0) {
                dynamicFilters = new ArrayList<IDatabaseWriterFilter>(filters.size() + 1);
                dynamicFilters.addAll(filters);
                dynamicFilters.addAll(databaseWriterFilters);

                dynamicErrorHandlers = new ArrayList<IDatabaseWriterErrorHandler>(
                        errorHandlers.size() + 1);
                dynamicErrorHandlers.addAll(errorHandlers);
                dynamicErrorHandlers.addAll(databaseWriterFilters);
            }

            List<TransformTableNodeGroupLink> transformsList = transformService.findTransformsFor(
                    link, TransformPoint.LOAD);
            transforms = transformsList != null ? transformsList
                    .toArray(new TransformTable[transformsList.size()]) : null;

            List<IncomingError> incomingErrors = getIncomingErrors(batchId, sourceNodeId);
            for (IncomingError incomingError : incomingErrors) {
                if (incomingError.isResolveIgnore()
                        || StringUtils.isNotBlank(incomingError.getResolveData())) {
                    resolvedDatas.add(new ResolvedData(incomingError.getFailedRowNumber(),
                            incomingError.getResolveData(), incomingError.isResolveIgnore()));
                }
            }

        }

        TransformWriter transformWriter = new TransformWriter(platform, TransformPoint.LOAD, null,
                transformService.getColumnTransforms(), transforms);

        IDataWriter targetWriter = getFactory(channelId).getDataWriter(sourceNodeId,
                symmetricDialect, transformWriter, dynamicFilters, dynamicErrorHandlers,
                getConflictSettingsNodeGroupLinks(link, false), resolvedDatas);
        transformWriter.setNestedWriter(new ProcessInfoDataWriter(targetWriter, processInfo));
        return transformWriter;
    }

    protected IDataLoaderFactory getFactory(String channelId) {
        Channel channel = configurationService.getChannel(channelId);
        String dataLoaderType = "default";
        IDataLoaderFactory factory = null;
        if (channel != null) {
            dataLoaderType = channel.getDataLoaderType();
        }

        Map<String, IDataLoaderFactory> dataLoaderFactories = getDataLoaderFactories();
        factory = dataLoaderFactories.get(dataLoaderType);

        if (factory == null) {
            log.warn(
                    "Could not find a data loader factory of type '{}'.  Using the 'default' data loader.",
                    dataLoaderType);
            factory = dataLoaderFactories.get("default");
        }

        if (!factory.isPlatformSupported(platform)) {
            log.warn(
                    "The current platform does not support a data loader type of '{}'.  Using the 'default' data loader.",
                    dataLoaderType);
            factory = dataLoaderFactories.get("default");
        }

        return factory;
    }

    public List<ConflictNodeGroupLink> getConflictSettingsNodeGroupLinks() {
        List<ConflictNodeGroupLink> list = new ArrayList<DataLoaderService.ConflictNodeGroupLink>();
        list = sqlTemplate.query(getSql("selectConflictSettingsSql"),
                new ConflictSettingsNodeGroupLinkMapper());
        return list;
    }

    public void clearCache() {
        synchronized (this) {
            conflictSettingsCache.clear();
            lastConflictCacheResetTimeInMs = System.currentTimeMillis();
        }
    }

    public List<ConflictNodeGroupLink> getConflictSettingsNodeGroupLinks(NodeGroupLink link,
            boolean refreshCache) {
        if (link != null) {
            long cacheTime = parameterService
                    .getLong(ParameterConstants.CACHE_TIMEOUT_CONFLICT_IN_MS);
            if (System.currentTimeMillis() - lastConflictCacheResetTimeInMs > cacheTime
                    || refreshCache) {
                clearCache();
            }

            List<ConflictNodeGroupLink> list = conflictSettingsCache.get(link);
            if (list == null) {
                list = sqlTemplate.query(
                        getSql("selectConflictSettingsSql",
                                " where source_node_group_id=? and target_node_group_id=?"),
                        new ConflictSettingsNodeGroupLinkMapper(), link.getSourceNodeGroupId(),
                        link.getTargetNodeGroupId());
                synchronized (this) {
                    conflictSettingsCache.put(link, list);
                }
            }

            return list;
        } else {
            return new ArrayList<DataLoaderService.ConflictNodeGroupLink>(0);
        }
    }

    public void delete(ConflictNodeGroupLink settings) {
        sqlTemplate.update(getSql("deleteConflictSettingsSql"), settings.getConflictId());
    }

    public void save(ConflictNodeGroupLink setting) {
        this.lastConflictCacheResetTimeInMs = 0;
        if (sqlTemplate.update(
                getSql("updateConflictSettingsSql"),
                new Object[] { setting.getNodeGroupLink().getSourceNodeGroupId(),
                        setting.getNodeGroupLink().getTargetNodeGroupId(),
                        setting.getTargetChannelId(), setting.getTargetCatalogName(),
                        setting.getTargetSchemaName(), setting.getTargetTableName(),
                        setting.getDetectType().name(), setting.getResolveType().name(),
                        setting.getPingBack().name(), setting.isResolveChangesOnly() ? 1 : 0,
                        setting.isResolveRowOnly() ? 1 : 0, setting.getDetectExpression(),
                        setting.getLastUpdateBy(), setting.getConflictId() }, new int[] {
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
                        Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR }) == 0) {
            sqlTemplate.update(
                    getSql("insertConflictSettingsSql"),
                    new Object[] { setting.getNodeGroupLink().getSourceNodeGroupId(),
                            setting.getNodeGroupLink().getTargetNodeGroupId(),
                            setting.getTargetChannelId(), setting.getTargetCatalogName(),
                            setting.getTargetSchemaName(), setting.getTargetTableName(),
                            setting.getDetectType().name(), setting.getResolveType().name(),
                            setting.getPingBack().name(), setting.isResolveChangesOnly() ? 1 : 0,
                            setting.isResolveRowOnly() ? 1 : 0, setting.getDetectExpression(),
                            setting.getLastUpdateBy(), setting.getConflictId() }, new int[] {
                            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR });
        }
    }

    public List<IncomingError> getIncomingErrors(long batchId, String nodeId) {
        return sqlTemplate.query(getSql("selectIncomingErrorSql"), new IncomingErrorMapper(),
                batchId, nodeId);
    }

    public IncomingError getCurrentIncomingError(long batchId, String nodeId) {
        return sqlTemplate.queryForObject(getSql("selectCurrentIncomingErrorSql"),
                new IncomingErrorMapper(), batchId, nodeId);
    }

    public void insertIncomingError(IncomingError incomingError) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            insertIncomingError(transaction, incomingError);
            transaction.commit();
        } catch (Error ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw ex;
        } catch (RuntimeException ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw ex;
        } finally {
            close(transaction);
        }
    }

    public void insertIncomingError(ISqlTransaction transaction, IncomingError incomingError) {
        if (StringUtils.isNotBlank(incomingError.getNodeId()) && incomingError.getBatchId() >= 0) {
            transaction.prepareAndExecute(
                    getSql("insertIncomingErrorSql"),
                    new Object[] { incomingError.getBatchId(), incomingError.getNodeId(),
                            incomingError.getFailedRowNumber(),
                            incomingError.getFailedLineNumber(),
                            incomingError.getTargetCatalogName(),
                            incomingError.getTargetSchemaName(),
                            incomingError.getTargetTableName(),
                            incomingError.getEventType().getCode(),
                            incomingError.getBinaryEncoding().name(),
                            incomingError.getColumnNames(),
                            incomingError.getPrimaryKeyColumnNames(), incomingError.getRowData(),
                            incomingError.getOldData(), incomingError.getCurData(),
                            incomingError.getResolveData(),
                            incomingError.isResolveIgnore() ? 1 : 0, incomingError.getConflictId(),
                            incomingError.getCreateTime(), incomingError.getLastUpdateBy(),
                            incomingError.getLastUpdateTime() }, new int[] { Types.BIGINT,
                            Types.VARCHAR, Types.BIGINT, Types.BIGINT, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR, Types.CHAR, Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR, Types.SMALLINT, Types.VARCHAR, Types.TIMESTAMP,
                            Types.VARCHAR, Types.TIMESTAMP });
        }
    }

    public void updateIncomingError(IncomingError incomingError) {
        sqlTemplate.update(getSql("updateIncomingErrorSql"), incomingError.getResolveData(),
                incomingError.isResolveIgnore() ? 1 : 0, incomingError.getBatchId(),
                incomingError.getNodeId(), incomingError.getFailedRowNumber());
    }

    /**
     * Used for unit tests
     */
    protected void setTransportManager(ITransportManager transportManager) {
        this.transportManager = transportManager;
    }

    class ConflictSettingsNodeGroupLinkMapper implements ISqlRowMapper<ConflictNodeGroupLink> {
        public ConflictNodeGroupLink mapRow(Row rs) {
            ConflictNodeGroupLink setting = new ConflictNodeGroupLink();
            setting.setNodeGroupLink(configurationService.getNodeGroupLinkFor(
                    rs.getString("source_node_group_id"), rs.getString("target_node_group_id"),
                    false));
            setting.setTargetChannelId(rs.getString("target_channel_id"));
            setting.setTargetCatalogName(rs.getString("target_catalog_name"));
            setting.setTargetSchemaName(rs.getString("target_schema_name"));
            setting.setTargetTableName(rs.getString("target_table_name"));
            setting.setDetectType(DetectConflict.valueOf(rs.getString("detect_type").toUpperCase()));
            setting.setResolveType(ResolveConflict.valueOf(rs.getString("resolve_type")
                    .toUpperCase()));
            setting.setPingBack(PingBack.valueOf(rs.getString("ping_back")));
            setting.setResolveChangesOnly(rs.getBoolean("resolve_changes_only"));
            setting.setResolveRowOnly(rs.getBoolean("resolve_row_only"));
            setting.setDetectExpression(rs.getString("detect_expression"));
            setting.setLastUpdateBy(rs.getString("last_update_by"));
            setting.setConflictId(rs.getString("conflict_id"));
            setting.setCreateTime(rs.getDateTime("create_time"));
            setting.setLastUpdateTime(rs.getDateTime("last_update_time"));
            return setting;
        }
    }

    class IncomingErrorMapper implements ISqlRowMapper<IncomingError> {
        public IncomingError mapRow(Row rs) {
            IncomingError incomingError = new IncomingError();
            incomingError.setBatchId(rs.getLong("batch_id"));
            incomingError.setNodeId(rs.getString("node_id"));
            incomingError.setFailedRowNumber(rs.getLong("failed_row_number"));
            incomingError.setFailedLineNumber(rs.getLong("failed_line_number"));
            incomingError.setTargetCatalogName(rs.getString("target_catalog_name"));
            incomingError.setTargetSchemaName(rs.getString("target_schema_name"));
            incomingError.setTargetTableName(rs.getString("target_table_name"));
            incomingError.setEventType(DataEventType.getEventType(rs.getString("event_type")));
            incomingError
                    .setBinaryEncoding(BinaryEncoding.valueOf(rs.getString("binary_encoding")));
            incomingError.setColumnNames(rs.getString("column_names"));
            incomingError.setPrimaryKeyColumnNames(rs.getString("pk_column_names"));
            incomingError.setRowData(rs.getString("row_data"));
            incomingError.setOldData(rs.getString("old_data"));
            incomingError.setCurData(rs.getString("cur_data"));
            incomingError.setResolveData(rs.getString("resolve_data"));
            incomingError.setResolveIgnore(rs.getBoolean("resolve_ignore"));
            incomingError.setConflictId(rs.getString("conflict_id"));
            incomingError.setCreateTime(rs.getDateTime("create_time"));
            incomingError.setLastUpdateBy(rs.getString("last_update_by"));
            incomingError.setLastUpdateTime(rs.getDateTime("last_update_time"));
            return incomingError;
        }
    }

    class LoadIntoDatabaseOnArrivalListener implements IProtocolDataWriterListener {

        private ManageIncomingBatchListener listener;

        private long batchStartsToArriveTimeInMs;

        private String sourceNodeId;

        private ProcessInfo processInfo;

        public LoadIntoDatabaseOnArrivalListener(ProcessInfo processInfo, String sourceNodeId,
                ManageIncomingBatchListener listener) {
            this.sourceNodeId = sourceNodeId;
            this.listener = listener;
            this.processInfo = processInfo;
        }

        public void start(DataContext ctx, Batch batch) {
            batchStartsToArriveTimeInMs = System.currentTimeMillis();
            processInfo.setStatus(ProcessInfo.Status.TRANSFERRING);
        }

        public void end(DataContext ctx, Batch batch, IStagedResource resource) {

            long networkMillis = System.currentTimeMillis() - batchStartsToArriveTimeInMs;

            try {
                processInfo.setStatus(ProcessInfo.Status.LOADING);
                DataProcessor processor = new DataProcessor(new ProtocolDataReader(BatchType.LOAD,
                        batch.getTargetNodeId(), resource), null, listener, "data load from stage") {
                    @Override
                    protected IDataWriter chooseDataWriter(Batch batch) {
                        return buildDataWriter(processInfo, sourceNodeId, batch.getChannelId(),
                                batch.getBatchId());
                    }
                };

                processor.process(ctx);
            } finally {
                if (listener.currentBatch != null) {
                    listener.currentBatch.setNetworkMillis(networkMillis);
                    if (batch.isIgnored()) {
                        listener.currentBatch.incrementIgnoreCount();
                    }
                }
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
                if (batch.getBatchId() == Constants.VIRTUAL_BATCH_FOR_REGISTRATION) {
                    /* Remove outgoing configuration batches because we are about to get 
                     * the complete configuration.
                     */
                    IOutgoingBatchService outgoingBatchService = engine.getOutgoingBatchService();
                    IDataService dataService = engine.getDataService();
                    dataService.deleteCapturedConfigChannelData();
                    outgoingBatchService.markAllConfigAsSentForNode(batch.getSourceNodeId());
                }
                IncomingBatch incomingBatch = new IncomingBatch(batch);
                this.batchesProcessed.add(incomingBatch);
                if (incomingBatchService.acquireIncomingBatch(incomingBatch)) {
                    this.currentBatch = incomingBatch;
                    return true;
                }
            }
            return false;
        }

        public void afterBatchStarted(DataContext context) {
            Batch batch = context.getBatch();
            ISqlTransaction transaction = context.findTransaction();
            if (transaction != null) {
                symmetricDialect.disableSyncTriggers(transaction, batch.getSourceNodeId());
            }
        }

        public void batchSuccessful(DataContext context) {
            Batch batch = context.getBatch();
            this.currentBatch.setValues(context.getReader().getStatistics().get(batch), context
                    .getWriter().getStatistics().get(batch), true);
            statisticManager.incrementDataLoaded(this.currentBatch.getChannelId(),
                    this.currentBatch.getStatementCount());
            statisticManager.incrementDataBytesLoaded(this.currentBatch.getChannelId(),
                    this.currentBatch.getByteCount());
            Status oldStatus = this.currentBatch.getStatus();
            try {
                this.currentBatch.setStatus(Status.OK);
                if (incomingBatchService.isRecordOkBatchesEnabled()) {
                    incomingBatchService.updateIncomingBatch(this.currentBatch);
                } else if (this.currentBatch.isRetry()) {
                    incomingBatchService.deleteIncomingBatch(this.currentBatch);
                }
            } catch (RuntimeException ex) {
                this.currentBatch.setStatus(oldStatus);
                throw ex;
            }
        }

        protected void enableSyncTriggers(DataContext context) {
            try {
                ISqlTransaction transaction = context.findTransaction();
                if (transaction != null) {
                    symmetricDialect.enableSyncTriggers(transaction);
                }
            } catch (Exception ex) {
                log.error("", ex);
            }
        }

        public void batchInError(DataContext context, Throwable ex) {
            try {
                if (this.currentBatch == null) {
                    /*
                     * if the current batch is null, there isn't anything we can
                     * do other than log the error
                     */
                    throw ex;
                }
                
                /*
                 * Reread batch to make sure it wasn't set to IG or OK
                 */
                engine.getIncomingBatchService().refreshIncomingBatch(currentBatch);
                
                Batch batch = context.getBatch();
                if (context.getWriter() != null
                        && context.getReader().getStatistics().get(batch) != null
                        && context.getWriter().getStatistics().get(batch) != null) {
                    this.currentBatch.setValues(context.getReader().getStatistics().get(batch),
                            context.getWriter().getStatistics().get(batch), false);
                    statisticManager.incrementDataLoaded(this.currentBatch.getChannelId(),
                            this.currentBatch.getStatementCount());
                    statisticManager.incrementDataBytesLoaded(this.currentBatch.getChannelId(),
                            this.currentBatch.getByteCount());
                    statisticManager.incrementDataLoadedErrors(this.currentBatch.getChannelId(), 1);
                } else {
                    log.error("An error caused a batch to fail without attempting to load data", ex);
                }

                enableSyncTriggers(context);

                if (ex instanceof IOException || ex instanceof TransportException
                        || ex instanceof IoException) {
                    log.warn("Failed to load batch {} because: {}",
                            this.currentBatch.getNodeBatchId(), ex.getMessage());
                    this.currentBatch.setSqlMessage(ex.getMessage());
                } else {
                    logBatchInError(ex);
                    
                    SQLException se = unwrapSqlException(ex);
                    if (ex instanceof ConflictException) {
                        String message = ex.getMessage();
                        if (se != null && isNotBlank(se.getMessage())) {
                            message = message + " " + se.getMessage();
                        }
                        this.currentBatch.setSqlMessage(message);
                        this.currentBatch.setSqlState(ErrorConstants.CONFLICT_STATE);
                        this.currentBatch.setSqlCode(ErrorConstants.CONFLICT_CODE);
                    } else if (se != null) {
                        this.currentBatch.setSqlState(se.getSQLState());
                        this.currentBatch.setSqlCode(se.getErrorCode());
                        this.currentBatch.setSqlMessage(se.getMessage());
                    } else {
                        this.currentBatch.setSqlMessage(ex.getMessage());
                    }

                }

                ISqlTransaction transaction = context.findTransaction();

                // If we were in the process of skipping or ignoring a batch
                // then its status would have been OK. We should not
                // set the status to ER.
                if (this.currentBatch.getStatus() != Status.OK &&
                        this.currentBatch.getStatus() != Status.IG) {

                    this.currentBatch.setStatus(IncomingBatch.Status.ER);
                    if (context.getTable() != null && context.getData() != null) {
                        try {
                            IncomingError error = new IncomingError();
                            error.setBatchId(this.currentBatch.getBatchId());
                            error.setNodeId(this.currentBatch.getNodeId());
                            error.setTargetCatalogName(context.getTable().getCatalog());
                            error.setTargetSchemaName(context.getTable().getSchema());
                            error.setTargetTableName(context.getTable().getName());
                            error.setColumnNames(Table.getCommaDeliminatedColumns(context
                                    .getTable().getColumns()));
                            error.setPrimaryKeyColumnNames(Table.getCommaDeliminatedColumns(context
                                    .getTable().getPrimaryKeyColumns()));
                            error.setCsvData(context.getData());
                            error.setCurData((String) context.get(DefaultDatabaseWriter.CUR_DATA));
                            error.setBinaryEncoding(context.getBatch().getBinaryEncoding());
                            error.setEventType(context.getData().getDataEventType());
                            error.setFailedLineNumber(this.currentBatch.getFailedLineNumber());
                            error.setFailedRowNumber(this.currentBatch.getFailedRowNumber());
                            if (ex instanceof ConflictException) {
                                ConflictException conflictEx = (ConflictException) ex;
                                Conflict conflict = conflictEx.getConflict();
                                if (conflict != null) {
                                    error.setConflictId(conflict.getConflictId());
                                }
                            }
                            if (transaction != null) {
                                insertIncomingError(transaction, error);
                            } else {
                                insertIncomingError(error);
                            }
                        } catch (UniqueKeyException e) {
                            // ignore. we already inserted an error for this row
                            if (transaction != null) {
                                transaction.rollback();
                            }
                        }
                    }
                }

                if (transaction != null) {
                    if (incomingBatchService.isRecordOkBatchesEnabled()
                            || this.currentBatch.isRetry()) {
                        incomingBatchService.updateIncomingBatch(transaction, this.currentBatch);
                    } else {
                        incomingBatchService.insertIncomingBatch(transaction, this.currentBatch);
                    }
                } else {
                    if (incomingBatchService.isRecordOkBatchesEnabled()
                            || this.currentBatch.isRetry()) {
                        incomingBatchService.updateIncomingBatch(this.currentBatch);
                    } else {
                        incomingBatchService.insertIncomingBatch(this.currentBatch);
                    }
                }
            } catch (Throwable e) {
                log.error("Failed to record status of batch {}",
                        this.currentBatch != null ? this.currentBatch.getNodeBatchId() : context
                                .getBatch().getNodeBatchId(), e);
            }
        }

        protected void logBatchInError(Throwable ex) {
            final String ERROR_KEY = this.currentBatch.getNodeBatchId() + ex.getMessage();
            logSuppressor.logError(ERROR_KEY, String.format("Failed to load batch %s", this.currentBatch.getNodeBatchId()), ex);
        }

        public List<IncomingBatch> getBatchesProcessed() {
            return batchesProcessed;
        }

        public IncomingBatch getCurrentBatch() {
            return currentBatch;
        }
    }

    public static class ConflictNodeGroupLink extends Conflict {
        private static final long serialVersionUID = 1L;
        protected NodeGroupLink nodeGroupLink;

        public void setNodeGroupLink(NodeGroupLink nodeGroupLink) {
            this.nodeGroupLink = nodeGroupLink;
        }

        public NodeGroupLink getNodeGroupLink() {
            return nodeGroupLink;
        }
    }

}
