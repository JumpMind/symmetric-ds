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
import static org.jumpmind.symmetric.common.Constants.LOG_PROCESS_SUMMARY_THRESHOLD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.SyntaxParsingException;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ContextConstants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.load.DefaultReloadGenerator;
import org.jumpmind.symmetric.load.IReloadGenerator;
import org.jumpmind.symmetric.model.AbstractBatch.Status;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessType;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.TableReloadRequest;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.route.AbstractFileParsingRouter;
import org.jumpmind.symmetric.route.AuditTableDataRouter;
import org.jumpmind.symmetric.route.BshDataRouter;
import org.jumpmind.symmetric.route.CSVRouter;
import org.jumpmind.symmetric.route.ChannelRouterContext;
import org.jumpmind.symmetric.route.ColumnMatchDataRouter;
import org.jumpmind.symmetric.route.ConfigurationChangedDataRouter;
import org.jumpmind.symmetric.route.DBFRouter;
import org.jumpmind.symmetric.route.DataGapDetector;
import org.jumpmind.symmetric.route.DataGapFastDetector;
import org.jumpmind.symmetric.route.DataGapRouteReader;
import org.jumpmind.symmetric.route.DefaultBatchAlgorithm;
import org.jumpmind.symmetric.route.DefaultDataRouter;
import org.jumpmind.symmetric.route.DelayRoutingException;
import org.jumpmind.symmetric.route.FileSyncDataRouter;
import org.jumpmind.symmetric.route.IBatchAlgorithm;
import org.jumpmind.symmetric.route.IDataRouter;
import org.jumpmind.symmetric.route.IDataToRouteReader;
import org.jumpmind.symmetric.route.LookupTableDataRouter;
import org.jumpmind.symmetric.route.NonTransactionalBatchAlgorithm;
import org.jumpmind.symmetric.route.SimpleRouterContext;
import org.jumpmind.symmetric.route.SubSelectDataRouter;
import org.jumpmind.symmetric.route.TPSRouter;
import org.jumpmind.symmetric.route.TransactionalBatchAlgorithm;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.statistic.StatisticConstants;
import org.jumpmind.util.FormatUtils;

/**
 * @see IRouterService
 */
public class RouterService extends AbstractService implements IRouterService {
    
    public static final int MAX_LOGGING_LENGTH = 512;

    protected Map<String, Boolean> commonBatchesLastKnownState = new HashMap<String, Boolean>();
    
    protected long commonBatchesCacheTime;

    protected Map<String, Boolean> defaultRouterOnlyLastKnownState = new HashMap<String, Boolean>();
    
    protected long defaultRoutersCacheTime;

    protected transient ExecutorService readThread = null;

    protected ISymmetricEngine engine;
    
    protected IExtensionService extensionService;
    
    protected DataGapDetector gapDetector;

    protected boolean syncTriggersBeforeInitialLoadAttempted = false;
    
    protected boolean firstTimeCheck = true;

    public RouterService(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect());

        this.engine = engine;
        this.extensionService = engine.getExtensionService();

        extensionService.addExtensionPoint(DefaultBatchAlgorithm.NAME, new DefaultBatchAlgorithm());
        extensionService.addExtensionPoint(NonTransactionalBatchAlgorithm.NAME, new NonTransactionalBatchAlgorithm());
        extensionService.addExtensionPoint(TransactionalBatchAlgorithm.NAME, new TransactionalBatchAlgorithm());

        extensionService.addExtensionPoint(ConfigurationChangedDataRouter.ROUTER_TYPE, new ConfigurationChangedDataRouter(engine));
        extensionService.addExtensionPoint("bsh", new BshDataRouter(engine));
        extensionService.addExtensionPoint("subselect", new SubSelectDataRouter(symmetricDialect));
        extensionService.addExtensionPoint("lookuptable", new LookupTableDataRouter(symmetricDialect));
        extensionService.addExtensionPoint("default", new DefaultDataRouter());
        extensionService.addExtensionPoint("audit", new AuditTableDataRouter(engine));
        extensionService.addExtensionPoint("column", new ColumnMatchDataRouter(engine.getConfigurationService(),
                engine.getSymmetricDialect()));
        extensionService.addExtensionPoint(FileSyncDataRouter.ROUTER_TYPE, new FileSyncDataRouter(engine));
        extensionService.addExtensionPoint("dbf", new DBFRouter(engine));
        extensionService.addExtensionPoint("tps", new TPSRouter(engine));

        extensionService.addExtensionPoint("csv", new CSVRouter(engine));
        extensionService.addExtensionPoint(DefaultReloadGenerator.NAME, new DefaultReloadGenerator(engine));

        setSqlMap(new RouterServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));   
    }

    /**
     * For use in data load events
     */
    public boolean shouldDataBeRouted(SimpleRouterContext context, DataMetaData dataMetaData,
            Node node, boolean initialLoad, boolean initialLoadSelectUsed, TriggerRouter triggerRouter) {
        IDataRouter router = getDataRouter(dataMetaData.getRouter());
        Set<Node> oneNodeSet = new HashSet<Node>(1);
        oneNodeSet.add(node);
        Collection<String> nodeIds = router.routeToNodes(context, dataMetaData, oneNodeSet, initialLoad,
                initialLoadSelectUsed, triggerRouter);
        return nodeIds != null && nodeIds.contains(node.getNodeId());
    }

    public synchronized void stop() {
        if (readThread != null) {
            try {
                log.info("RouterService is shutting down");
                readThread.shutdown();
                readThread = null;
            } catch (Exception ex) {
                log.error("", ex);
            }
        }
    }

    /**
     * This method will route data to specific nodes.
     */
    synchronized public long routeData(boolean force) {
        long dataCount = -1l;
        Node identity = engine.getNodeService().findIdentity();
        if (identity != null) {
            if (force || engine.getClusterService().lock(ClusterConstants.ROUTE)) {
                try {
                    if (firstTimeCheck) {
                        engine.getOutgoingBatchService().updateAbandonedRoutingBatches();
                        if (engine.getDataService().fixLastDataGap()) {
                            engine.getContextService().save(ContextConstants.ROUTING_FULL_GAP_ANALYSIS, Boolean.TRUE.toString());
                        }
                        firstTimeCheck = false;
                    }


                    if (gapDetector == null) {
                        if (parameterService.is(ParameterConstants.ROUTING_USE_FAST_GAP_DETECTOR)) {
                            gapDetector = new DataGapFastDetector(engine.getDataService(), parameterService, engine.getContextService(), 
                                    symmetricDialect, this, engine.getStatisticManager(), engine.getNodeService());
                        } else {
                            gapDetector = new DataGapDetector(engine.getDataService(), parameterService, symmetricDialect, 
                                    this, engine.getStatisticManager(), engine.getNodeService());                        
                        }
                    }
                    insertInitialLoadEvents();
                    engine.getClusterService().refreshLock(ClusterConstants.ROUTE);

                    long ts = System.currentTimeMillis();
                    gapDetector.beforeRouting();
                    
                    dataCount = routeDataForEachChannel();
                    ts = System.currentTimeMillis() - ts;
                    if (dataCount > 0 || ts > Constants.LONG_OPERATION_THRESHOLD) {
                        log.info("Routed {} data events in {} ms", dataCount, ts);
                    }
                    if (dataCount > 0) {
                        gapDetector.afterRouting();
                    }
                } finally {
                    if (!force) {
                        engine.getClusterService().unlock(ClusterConstants.ROUTE);
                    }
                }
            }
        }
        return dataCount;
    }

    /**
     * If a load has been queued up by setting the initial load enabled or
     * reverse initial load enabled flags, then the router service will insert
     * the reload events. This process will not run at the same time sync
     * triggers is running.
     */
    protected void insertInitialLoadEvents() {

        ProcessInfo processInfo = engine.getStatisticManager().newProcessInfo(
                new ProcessInfoKey(engine.getNodeService().findIdentityNodeId(), null,
                        ProcessType.INSERT_LOAD_EVENTS));
        processInfo.setStatus(ProcessInfo.ProcessStatus.PROCESSING);

        try {

            INodeService nodeService = engine.getNodeService();
            IDataService dataService = engine.getDataService();
            ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
            Node identity = nodeService.findIdentity();
            if (identity != null) {
                boolean isClusteringEnabled = parameterService.is(ParameterConstants.CLUSTER_LOCKING_ENABLED);
                NodeSecurity identitySecurity = nodeService.findNodeSecurity(identity.getNodeId(), !isClusteringEnabled);
                if (engine.getParameterService().isRegistrationServer()
                        || (identitySecurity != null && !identitySecurity.isRegistrationEnabled() && identitySecurity
                                .getRegistrationTime() != null)) {

                    List<NodeSecurity> nodeSecurities = findNodesThatAreReadyForInitialLoad();
                    Map<String, List<TriggerRouter>> triggerRoutersByTargetNodeGroupId = new HashMap<String, List<TriggerRouter>>();
                    
                    if (nodeSecurities != null && nodeSecurities.size() > 0) {
                        gapDetector.setFullGapAnalysis(true);
                        boolean reverseLoadFirst = parameterService
                                .is(ParameterConstants.INITIAL_LOAD_REVERSE_FIRST);
                        boolean isInitialLoadQueued = false;

                        
                        for (NodeSecurity security : nodeSecurities) {
                        		Node targetNode = engine.getNodeService().findNode(security.getNodeId());
                            List<TriggerHistory> activeHistories = extensionService.getExtensionPoint(IReloadGenerator.class).getActiveTriggerHistories(targetNode);
                            
                            if (activeHistories.size() > 0) {
                                boolean thisMySecurityRecord = security.getNodeId().equals(
                                        identity.getNodeId());
                                boolean reverseLoadQueued = security.isRevInitialLoadEnabled();
                                boolean initialLoadQueued = security.isInitialLoadEnabled();
                                boolean registered = security.getRegistrationTime() != null;
                                if (thisMySecurityRecord && reverseLoadQueued
                                        && (reverseLoadFirst || !initialLoadQueued)) {
                                    sendReverseInitialLoad(processInfo);
                                } else if (!thisMySecurityRecord && registered && initialLoadQueued
                                        &&  (!reverseLoadFirst || !reverseLoadQueued)) {
                                    long ts = System.currentTimeMillis();
                                    List<TriggerRouter> triggerRouters = triggerRoutersByTargetNodeGroupId.get(targetNode.getNodeGroupId());
                                    if (triggerRouters == null) {
                                        triggerRouters = triggerRouterService.getAllTriggerRoutersForReloadForCurrentNode(parameterService.getNodeGroupId(), targetNode.getNodeGroupId());
                                        triggerRoutersByTargetNodeGroupId.put(targetNode.getNodeGroupId(), triggerRouters);
                                    }                                    
                                    dataService.insertReloadEvents(
                                            targetNode,
                                            false, processInfo, activeHistories, triggerRouters);
                                    isInitialLoadQueued = true;
                                    ts = System.currentTimeMillis() - ts;
                                    if (ts > Constants.LONG_OPERATION_THRESHOLD) {
                                        log.warn("Inserted reload events for node {} took longer than expected.  It took {} ms",
                                                security.getNodeId(), ts);
                                    } else {
                                        log.info("Inserted reload events for node {} in {} ms",
                                                security.getNodeId(), ts);
                                    }
                                }
                            } else {
                                List<NodeGroupLink> links = engine.getConfigurationService()
                                        .getNodeGroupLinksFor(parameterService.getNodeGroupId(),
                                                false);
                                if (links == null || links.size() == 0) {
                                    log.warn(
                                            "Could not queue up a load for {} because a node group link is NOT configured over which a load could be delivered",
                                            security.getNodeId());
                                } else {
                                    log.warn(
                                            "Could not queue up a load for {} because sync triggers has not yet run",
                                            security.getNodeId());
                                    if (!syncTriggersBeforeInitialLoadAttempted) {
                                        syncTriggersBeforeInitialLoadAttempted = true;
                                        engine.getTriggerRouterService().syncTriggers();
                                    }
                                }
                            }
                        }
                        if (isInitialLoadQueued) {
                            gapDetector.setFullGapAnalysis(true);
                        }
                    }
                    
                    processTableRequestLoads(identity, processInfo, triggerRoutersByTargetNodeGroupId);
                }
            }

            processInfo.setStatus(ProcessInfo.ProcessStatus.OK);
        } catch (Exception ex) {
            processInfo.setStatus(ProcessInfo.ProcessStatus.ERROR);
            log.error("", ex);
        }

    }

    public void processTableRequestLoads(Node source, ProcessInfo processInfo,  Map<String, List<TriggerRouter>> triggerRoutersByTargetNodeGroupId) {
        List<TableReloadRequest> loadsToProcess = engine.getDataService().getTableReloadRequestToProcess(source.getNodeId());
        if (loadsToProcess.size() > 0) {
            processInfo.setStatus(ProcessInfo.ProcessStatus.CREATING);
            log.info("Found " + loadsToProcess.size() + " table reload requests to process.");
            gapDetector.setFullGapAnalysis(true);
            
            boolean useExtractJob = parameterService.is(ParameterConstants.INITIAL_LOAD_USE_EXTRACT_JOB, true);
            boolean streamToFile = parameterService.is(ParameterConstants.STREAM_TO_FILE_ENABLED, false);

            Map<String, List<TableReloadRequest>> requestsSplitByLoad = new HashMap<String, List<TableReloadRequest>>();
            for (TableReloadRequest load : loadsToProcess) {
                Node targetNode = engine.getNodeService().findNode(load.getTargetNodeId());
                if (!useExtractJob || streamToFile) {
                    if (load.isFullLoadRequest() && isValidLoadTarget(load.getTargetNodeId())) {
                        List<TableReloadRequest> fullLoad = new ArrayList<TableReloadRequest>();
                        fullLoad.add(load);

                        List<TriggerRouter> triggerRouters = engine.getTriggerRouterService()
                                .getAllTriggerRoutersForReloadForCurrentNode(parameterService.getNodeGroupId(), targetNode.getNodeGroupId());

                        List<TriggerHistory> activeHistories = extensionService.getExtensionPoint(IReloadGenerator.class)
                                .getActiveTriggerHistories(targetNode);

                        engine.getDataService().insertReloadEvents(targetNode, false, fullLoad, processInfo, activeHistories, triggerRouters);
                    } else {
                        NodeSecurity targetNodeSecurity = engine.getNodeService().findNodeSecurity(load.getTargetNodeId());

                        boolean registered = targetNodeSecurity != null && (targetNodeSecurity.getRegistrationTime() != null
                                || targetNodeSecurity.getNodeId().equals(targetNodeSecurity.getCreatedAtNodeId()));
                        if (registered) {
                            // Make loads unique to the target and create time
                            String key = load.getTargetNodeId() + "::" + load.getCreateTime().toString();
                            if (!requestsSplitByLoad.containsKey(key)) {
                                requestsSplitByLoad.put(key, new ArrayList<TableReloadRequest>());
                            }
                            requestsSplitByLoad.get(key).add(load);
                        } else {
                            log.warn("There was a load queued up for '{}', but the node is not registered.  It is being ignored",
                                    load.getTargetNodeId());
                        }
                    }
                } else {
                    throw new SymmetricException(String.format("Node '%s' can't process load for '%s' because of confilcting parameters: %s=%s and %s=%s", 
                            source.getNodeId(), load.getTargetNodeId(), ParameterConstants.INITIAL_LOAD_USE_EXTRACT_JOB, useExtractJob, ParameterConstants.STREAM_TO_FILE_ENABLED,
                            streamToFile));
                }
            }
            
            for (Map.Entry<String, List<TableReloadRequest>> entry : requestsSplitByLoad.entrySet()) {
                Node targetNode = engine.getNodeService().findNode(entry.getKey().split("::")[0]);
                ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
                List<TriggerRouter> triggerRouters = triggerRoutersByTargetNodeGroupId.get(targetNode.getNodeGroupId());
                if (triggerRouters == null) {
                    triggerRouters = triggerRouterService.getAllTriggerRoutersForReloadForCurrentNode(parameterService.getNodeGroupId(), targetNode.getNodeGroupId());
                    triggerRoutersByTargetNodeGroupId.put(targetNode.getNodeGroupId(), triggerRouters);
                }
                List<TriggerHistory> activeHistories = extensionService.getExtensionPoint(IReloadGenerator.class).getActiveTriggerHistories(targetNode);
                
                engine.getDataService().insertReloadEvents(
                        targetNode,
                        false, entry.getValue(), processInfo, activeHistories, triggerRouters);
            }
        }
    }
    public boolean isValidLoadTarget(String targetNodeId) {
        boolean result = false;
        NodeSecurity targetNodeSecurity = engine.getNodeService().findNodeSecurity(targetNodeId);

        boolean reverseLoadFirst = parameterService.is(ParameterConstants.INITIAL_LOAD_REVERSE_FIRST);
        boolean registered = targetNodeSecurity.getRegistrationTime() != null;
        boolean reverseLoadQueued = targetNodeSecurity.isRevInitialLoadEnabled();

        if (registered && (!reverseLoadFirst || !reverseLoadQueued)) {
            result = true;
        } else {
            log.info("Unable to process load for target node id " + targetNodeId + " [registered: " + registered
                    + ", reverse load first: " + reverseLoadFirst + ", reverse load queued: " + reverseLoadQueued
                    + "]");
        }
        return result;
    }
    
    @Override
    public int countNodesThatHaveReloadsQueuedUp() {
        Set<String> nodeIds = new HashSet<>();
        IDataService dataService = engine.getDataService();
        INodeService nodeService = engine.getNodeService();
        String me = nodeService.findIdentityNodeId();        
        List<TableReloadRequest> toProcess = dataService.getTableReloadRequestToProcess(me);
        for (TableReloadRequest tableReloadRequest : toProcess) {
            nodeIds.add(tableReloadRequest.getTargetNodeId());
        }
        
        List<NodeSecurity> nodes = findNodesThatAreReadyForInitialLoad();
        for (NodeSecurity nodeSecurity : nodes) {
            nodeIds.add(nodeSecurity.getNodeId());
        }
        return nodeIds.size();
    }

    public List<NodeSecurity> findNodesThatAreReadyForInitialLoad() {
        INodeService nodeService = engine.getNodeService();
        IConfigurationService configurationService = engine.getConfigurationService();
        String me = nodeService.findIdentityNodeId();
        List<NodeSecurity> toReturn = new ArrayList<NodeSecurity>();
        List<NodeSecurity> securities = nodeService.findNodeSecurityWithLoadEnabled();
        for (NodeSecurity nodeSecurity : securities) {
            if (((!nodeSecurity.getNodeId().equals(me)
                    && nodeSecurity
                        .isInitialLoadEnabled())
                    || (!nodeSecurity.getNodeId().equals(me) && configurationService
                            .isMasterToMaster()) || (nodeSecurity.getNodeId().equals(me) && nodeSecurity
                    .isRevInitialLoadEnabled()))) {
                toReturn.add(nodeSecurity);
            }
        }
        return toReturn;
    }

    protected void sendReverseInitialLoad(ProcessInfo processInfo) {
        INodeService nodeService = engine.getNodeService();
        boolean queuedLoad = false;
        List<Node> nodes = new ArrayList<Node>();
        nodes.addAll(nodeService.findTargetNodesFor(NodeGroupLinkAction.P));
        nodes.addAll(nodeService.findTargetNodesFor(NodeGroupLinkAction.W));
        for (Node node : nodes) {
            engine.getDataService().insertReloadEvents(node, true, processInfo);
            queuedLoad = true;
        }

        if (!queuedLoad) {
            log.info("{} was enabled but no nodes were linked to load",
                    ParameterConstants.AUTO_RELOAD_REVERSE_ENABLED);
        }
    }

    /**
     * We route data channel by channel for two reasons. One is that if/when we
     * decide to multi-thread the routing it is a simple matter of inserting a
     * thread pool here and waiting for all channels to be processed. The other
     * reason is to reduce the number of connections we are required to have.
     */
    protected int routeDataForEachChannel() {
        int dataCount = 0;
        Node sourceNode = engine.getNodeService().findIdentity();
        ProcessInfo processInfo = engine.getStatisticManager().newProcessInfo(
                new ProcessInfoKey(sourceNode.getNodeId(), null, ProcessType.ROUTER_JOB));
        processInfo.setStatus(ProcessInfo.ProcessStatus.PROCESSING);
        try {
            final List<NodeChannel> channels = engine.getConfigurationService().getNodeChannels(false);
            Set<String> readyChannels = null;
            if (parameterService.is(ParameterConstants.ROUTING_QUERY_CHANNELS_FIRST)) {
                readyChannels = getReadyChannels();
            }
            for (NodeChannel nodeChannel : channels) {
                engine.getClusterService().refreshLock(ClusterConstants.ROUTE);
                if (nodeChannel.isEnabled() && (readyChannels == null || readyChannels.contains(nodeChannel.getChannelId()))) {
                    processInfo.setCurrentChannelId(nodeChannel.getChannelId());
                    dataCount += routeDataForChannel(processInfo, nodeChannel, sourceNode);
                } else if (!nodeChannel.isEnabled()) {
                    gapDetector.setIsAllDataRead(false);
                    if (log.isDebugEnabled()) {
                        log.debug("Not routing the {} channel.  It is either disabled or suspended.", nodeChannel.getChannelId());                            
                    }
                }
            }
            processInfo.setStatus(ProcessInfo.ProcessStatus.OK);
        } catch (RuntimeException ex) {
            processInfo.setStatus(ProcessInfo.ProcessStatus.ERROR);
            firstTimeCheck = true;
            throw ex;
        }
        return dataCount;
    }

    protected Set<String> getReadyChannels() {
        List<DataGap> dataGaps = gapDetector.getDataGaps();
        int dataIdSqlType = engine.getSymmetricDialect().getSqlTypeForIds();
        int numberOfGapsToQualify = parameterService.getInt(ParameterConstants.ROUTING_MAX_GAPS_TO_QUALIFY_IN_SQL, 100);
        int maxGapsBeforeGreaterThanQuery = parameterService.getInt(
                ParameterConstants.ROUTING_DATA_READER_THRESHOLD_GAPS_TO_USE_GREATER_QUERY, 100);
        String sql;
        Object[] args;
        int[] types;
        if (maxGapsBeforeGreaterThanQuery > 0 && dataGaps.size() > maxGapsBeforeGreaterThanQuery) {
            sql = getSql("selectChannelsUsingStartDataId");
            args = new Object[] { dataGaps.get(0).getStartId() };
            types = new int[] { dataIdSqlType };

        } else {
            sql = qualifyUsingDataGaps(dataGaps, numberOfGapsToQualify, getSql("selectChannelsUsingGapsSql"));            
            int numberOfArgs = 2 * (numberOfGapsToQualify < dataGaps.size() ? numberOfGapsToQualify : dataGaps.size());
            args = new Object[numberOfArgs];
            types = new int[numberOfArgs];

            for (int i = 0; i < numberOfGapsToQualify && i < dataGaps.size(); i++) {
                DataGap gap = dataGaps.get(i);
                args[i * 2] = gap.getStartId();
                types[i * 2] = dataIdSqlType;
                if ((i + 1) == numberOfGapsToQualify && (i + 1) < dataGaps.size()) {
                    args[i * 2 + 1] = dataGaps.get(dataGaps.size() - 1).getEndId();
                } else {
                    args[i * 2 + 1] = gap.getEndId();
                }
                types[i * 2 + 1] = dataIdSqlType;
            }

        }
        final Set<String> readyChannels = new HashSet<String>();
        sqlTemplateDirty.query(sql, new ISqlRowMapper<String>() {
            public String mapRow(Row row) {
                readyChannels.add(row.getString("channel_id"));
                return null;
            }                
        }, args, types);
        return readyChannels;
    }

    protected String qualifyUsingDataGaps(List<DataGap> dataGaps, int numberOfGapsToQualify,
            String sql) {
        StringBuilder gapClause = new StringBuilder();
        for (int i = 0; i < numberOfGapsToQualify && i < dataGaps.size(); i++) {
            if (i == 0) {
                gapClause.append("(");
            } else {
                gapClause.append(" or ");
            }
            gapClause.append("(data_id between ? and ?)");
        }
        gapClause.append(")");
        return FormatUtils.replace("dataRange", gapClause.toString(), sql);
    }

    protected boolean producesCommonBatches(Channel channel, String nodeGroupId, List<TriggerRouter> triggerRouters) {
        String channelId = channel.getChannelId();
        Boolean producesCommonBatches = commonBatchesLastKnownState.get(channelId);
        long cacheTime = parameterService.getLong(ParameterConstants.CACHE_CHANNEL_COMMON_BATCHES_IN_MS);
        if (producesCommonBatches == null || System.currentTimeMillis() - commonBatchesCacheTime > cacheTime) {
            producesCommonBatches = !Constants.CHANNEL_CONFIG.equals(channelId)
                    && !channel.isFileSyncFlag()
                    && !channel.isReloadFlag() 
                    && !Constants.CHANNEL_HEARTBEAT.equals(channelId) 
                             && !Constants.CHANNEL_MONITOR.equals(channelId);
            if (producesCommonBatches && triggerRouters != null) {
                List<TriggerRouter> testableTriggerRouters = new ArrayList<TriggerRouter>();
                for (TriggerRouter triggerRouter : triggerRouters) {
                    if (triggerRouter.getTrigger().getChannelId().equals(channelId)) {
                        testableTriggerRouters.add(triggerRouter);
                    } else {
                        /*
                         * This trigger is not on this channel. If there is
                         * another trigger on this channel for the same table
                         * AND this trigger is syncing to this node, then
                         * consider it to check on common batch mode
                         */
                        String anotherChannelTableName = triggerRouter.getTrigger()
                                .getFullyQualifiedSourceTableName();
                        for (TriggerRouter triggerRouter2 : triggerRouters) {
                            String currentTableName = triggerRouter2
                                    .getTrigger()
                                    .getFullyQualifiedSourceTableName();
                            String currentChannelId = triggerRouter2.getTrigger().getChannelId();
                            if (anotherChannelTableName.equals(currentTableName) && currentChannelId.equals(channelId)
                                    && triggerRouter.getRouter().getNodeGroupLink().getTargetNodeGroupId()
                                            .equals(triggerRouter2.getRouter().getNodeGroupLink().getSourceNodeGroupId()) &&
                                            triggerRouter.getRouter().getNodeGroupLink().getSourceNodeGroupId()
                                            .equals(triggerRouter2.getRouter().getNodeGroupLink().getTargetNodeGroupId())) {
                                testableTriggerRouters.add(triggerRouter);
                            }
                        }
                    }
                }         
                for (TriggerRouter triggerRouter : testableTriggerRouters) {
                    boolean isDefaultRouter = "default".equals(triggerRouter.getRouter().getRouterType());
                    /*
                     * If the data router is not a default data router or there will
                     * be incoming data on the channel where sync_on_incoming_batch
                     * is on, then we can not do 'optimal' routing. When
                     * sync_on_incoming_batch is on, then we might not be sending
                     * data to all nodes in a node_group. We can only do 'optimal'
                     * routing if data is going to go to all nodes in a group.
                     */
                    if (triggerRouter.getRouter().getNodeGroupLink().getSourceNodeGroupId()
                            .equals(nodeGroupId)) {
                        if (!isDefaultRouter) {
                            producesCommonBatches = false;
                            break;
                        } else {
                            if (triggerRouter.getTrigger().isSyncOnIncomingBatch()) {
                                String outgoingTableName = triggerRouter.getTrigger()
                                        .getFullyQualifiedSourceTableName();
                                for (TriggerRouter triggerRouter2 : testableTriggerRouters) {
                                    String incomingTableName = triggerRouter2.getTrigger().getFullyQualifiedSourceTableName();
                                    String targetNodeGroupId = triggerRouter2.getRouter().getNodeGroupLink()
                                            .getTargetNodeGroupId();
                                    if (incomingTableName.equals(outgoingTableName) && targetNodeGroupId.equals(nodeGroupId)) {
                                        producesCommonBatches = false;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
    
            if (!producesCommonBatches.equals(commonBatchesLastKnownState.get(channelId))) {
                if (producesCommonBatches) {
                    log.info("The '{}' channel is in common batch mode", channelId);
                } else {
                    log.info("The '{}' channel is NOT in common batch mode", channelId);
                }
                commonBatchesLastKnownState.put(channelId, producesCommonBatches);
            }
            commonBatchesCacheTime = System.currentTimeMillis();
        }
        return producesCommonBatches;
    }
    
    protected boolean onlyDefaultRoutersAssigned(Channel channel, String nodeGroupId, List<TriggerRouter> triggerRouters) {
        String channelId = channel.getChannelId();
        Boolean onlyDefaultRoutersAssigned = defaultRouterOnlyLastKnownState.get(channelId);
        long cacheTime = parameterService.getLong(ParameterConstants.CACHE_CHANNEL_DEFAULT_ROUTER_IN_MS);
        if (onlyDefaultRoutersAssigned == null || System.currentTimeMillis() - defaultRoutersCacheTime > cacheTime) {
            onlyDefaultRoutersAssigned = !Constants.CHANNEL_CONFIG.equals(channelId)
                    && !channel.isFileSyncFlag()
                    && !channel.isReloadFlag() 
                    && !Constants.CHANNEL_HEARTBEAT.equals(channelId)
                    && !Constants.CHANNEL_MONITOR.equals(channelId);
            if (onlyDefaultRoutersAssigned && triggerRouters != null) {           
                for (TriggerRouter triggerRouter : triggerRouters) {
                    if (triggerRouter.getTrigger().getChannelId().equals(channel.getChannelId()) &&
                            triggerRouter.getRouter().getNodeGroupLink().getSourceNodeGroupId()
                            .equals(nodeGroupId) && !"default".equals(triggerRouter.getRouter().getRouterType())) {
                        onlyDefaultRoutersAssigned = false;
                    } 
                }         
            }
    
            if (!onlyDefaultRoutersAssigned.equals(defaultRouterOnlyLastKnownState.get(channelId))) {
                if (onlyDefaultRoutersAssigned) {
                    log.info("The '{}' channel for the '{}' node group has only default routers assigned to it.  Change data won't be selected during routing", channelId, nodeGroupId);
                } 
                defaultRouterOnlyLastKnownState.put(channelId, onlyDefaultRoutersAssigned);
            }
            defaultRoutersCacheTime = System.currentTimeMillis();
        }
        return onlyDefaultRoutersAssigned;
    }

    protected int routeDataForChannel(ProcessInfo processInfo, final NodeChannel nodeChannel, final Node sourceNode) {
        ChannelRouterContext context = null;
        long ts = System.currentTimeMillis();
        int dataCount = -1;
        try {
            List<TriggerRouter> triggerRouters = engine.getTriggerRouterService().getTriggerRouters(false);
            boolean producesCommonBatches = producesCommonBatches(nodeChannel.getChannel(), parameterService.getNodeGroupId(),
                    triggerRouters);
            boolean onlyDefaultRoutersAssigned = onlyDefaultRoutersAssigned(nodeChannel.getChannel(),
                    parameterService.getNodeGroupId(), triggerRouters);
            
            context = new ChannelRouterContext(sourceNode.getNodeId(), nodeChannel,
                    symmetricDialect.getPlatform().getSqlTemplate().startSqlTransaction());
            context.setProduceCommonBatches(producesCommonBatches);
            context.setOnlyDefaultRoutersAssigned(onlyDefaultRoutersAssigned);
            context.setDataGaps(gapDetector.getDataGaps());

            dataCount = selectDataAndRoute(processInfo, context);
            return dataCount;
        } catch (DelayRoutingException ex) {
            log.info("The routing process for the {} channel is being delayed.  {}", nodeChannel.getChannelId(), isNotBlank(ex.getMessage()) ? ex.getMessage() : "");
            if (context != null) {
                context.rollback();
            }
            return 0;
        } catch (InterruptedException ex) {
            log.warn("The routing process was interrupted.  Rolling back changes");
            if (context != null) {
                context.rollback();
            }
            return 0;
        } catch (SyntaxParsingException ex) {
            log.error(
                    String.format(
                            "Failed to route and batch data on '%s' channel due to an invalid router expression",
                            nodeChannel.getChannelId()), ex);
            if (context != null) {
                context.rollback();
            }
            return 0;
        } catch (Throwable ex) {
            log.error(
                    String.format("Failed to route and batch data on '%s' channel",
                            nodeChannel.getChannelId()), ex);
            if (context != null) {
                context.rollback();
            }
            return 0;
        } finally {
            try {
                if (dataCount > 0) {
                    long insertTs = System.currentTimeMillis();
                    engine.getDataService().insertDataEvents(context.getSqlTransaction(),
                            context.getDataEventList());
                    context.clearDataEventsList();
                    completeBatchesAndCommit(context);
                    gapDetector.addDataIds(context.getDataIds());
                    gapDetector.setIsAllDataRead(context.getDataIds().size() < context.getChannel().getMaxDataToRoute());
                    context.incrementStat(System.currentTimeMillis() - insertTs,
                            ChannelRouterContext.STAT_INSERT_DATA_EVENTS_MS);

                    if (parameterService.is(ParameterConstants.ROUTING_COLLECT_STATS_UNROUTED)) {
                        Data lastDataProcessed = context.getLastDataProcessed();
                        if (lastDataProcessed != null && lastDataProcessed.getDataId() > 0) {
                            String channelId = nodeChannel.getChannelId();
                            long queryTs = System.currentTimeMillis();
                            long dataLeftToRoute = sqlTemplate.queryForInt(
                                    getSql("selectUnroutedCountForChannelSql"), channelId,
                                    lastDataProcessed.getDataId());
                            queryTs = System.currentTimeMillis() - queryTs;
                            if (queryTs > Constants.LONG_OPERATION_THRESHOLD) {
                                log.warn("Unrouted query for channel {} took longer than expected. The query took {} ms.", channelId, queryTs);
                            }
                            engine.getStatisticManager().setDataUnRouted(channelId, dataLeftToRoute);
                        }
                    }
                } else {
                    gapDetector.setIsAllDataRead(false);
                }
            } catch (Exception e) {
                if (context != null) {
                    context.rollback();
                }
                log.error("", e);
            } finally {
                long totalTime = System.currentTimeMillis() - ts;
                if (context != null) {
	                context.incrementStat(totalTime, ChannelRouterContext.STAT_ROUTE_TOTAL_TIME);
	                context.logStats(log, totalTime);
	                context.cleanup();
                }
            }
        }
    }

    protected void completeBatchesAndCommit(ChannelRouterContext context) {
        Set<IDataRouter> usedRouters = new HashSet<IDataRouter>(context.getUsedDataRouters());
        List<OutgoingBatch> batches = new ArrayList<OutgoingBatch>(context.getBatchesByNodes()
                .values());

        gapDetector.setFullGapAnalysis(context.getSqlTransaction(), true);

        if (engine.getParameterService().is(ParameterConstants.ROUTING_LOG_STATS_ON_BATCH_ERROR)) {
            engine.getStatisticManager().addRouterStats(context.getStartDataId(), context.getEndDataId(), 
                    context.getDataReadCount(), context.getPeekAheadFillCount(),
                    context.getDataGaps(), null, batches);
        }

        for (OutgoingBatch batch : batches) {
            batch.setRouterMillis(System.currentTimeMillis() - batch.getCreateTime().getTime());
            for (IDataRouter dataRouter : usedRouters) {
                dataRouter.completeBatch(context, batch);
            }
            if (Constants.UNROUTED_NODE_ID.equals(batch.getNodeId())) {
                batch.setStatus(Status.OK);
            } else {
                batch.setStatus(Status.NE);
            }
            engine.getOutgoingBatchService().updateOutgoingBatch(context.getSqlTransaction(), batch);
            context.getBatchesByNodes().remove(batch.getNodeId());
        }
        
        context.commit();

        for (IDataRouter dataRouter : usedRouters) {
            dataRouter.contextCommitted(context);
        }
        context.setNeedsCommitted(false);
    }

    protected Set<Node> findAvailableNodes(TriggerRouter triggerRouter, ChannelRouterContext context) {
        Set<Node> nodes = context.getAvailableNodes().get(triggerRouter);
        if (nodes == null) {
            nodes = new HashSet<Node>();
            Router router = triggerRouter.getRouter();
            NodeGroupLink link = engine.getConfigurationService().getNodeGroupLinkFor(
                    router.getNodeGroupLink().getSourceNodeGroupId(),
                    router.getNodeGroupLink().getTargetNodeGroupId(), false);
            if (link != null) {
                nodes.addAll(engine.getNodeService().findEnabledNodesFromNodeGroup(
                        router.getNodeGroupLink().getTargetNodeGroupId()));
            } else {
                log.error("The router {} has no node group link configured from {} to {}",
                        new Object[] { router.getRouterId(),
                                router.getNodeGroupLink().getSourceNodeGroupId(),
                                router.getNodeGroupLink().getTargetNodeGroupId() });
            }
            context.getAvailableNodes().put(triggerRouter, nodes);
        }
        
        return engine.getGroupletService().getTargetEnabled(triggerRouter, nodes);
    }

	protected IDataToRouteReader startReading(ChannelRouterContext context) {
		IDataToRouteReader reader = new DataGapRouteReader(context, engine);
		if (parameterService.is(ParameterConstants.SYNCHRONIZE_ALL_JOBS)) {
			reader.run();
		} else {
			if (readThread == null) {
				readThread = Executors.newCachedThreadPool(new ThreadFactory() {
					final AtomicInteger threadNumber = new AtomicInteger(1);
					final String namePrefix = parameterService.getEngineName()
							.toLowerCase() + "-router-reader-";

					public Thread newThread(Runnable r) {
						Thread t = new Thread(r);
						t.setName(namePrefix + threadNumber.getAndIncrement());
						if (t.isDaemon()) {
							t.setDaemon(false);
						}
						if (t.getPriority() != Thread.NORM_PRIORITY) {
							t.setPriority(Thread.NORM_PRIORITY);
						}
						return t;
					}
				});
			}
			readThread.execute(reader);
		}

		return reader;
	}

    /**
     * Pre-read data and fill up a queue so we can peek ahead to see if we have
     * crossed a database transaction boundary. Then route each {@link Data}
     * while continuing to keep the queue filled until the result set is
     * entirely read.
     * 
     * @param conn
     *            The connection to use for selecting the data.
     * @param context
     *            The current context of the routing process
     */
    protected int selectDataAndRoute(ProcessInfo processInfo, ChannelRouterContext context) throws InterruptedException {
        IDataToRouteReader reader = startReading(context);
        Data data = null;
        Data nextData = null;
        int totalDataCount = 0;
        int totalDataEventCount = 0;
        int statsDataCount = 0;
        int statsDataEventCount = 0;
        final int maxNumberOfEventsBeforeFlush = parameterService
                .getInt(ParameterConstants.ROUTING_FLUSH_JDBC_BATCH_SIZE);
        try {
            long ts = System.currentTimeMillis();
            long startTime = System.currentTimeMillis();
            nextData = reader.take();
            do {
                if (nextData != null) {
                    data = nextData;
                    nextData = reader.take();
                    if (data != null) {
                        processInfo.setCurrentTableName(data.getTableName());
                        processInfo.incrementCurrentDataCount();
                        boolean atTransactionBoundary = false;
                        if (nextData != null) {
                            String nextTxId = nextData.getTransactionId();
                            atTransactionBoundary = nextTxId == null
                                    || !nextTxId.equals(data.getTransactionId());
                        }
                        context.setEncountedTransactionBoundary(atTransactionBoundary);
                        statsDataCount++;
                        totalDataCount++;
                        int dataEventsInserted = routeData(processInfo, data, context);
                        statsDataEventCount += dataEventsInserted;
                        totalDataEventCount += dataEventsInserted;
                        long insertTs = System.currentTimeMillis();
                        try {
                            if (maxNumberOfEventsBeforeFlush <= context.getDataEventList().size()
                                    || context.isNeedsCommitted()) {
                                engine.getDataService().insertDataEvents(
                                        context.getSqlTransaction(), context.getDataEventList());
                                context.clearDataEventsList();
                            }
                            if (context.isNeedsCommitted()) {
                                completeBatchesAndCommit(context);
                            }
                        } finally {
                            context.incrementStat(System.currentTimeMillis() - insertTs,
                                    ChannelRouterContext.STAT_INSERT_DATA_EVENTS_MS);

                            if (statsDataCount > StatisticConstants.FLUSH_SIZE_ROUTER_DATA) {
                                engine.getStatisticManager().incrementDataRouted(
                                        context.getChannel().getChannelId(), statsDataCount);
                                statsDataCount = 0;
                                engine.getStatisticManager().incrementDataEventInserted(
                                        context.getChannel().getChannelId(), statsDataEventCount);
                                statsDataEventCount = 0;
                            }
                        }
              
                        long routeTs = System.currentTimeMillis() - ts;
                        if (routeTs > LOG_PROCESS_SUMMARY_THRESHOLD) {
                            engine.getClusterService().refreshLock(ClusterConstants.ROUTE);
                            log.info(
                                    "Routing for channel '{}' has been processing for {} seconds. The following stats have been gathered: "
                                            + "totalDataRoutedCount={}, totalDataEventCount={}, startDataId={}, endDataId={}, dataReadCount={}, peekAheadFillCount={}, dataGaps={}",
                                    new Object[] {  context.getChannel().getChannelId(), ((System.currentTimeMillis()-startTime) / 1000), totalDataCount, totalDataEventCount, context.getStartDataId(),
                                            context.getEndDataId(), context.getDataReadCount(), context.getPeekAheadFillCount(), 
                                            StringUtils.abbreviate(context.getDataGaps().toString(), MAX_LOGGING_LENGTH)
                                            });
                            ts = System.currentTimeMillis();
                        }

                        context.setLastDataProcessed(data);
                    }
                } else {
                    data = null;
                }
            } while (data != null);
            
            long routeTime = System.currentTimeMillis() - startTime;
            if (routeTime > 60000 && context != null) {
                log.info(
                        "Done routing for channel '{}' which took {} seconds",
                        new Object[] { context.getChannel().getChannelId(), ((System.currentTimeMillis() - startTime) / 1000) });
                ts = System.currentTimeMillis();
            }

        } finally {
            reader.setReading(false);
            if (statsDataCount > 0) {
                engine.getStatisticManager().incrementDataRouted(
                        context.getChannel().getChannelId(), statsDataCount);
            }
            if (statsDataEventCount > 0) {
                engine.getStatisticManager().incrementDataEventInserted(
                        context.getChannel().getChannelId(), statsDataEventCount);
            }
        }
        context.incrementStat(totalDataCount, ChannelRouterContext.STAT_DATA_ROUTED_COUNT);
        return totalDataEventCount;

    }

    @SuppressWarnings("unchecked")
    protected int routeData(ProcessInfo processInfo, Data data, ChannelRouterContext context) {
        int numberOfDataEventsInserted = 0;
        List<TriggerRouter> triggerRouters = getTriggerRoutersForData(data);
        Table table = symmetricDialect.getTable(data.getTriggerHistory(), true);
        if (table == null) {
        	table = buildTableFromTriggerHistory(data.getTriggerHistory());
        }
        if (triggerRouters != null && triggerRouters.size() > 0) {
            for (TriggerRouter triggerRouter : triggerRouters) {
                DataMetaData dataMetaData = new DataMetaData(data, table, triggerRouter.getRouter(),
                        context.getChannel());
                Collection<String> nodeIds = null;
                if (!context.getChannel().isIgnoreEnabled()
                        && triggerRouter.isRouted(data.getDataEventType())) {

                    String targetNodeIds = data.getNodeList();
                    if (StringUtils.isNotBlank(targetNodeIds)) {
                        List<String> targetNodeIdsList = Arrays.asList(targetNodeIds.split(","));
                        nodeIds = CollectionUtils.intersection(targetNodeIdsList, toNodeIds(findAvailableNodes(triggerRouter, context)));

                        if (nodeIds.size() == 0) {
                            log.info(
                                    "None of the target nodes specified in the data.node_list field ({}) were qualified nodes. Data id {} for table '{}' will not be routed using the {} router",
                                    new Object[] {targetNodeIds, data.getDataId(), data.getTableName(), triggerRouter.getRouter().getRouterId() });
                        }
                    } else {
                        try {
                            IDataRouter dataRouter = getDataRouter(triggerRouter.getRouter());
                            context.addUsedDataRouter(dataRouter);
                            long ts = System.currentTimeMillis();
                            nodeIds = dataRouter.routeToNodes(context, dataMetaData,
                                    findAvailableNodes(triggerRouter, context), false, false,
                                    triggerRouter);
                            context.incrementStat(System.currentTimeMillis() - ts,
                                    ChannelRouterContext.STAT_DATA_ROUTER_MS);
                        } catch (DelayRoutingException ex) {
                            throw ex;
                        } catch (RuntimeException ex) {
                            StringBuilder failureMessage = new StringBuilder(
                                    "Failed to route data: ");
                            failureMessage.append(data.getDataId());
                            failureMessage.append(" for table: ");
                            failureMessage.append(data.getTableName());
                            failureMessage.append(".\n");
                            data.writeCsvDataDetails(failureMessage);
                            throw new SymmetricException(failureMessage.toString(), ex);
                        }
                    }

                    if (nodeIds != null) {
                        if (!triggerRouter.isPingBackEnabled() && data.getSourceNodeId() != null) {
                            nodeIds.remove(data.getSourceNodeId());
                        }

                        // should never route to self
                        nodeIds.remove(engine.getNodeService().findIdentityNodeId());

                    }
                }

                numberOfDataEventsInserted += insertDataEvents(processInfo, context, dataMetaData, nodeIds);
            }

        } else {
            log.warn(
                    "Could not find trigger routers for trigger history id of {} (table {}).  "
                    + "Data with the id of {} and channel id {} will be assigned to an unrouted batch. "
                    + "There is a good chance that data was captured and the trigger router link was removed before the data could be routed, or "
                    + "that there is an orphaned symmetric trigger on the table.",
                    data.getTriggerHistory().getTriggerHistoryId(), data.getTableName(), data.getDataId(), data.getChannelId());
            numberOfDataEventsInserted += insertDataEvents(processInfo, context, new DataMetaData(data, table,
                    null, context.getChannel()), new HashSet<String>(0));
        }

        context.incrementStat(numberOfDataEventsInserted,
                ChannelRouterContext.STAT_DATA_EVENTS_INSERTED);
        return numberOfDataEventsInserted;

    }

    protected int insertDataEvents(ProcessInfo processInfo, ChannelRouterContext context, DataMetaData dataMetaData,
            Collection<String> nodeIds) {
        int numberOfDataEventsInserted = 0;
        if (nodeIds == null || nodeIds.size() == 0) {
            nodeIds = new HashSet<String>(1);
            nodeIds.add(Constants.UNROUTED_NODE_ID);
        }
        long ts = System.currentTimeMillis();
        long batchIdToReuse = -1;
        boolean dataEventAdded = false;
        for (String nodeId : nodeIds) {
            if (nodeId != null) {
                Map<String, OutgoingBatch> batches = context.getBatchesByNodes();
                OutgoingBatch batch = batches.get(nodeId);
                if (batch == null) {
                    batch = new OutgoingBatch(nodeId, dataMetaData.getNodeChannel().getChannelId(),
                            Status.RT);
                    batch.setBatchId(batchIdToReuse);
                    batch.setCommonFlag(context.isProduceCommonBatches());
                    
					log.debug(
							"About to insert a new batch for node {} on the '{}' channel.  Batches in progress are: {}.",
							new Object[] { nodeId, batch.getChannelId(),
									context.getBatchesByNodes().values() });

                    engine.getOutgoingBatchService().insertOutgoingBatch(batch);
                    processInfo.incrementBatchCount();
                    context.getBatchesByNodes().put(nodeId, batch);

                    // if in reuse mode, then share the batch id
                    if (context.isProduceCommonBatches()) {
                        batchIdToReuse = batch.getBatchId();
                    }
                }
                
                if (dataMetaData.getData().getDataEventType() == DataEventType.RELOAD) {
                    long loadId = context.getLastLoadId();
                    if (loadId < 0) {
                        loadId = engine.getSequenceService().nextVal(context.getSqlTransaction(), Constants.SEQUENCE_OUTGOING_BATCH_LOAD_ID);
                        context.setLastLoadId(loadId);
                    }
                    batch.setLoadId(loadId);
                    if (context.getChannel().isReloadFlag()) {
                        context.setNeedsCommitted(true);
                    }
                } else {
                    context.setLastLoadId(-1);
                }

                batch.incrementRowCount(dataMetaData.getData().getDataEventType());
                batch.incrementDataRowCount();
                batch.incrementTableCount(dataMetaData.getTable().getNameLowerCase());
                
                if (!context.isProduceCommonBatches()
                        || (context.isProduceCommonBatches() && !dataEventAdded)) {
                    Router router = dataMetaData.getRouter();
                    context.addDataEvent(dataMetaData.getData().getDataId(), batch.getBatchId(),
                            router != null ? router.getRouterId()
                                    : Constants.UNKNOWN_ROUTER_ID);
                    numberOfDataEventsInserted++;
                    dataEventAdded = true;
                }
                Map<String, IBatchAlgorithm> batchAlgorithms = extensionService.getExtensionPointMap(IBatchAlgorithm.class);
                if (batchAlgorithms.get(context.getChannel().getBatchAlgorithm()).isBatchComplete(
                        batch, dataMetaData, context)) {
                    context.setNeedsCommitted(true);
                }
            }
        }
        context.incrementStat(System.currentTimeMillis() - ts,
                ChannelRouterContext.STAT_INSERT_DATA_EVENTS_MS);
        return numberOfDataEventsInserted;
    }

    protected IDataRouter getDataRouter(Router router) {
        IDataRouter dataRouter = null;
        Map<String, IDataRouter> routers = getRouters();
        if (!StringUtils.isBlank(router.getRouterType())) {
            dataRouter = routers.get(router.getRouterType());
            if (dataRouter == null) {
                log.warn(
                        "Could not find configured router type of {} with the id of {}. Defaulting the router",
                        router.getRouterType(), router.getRouterId());
            }
        }

        if (dataRouter == null) {
            return getRouters().get("default");
        }
        return dataRouter;
    }

    protected List<TriggerRouter> getTriggerRoutersForData(Data data) {
        List<TriggerRouter> triggerRouters = null;
        if (data != null) {
            if (data.getTriggerHistory() != null) {
                triggerRouters = engine.getTriggerRouterService()
                        .getTriggerRoutersForCurrentNode(false)
                        .get((data.getTriggerHistory().getTriggerId()));
                if (triggerRouters == null && data.getTriggerHistory().getTriggerId() != null && data.getTriggerHistory().getTriggerId().equals(AbstractFileParsingRouter.TRIGGER_ID_FILE_PARSER)) {
                	TriggerRouter dynamicTriggerRouter = new TriggerRouter();
                	String routerId = AbstractFileParsingRouter.getRouterIdFromExternalData(data.getExternalData());
                	dynamicTriggerRouter.setRouter(engine.getTriggerRouterService().getRouterById(routerId));
                	dynamicTriggerRouter.setTrigger(new Trigger());
                	triggerRouters = new ArrayList<TriggerRouter>();
                	triggerRouters.add(dynamicTriggerRouter);
                	data.setDataEventType(DataEventType.INSERT);
                }
                if (triggerRouters == null || triggerRouters.size() == 0) {
                    triggerRouters = engine.getTriggerRouterService()
                            .getTriggerRoutersForCurrentNode(true)
                            .get((data.getTriggerHistory().getTriggerId()));
                }
            } else {
                log.warn(
                        "Could not find a trigger hist record for recorded data {}.  Was the trigger hist record deleted manually?",
                        data.getDataId());
            }
        }
        return triggerRouters;
    }

    public long getUnroutedDataCount() {
        long maxDataIdAlreadyRouted = sqlTemplateDirty
                .queryForLong(getSql("selectLastDataIdRoutedUsingDataGapSql"));
        long leftToRoute = engine.getDataService().findMaxDataId() - maxDataIdAlreadyRouted;
        
        if (leftToRoute > 0) {
            return leftToRoute;
        } else {
            return 0;
        }
    }

    public List<String> getAvailableBatchAlgorithms() {
        return new ArrayList<String>(extensionService.getExtensionPointMap(IBatchAlgorithm.class).keySet());
    }

    public Map<String, IDataRouter> getRouters() {
        return extensionService.getExtensionPointMap(IDataRouter.class);
    }

    public List<DataGap> getDataGaps() {
        return gapDetector.getDataGaps();
    }

    protected Table buildTableFromTriggerHistory(TriggerHistory triggerHistory) {
    	Table table = new Table(triggerHistory.getSourceCatalogName(), triggerHistory.getSourceSchemaName(), triggerHistory.getSourceTableName());
    	String[] columnNames = triggerHistory.getColumnNames().split(",");
    	for (String columnName : columnNames) {
    		table.addColumn(new Column(columnName));
    	}
    	return table;
    }

    @Override
    public Set<Channel> getCommomBatchChannels(List<Channel> channels, String nodeGroupId, List<TriggerRouter> triggerRouters) {
        Set<Channel> commonBatchChannels = new HashSet<Channel>();
        
        for (Channel channel : channels) {
            if (producesCommonBatches(channel, nodeGroupId, triggerRouters)) {
                commonBatchChannels.add(channel);
            }
        }
        return commonBatchChannels;
    }
}
