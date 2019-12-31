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
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.SyntaxParsingException;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ContextConstants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.ProtocolException;
import org.jumpmind.symmetric.model.AbstractBatch.Status;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessType;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.route.AbstractFileParsingRouter;
import org.jumpmind.symmetric.route.AuditTableDataRouter;
import org.jumpmind.symmetric.route.BshDataRouter;
import org.jumpmind.symmetric.route.CSVRouter;
import org.jumpmind.symmetric.route.ChannelRouterContext;
import org.jumpmind.symmetric.route.ColumnMatchDataRouter;
import org.jumpmind.symmetric.route.CommonBatchCollisionException;
import org.jumpmind.symmetric.route.ConfigurationChangedDataRouter;
import org.jumpmind.symmetric.route.ConvertToReloadRouter;
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
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.statistic.StatisticConstants;
import org.jumpmind.symmetric.util.CounterStat;
import org.jumpmind.util.FormatUtils;

/**
 * @see IRouterService
 */
public class RouterService extends AbstractService implements IRouterService {
    
    final int MAX_LOGGING_LENGTH = 512;

    protected Map<Integer, CounterStat> missingTriggerRouter = new HashMap<Integer, CounterStat>();
    
    protected Map<String, CounterStat> invalidRouterType = new HashMap<String, CounterStat>();
    
    protected long triggerRouterCacheTime = 0;

    protected Map<String, Boolean> commonBatchesLastKnownState = new HashMap<String, Boolean>();
    
    protected long commonBatchesCacheTime;

    protected Map<String, Boolean> defaultRouterOnlyLastKnownState = new HashMap<String, Boolean>();
    
    protected long defaultRoutersCacheTime;

    protected transient ExecutorService readThread = null;

    protected ISymmetricEngine engine;
    
    protected IExtensionService extensionService;
    
    protected DataGapDetector gapDetector;
    
    protected boolean firstTimeCheck = true;
    
    protected boolean hasMaxDataRoutedOnChannel;

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
        extensionService.addExtensionPoint(ConvertToReloadRouter.ROUTER_ID, new ConvertToReloadRouter(engine));

        setSqlMap(new RouterServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
        
        gapDetector = new DataGapFastDetector(engine.getDataService(), parameterService, engine.getContextService(), 
                symmetricDialect, this, engine.getStatisticManager(), engine.getNodeService());
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
                    
                    do {
                        engine.getClusterService().refreshLock(ClusterConstants.ROUTE);
    
                        long ts = System.currentTimeMillis();
                        hasMaxDataRoutedOnChannel = false;
                        gapDetector.beforeRouting();
                        
                        dataCount = routeDataForEachChannel();
                        ts = System.currentTimeMillis() - ts;
                        if (dataCount > 0 || ts > Constants.LONG_OPERATION_THRESHOLD) {
                            log.info("Routed {} data events in {} ms", dataCount, ts);
                        }
                        if (dataCount > 0) {
                            gapDetector.afterRouting();
                        }
                        if (hasMaxDataRoutedOnChannel) {
                            log.debug("Immediately routing again because a channel reached max data to route");
                        }
                    } while (hasMaxDataRoutedOnChannel);

                } finally {
                    if (!force) {
                        engine.getClusterService().unlock(ClusterConstants.ROUTE);
                    }
                    
                    for (CounterStat counterStat : invalidRouterType.values()) {
                        Router router = (Router) counterStat.getObject();
                        log.warn("Invalid router type of '{}' configured on router '{}'.  Using default router instead.",
                                router.getRouterType(), router.getRouterId());
                    }
                    invalidRouterType.clear();

                    for (CounterStat counterStat : missingTriggerRouter.values()) {
                        Data data = (Data) counterStat.getObject();
                        log.warn("Ignoring data captured for table '{}' because there is no trigger router configured for it.  "
                                + "If you removed or disabled the trigger router, you can disregard this warning.  "
                                + "Starting with data id {} and trigger hist id {}, there were {} occurrences.",
                                data.getTableName(), data.getDataId(), data.getTriggerHistory().getTriggerHistoryId(), counterStat.getCount());
                    }
                    missingTriggerRouter.clear();
                }
            }
        }
        return dataCount;
    }

    /**
     * We route data channel by channel for two reasons. One is that if/when we
     * decide to multi-thread the routing it is a simple matter of inserting a
     * thread pool here and waiting for all channels to be processed. The other
     * reason is to reduce the number of connections we are required to have.
     */
    protected long routeDataForEachChannel() {
        long dataCount = 0;
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
                    processInfo.setCurrentTableName("");
                    processInfo.setCurrentChannelId(nodeChannel.getChannelId());
                    dataCount += routeDataForChannel(processInfo, nodeChannel, sourceNode, false, null, null);
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

    @Deprecated
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
                String message = "The '{}' channel is " + (producesCommonBatches ? "" : "NOT ") + "in common batch mode";
                if (channelId.equals(Constants.CHANNEL_CONFIG) || channelId.equals(Constants.CHANNEL_HEARTBEAT) ||
                        channelId.equals(Constants.CHANNEL_FILESYNC) || channelId.equals(Constants.CHANNEL_MONITOR) ||
                        channelId.equals(Constants.CHANNEL_FILESYNC_RELOAD) || channelId.equals(Constants.CHANNEL_RELOAD)) {
                    log.debug(message, channelId);
                } else {
                    log.info(message, channelId);
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

    protected long routeDataForChannel(ProcessInfo processInfo, final NodeChannel nodeChannel, final Node sourceNode, boolean isOverrideContainsBigLob,
            Map<String, OutgoingBatch> overrideBatchesByNodes, Map<Integer, Map<String, OutgoingBatch>> overrideBatchesByGroups) {
        ChannelRouterContext context = null;
        long ts = System.currentTimeMillis();
        long dataCount = -1;
        try {
            boolean useCommonGroups = parameterService.is(ParameterConstants.ROUTING_USE_COMMON_GROUPS);
            List<TriggerRouter> triggerRouters = engine.getTriggerRouterService().getTriggerRouters(false);
            boolean producesCommonBatches = !useCommonGroups && producesCommonBatches(nodeChannel.getChannel(), parameterService.getNodeGroupId(),
                    triggerRouters);
            boolean onlyDefaultRoutersAssigned = onlyDefaultRoutersAssigned(nodeChannel.getChannel(),
                    parameterService.getNodeGroupId(), triggerRouters);
            
            context = new ChannelRouterContext(sourceNode.getNodeId(), nodeChannel,
                    symmetricDialect.getPlatform().getSqlTemplate().startSqlTransaction(),
                    extensionService.getExtensionPointMap(IBatchAlgorithm.class));
            context.setProduceCommonBatches(producesCommonBatches);
            context.setProduceGroupBatches(useCommonGroups);
            context.setOnlyDefaultRoutersAssigned(onlyDefaultRoutersAssigned);
            context.setDataGaps(gapDetector.getDataGaps());
            context.setOverrideContainsBigLob(isOverrideContainsBigLob);
            
            if (overrideBatchesByNodes != null) {
                context.getBatchesByNodes().putAll(overrideBatchesByNodes);
            }
            if (overrideBatchesByNodes != null) {
                context.getBatchesByGroups().putAll(overrideBatchesByGroups);
            }

            dataCount = selectDataAndRoute(processInfo, context);
            return dataCount;
        } catch (DelayRoutingException ex) {
            log.info("The routing process for the {} channel is being delayed.  {}", nodeChannel.getChannelId(), isNotBlank(ex.getMessage()) ? ex.getMessage() : "");
            if (context != null) {
                context.rollback();
            }
            return context.getCommittedDataEventCount();
        } catch (InterruptedException ex) {
            log.warn("The routing process was interrupted.  Rolling back changes");
            if (context != null) {
                context.rollback();
            }
            return context.getCommittedDataEventCount();
        } catch (SyntaxParsingException ex) {
            log.error(
                    String.format(
                            "Failed to route and batch data on '%s' channel due to an invalid router expression",
                            nodeChannel.getChannelId()), ex);
            if (context != null) {
                context.rollback();
            }
            return context.getCommittedDataEventCount();
        } catch (ProtocolException ex) {
            if (context != null) {
                context.rollback();
            }
            if (isOverrideContainsBigLob) {
                log.error(String.format("Failed to route and batch data on '%s' channel", nodeChannel.getChannelId()), ex);
                return context.getCommittedDataEventCount();
            } else {
                Map<String, OutgoingBatch> batchesByNodes = new HashMap<String, OutgoingBatch>(context.getBatchesByNodes());
                Map<Integer, Map<String, OutgoingBatch>> batchesByGroups = new HashMap<Integer, Map<String, OutgoingBatch>>(context.getBatchesByGroups());
                long batchId = -1;
                for (OutgoingBatch batch : batchesByNodes.values()) {
                    batch.resetRouterStats();
                    batchId = batch.getBatchId();
                }
                for (Map<String, OutgoingBatch> groupBatches : batchesByGroups.values()) {
                    for (OutgoingBatch batch : groupBatches.values()) {
                        batch.resetRouterStats();
                        batchId = batch.getBatchId();
                    }                    
                }
                log.info("Re-attempting routing for batch {} with contains_big_lobs temporarily enabled for channel {}", 
                        batchId, nodeChannel.getChannelId());
                dataCount = 0;
                gapDetector.addDataIds(context.getDataIds());
                gapDetector.afterRouting();
                gapDetector.beforeRouting();
                long dataCountWithBigLob = routeDataForChannel(processInfo, nodeChannel, sourceNode, true, batchesByNodes, batchesByGroups);
                return context.getCommittedDataEventCount() + dataCountWithBigLob;
            }
        } catch (CommonBatchCollisionException e) {
            gapDetector.setIsAllDataRead(false);
            dataCount = 1; // we prevented writing the collision, so commit what we have
            throw e;
        } catch (Throwable ex) {
            log.error(
                    String.format("Failed to route and batch data on '%s' channel",
                            nodeChannel.getChannelId()), ex);
            if (context != null) {
                context.rollback();
            }
            return context.getCommittedDataEventCount();
        } finally {
            try {
                if (dataCount > 0) {
                    long insertTs = System.currentTimeMillis();
                    engine.getDataService().insertDataEvents(context.getSqlTransaction(),
                            context.getDataEventList());
                    context.clearDataEventsList();
                    context.incrementStat(System.currentTimeMillis() - insertTs, ChannelRouterContext.STAT_INSERT_DATA_EVENTS_MS);
                    completeBatchesAndCommit(context);
                    gapDetector.addDataIds(context.getDataIds());
                    gapDetector.setIsAllDataRead(context.getDataIds().size() < context.getChannel().getMaxDataToRoute());
                    hasMaxDataRoutedOnChannel |= context.getDataIds().size() >= context.getChannel().getMaxDataToRoute();

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
                } else if (dataCount == -1) {
                    // rolled back as exception, but let gap detector know about what was committed before halting
                    gapDetector.addDataIds(context.getDataIds());
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
        gapDetector.setFullGapAnalysis(context.getSqlTransaction(), true);

        Set<IDataRouter> usedRouters = new HashSet<IDataRouter>(context.getUsedDataRouters());
        List<OutgoingBatch> batches = new ArrayList<OutgoingBatch>(context.getBatchesByNodes().values());
        
        for (Map<String, OutgoingBatch> groupBatches : context.getBatchesByGroups().values()) {
            batches.addAll(groupBatches.values());
        }

        long ts = System.currentTimeMillis();
        completeBatches(context, batches, usedRouters);
        context.commit();
        context.incrementStat(System.currentTimeMillis() - ts, ChannelRouterContext.STAT_UPDATE_BATCHES_MS);

        for (IDataRouter dataRouter : usedRouters) {
            dataRouter.contextCommitted(context);
        }
        context.setNeedsCommitted(false);
    }

    protected void completeBatches(ChannelRouterContext context, List<OutgoingBatch> batches, Set<IDataRouter> usedRouters) {
        if (engine.getParameterService().is(ParameterConstants.ROUTING_LOG_STATS_ON_BATCH_ERROR)) {
            engine.getStatisticManager().addRouterStats(context.getStartDataId(), context.getEndDataId(), 
                    context.getDataReadCount(), context.getPeekAheadFillCount(),
                    context.getDataGaps(), null, batches);
        }

        for (OutgoingBatch batch : batches) {
            for (IDataRouter dataRouter : usedRouters) {
                dataRouter.completeBatch(context, batch);
            }
            if (Constants.UNROUTED_NODE_ID.equals(batch.getNodeId())) {
                batch.setStatus(Status.OK);
            } else {
                batch.setStatus(Status.NE);
            }
            batch.setRouterMillis((System.currentTimeMillis() - batch.getCreateTime().getTime()) / batches.size());
            engine.getOutgoingBatchService().updateOutgoingBatch(context.getSqlTransaction(), batch);
        }
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
                    final String namePrefix = parameterService.getEngineName().toLowerCase() + "-router-reader-";

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
    protected long selectDataAndRoute(ProcessInfo processInfo, ChannelRouterContext context) throws InterruptedException {
        IDataToRouteReader reader = startReading(context);
        Data data = null;
        Data nextData = null;
        long totalDataCount = 0;
        long totalDataEventCount = 0;
        long statsDataCount = 0;
        long statsDataEventCount = 0;
        final int maxNumberOfEventsBeforeFlush = parameterService
                .getInt(ParameterConstants.ROUTING_FLUSH_JDBC_BATCH_SIZE);
        try {
            long ts = System.currentTimeMillis();
            long startTime = ts;
            nextData = reader.take();
            do {
                if (nextData != null) {
                    data = nextData;
                    nextData = reader.take();
                    if (data != null) {
                        processInfo.setCurrentTableName(data.getTableName());
                        processInfo.incrementCurrentDataCount();
                        if (data.isPreRouted()) {
                            context.addData(data.getDataId());
                        } else {
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
                        }
                        long insertTs = System.currentTimeMillis();
                        try {
                            if (maxNumberOfEventsBeforeFlush <= context.getDataEventList().size()
                                    || context.isNeedsCommitted()) {
                                engine.getDataService().insertDataEvents(
                                        context.getSqlTransaction(), context.getDataEventList());
                                context.clearDataEventsList();
                                context.incrementStat(System.currentTimeMillis() - insertTs, ChannelRouterContext.STAT_INSERT_DATA_EVENTS_MS);
                            }
                            if (context.isNeedsCommitted()) {
                                completeBatchesAndCommit(context);
                            }
                        } finally {
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
                                            FormatUtils.abbreviateForLogging(context.getDataGaps(), MAX_LOGGING_LENGTH)
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
            boolean isUnrouted = false;
            boolean alreadyInsertedUnrouted = false;
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
                            if (ex instanceof ProtocolException && !context.getChannel().getChannel().isContainsBigLob() 
                                    && !context.isOverrideContainsBigLob()) {
                                log.warn(ex.getMessage() + "  If this happens often, it might be better to isolate the table with sym_channel.contains_big_lobs enabled.");
                                throw ex;
                            }

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

                isUnrouted = (nodeIds == null || nodeIds.size() == 0);
                if (!isUnrouted || !alreadyInsertedUnrouted) {
                    numberOfDataEventsInserted += insertDataEvents(processInfo, context, dataMetaData, nodeIds);
                    if (isUnrouted) {
                        alreadyInsertedUnrouted = true;
                    }
                }
            }

        } else {
            Integer triggerHistId = data.getTriggerHistory() != null ? data.getTriggerHistory().getTriggerHistoryId() : -1;
            CounterStat counterStat = missingTriggerRouter.get(triggerHistId);
            if (counterStat == null) {
                counterStat = new CounterStat(data);
                missingTriggerRouter.put(triggerHistId, counterStat);
            }
            counterStat.incrementCount();
            numberOfDataEventsInserted += insertDataEvents(processInfo, context, new DataMetaData(data, table,
                    null, context.getChannel()), new HashSet<String>(0));
        }

        context.incrementStat(numberOfDataEventsInserted,
                ChannelRouterContext.STAT_DATA_EVENTS_INSERTED);
        return numberOfDataEventsInserted;

    }

    protected int insertDataEvents(ProcessInfo processInfo, ChannelRouterContext context, DataMetaData dataMetaData,
            Collection<String> nodeIds) {
        final long ts = System.currentTimeMillis();
        final String tableName = dataMetaData.getTable().getNameLowerCase();
        final DataEventType eventType = dataMetaData.getData().getDataEventType();
        Map<String, OutgoingBatch> batches = null;
        long batchIdToReuse = -1;
        long loadId = -1;
        boolean dataEventAdded = false;
        boolean useCommonMode = context.isProduceCommonBatches() || context.isProduceGroupBatches();
        boolean firstTimeForGroup = false;
        int numberOfDataEventsInserted = 0;
        ISqlTransaction transaction = null;

        if (nodeIds == null || nodeIds.size() == 0) {
            nodeIds = new HashSet<String>(1);
            nodeIds.add(Constants.UNROUTED_NODE_ID);
        }
        
        if (context.isProduceGroupBatches()) {
            Map<Integer, Map<String, OutgoingBatch>> batchesByGroups = context.getBatchesByGroups();
            int groupKey = nodeIds.hashCode();
            batches = batchesByGroups.get(groupKey);
            if (batches == null) {
                batches = new HashMap<String, OutgoingBatch>();
                batchesByGroups.put(groupKey, batches);
                firstTimeForGroup = true;
            }
            useCommonMode = nodeIds.size() > 1;
        } else {
            batches = context.getBatchesByNodes();
        }
        
        if (eventType == DataEventType.RELOAD) {
            loadId = context.getLastLoadId();
            if (loadId < 0) {
                loadId = engine.getSequenceService().nextVal(Constants.SEQUENCE_OUTGOING_BATCH_LOAD_ID);
                context.setLastLoadId(loadId);
            }
            if (context.getChannel().isReloadFlag()) {
                context.setNeedsCommitted(true);
            }
        } else if (eventType == DataEventType.CREATE) {
            if (dataMetaData.getData().getPkData() != null) {
                try {
                    loadId = Long.parseLong(dataMetaData.getData().getPkData());
                } catch (NumberFormatException e) {
                }
            }
            context.setNeedsCommitted(true);
        } else {
            context.setLastLoadId(-1);
        }

        try {
            for (String nodeId : nodeIds) {
                if (nodeId != null) {
                    OutgoingBatch batch = batches.get(nodeId);
                    if (batch == null) {
                        batch = new OutgoingBatch(nodeId, dataMetaData.getNodeChannel().getChannelId(), Status.RT);
                        batch.setBatchId(batchIdToReuse);
                        batch.setCommonFlag(useCommonMode);
                        
                        if (log.isDebugEnabled()) {
                            log.debug("About to insert a new batch for node {} on the '{}' channel.  Batches in progress are: {}.",
                                    nodeId, batch.getChannelId(), batches.values());
                        }
                        
                        if (useCommonMode && !firstTimeForGroup) {
                            throw new CommonBatchCollisionException("Collision detected for common batch group");
                        }

                        if (transaction == null) {
                            transaction = sqlTemplate.startSqlTransaction();
                        }

                        engine.getOutgoingBatchService().insertOutgoingBatch(transaction, batch);
                        processInfo.incrementBatchCount();
                        context.incrementStat(1, ChannelRouterContext.STAT_BATCHES_INSERTED);
                        batches.put(nodeId, batch);
    
                        // if in reuse mode, then share the batch id
                        if (useCommonMode) {
                            batchIdToReuse = batch.getBatchId();
                        }   
                    }
    
                    batch.incrementRowCount(eventType);
                    batch.incrementDataRowCount();
                    batch.incrementTableCount(tableName);
    
                    if (loadId != -1) {
                        batch.setLoadId(loadId);
                    }
                    if (!useCommonMode || (useCommonMode && !dataEventAdded)) {
                        context.addDataEvent(dataMetaData.getData().getDataId(), batch.getBatchId());
                        numberOfDataEventsInserted++;
                        dataEventAdded = true;
                    }
                    if (context.isBatchComplete(batch, dataMetaData)) {
                        context.setNeedsCommitted(true);
                    }
                }
            }
            if (transaction != null) {
                transaction.commit();
                transaction = null;
            }
        } finally {
            if (transaction != null) {
                transaction.rollback();
            }
        }

        context.incrementStat(System.currentTimeMillis() - ts, ChannelRouterContext.STAT_INSERT_BATCHES_MS);
        return numberOfDataEventsInserted;
    }

    protected IDataRouter getDataRouter(Router router) {
        IDataRouter dataRouter = null;
        Map<String, IDataRouter> routers = getRouters();
        if (!StringUtils.isBlank(router.getRouterType())) {
            dataRouter = routers.get(router.getRouterType());
            if (dataRouter == null) {
                CounterStat counterStat = invalidRouterType.get(router.getRouterId());
                if (counterStat == null) {
                    counterStat = new CounterStat(router);
                    invalidRouterType.put(router.getRouterId(), counterStat);
                }
                counterStat.incrementCount();
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
                if ((triggerRouters == null || triggerRouters.size() == 0) && System.currentTimeMillis() - triggerRouterCacheTime > 10000) {
                    triggerRouters = engine.getTriggerRouterService()
                            .getTriggerRoutersForCurrentNode(true)
                            .get((data.getTriggerHistory().getTriggerId()));
                    triggerRouterCacheTime = System.currentTimeMillis();
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
        long maxDataIdAlreadyRouted = 0;
        if (parameterService.is(ParameterConstants.CLUSTER_LOCKING_ENABLED)) {
            maxDataIdAlreadyRouted = sqlTemplateDirty.queryForLong(getSql("selectLastDataIdRoutedUsingDataGapSql"));
        } else {
            DataGap lastGap = gapDetector.getLastDataGap();
            if (lastGap != null) {
                maxDataIdAlreadyRouted = lastGap.getStartId();
            }
        }
        long leftToRoute = (engine.getDataService().findMaxDataId() - maxDataIdAlreadyRouted) + 1;
        
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

}
