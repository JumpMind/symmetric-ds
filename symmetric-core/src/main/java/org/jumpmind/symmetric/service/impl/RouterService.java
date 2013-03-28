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

import java.util.ArrayList;
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

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SyntaxParsingException;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessInfoKey.ProcessType;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.route.AuditTableDataRouter;
import org.jumpmind.symmetric.route.BshDataRouter;
import org.jumpmind.symmetric.route.ChannelRouterContext;
import org.jumpmind.symmetric.route.ColumnMatchDataRouter;
import org.jumpmind.symmetric.route.ConfigurationChangedDataRouter;
import org.jumpmind.symmetric.route.DataGapDetector;
import org.jumpmind.symmetric.route.DataGapRouteReader;
import org.jumpmind.symmetric.route.DefaultBatchAlgorithm;
import org.jumpmind.symmetric.route.DefaultDataRouter;
import org.jumpmind.symmetric.route.IBatchAlgorithm;
import org.jumpmind.symmetric.route.IDataRouter;
import org.jumpmind.symmetric.route.IDataToRouteReader;
import org.jumpmind.symmetric.route.LookupTableDataRouter;
import org.jumpmind.symmetric.route.NonTransactionalBatchAlgorithm;
import org.jumpmind.symmetric.route.SimpleRouterContext;
import org.jumpmind.symmetric.route.SubSelectDataRouter;
import org.jumpmind.symmetric.route.TransactionalBatchAlgorithm;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.statistic.StatisticConstants;

/**
 * @see IRouterService
 */
public class RouterService extends AbstractService implements IRouterService {

    protected  Map<String, IDataRouter> routers;

    protected  Map<String, IBatchAlgorithm> batchAlgorithms;

    protected  Map<String, Boolean> defaultRouterOnlyLastState = new HashMap<String, Boolean>();

    protected transient ExecutorService readThread = null;

    protected ISymmetricEngine engine;

    public RouterService(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect());

        this.engine = engine;

        this.batchAlgorithms = new HashMap<String, IBatchAlgorithm>();
        this.batchAlgorithms.put("default", new DefaultBatchAlgorithm());
        this.batchAlgorithms.put("nontransactional", new NonTransactionalBatchAlgorithm());
        this.batchAlgorithms.put("transactional", new TransactionalBatchAlgorithm());

        this.routers = new HashMap<String, IDataRouter>();
        this.routers.put("configurationChanged", new ConfigurationChangedDataRouter(engine));
        this.routers.put("bsh", new BshDataRouter(engine));
        this.routers.put("subselect", new SubSelectDataRouter(symmetricDialect));
        this.routers.put("lookuptable", new LookupTableDataRouter(symmetricDialect));
        this.routers.put("default", new DefaultDataRouter());
        this.routers.put("audit", new AuditTableDataRouter(engine));
        this.routers.put("column", new ColumnMatchDataRouter(engine.getConfigurationService(),
                engine.getSymmetricDialect()));

        setSqlMap(new RouterServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    /**
     * For use in data load events
     */
    public boolean shouldDataBeRouted(SimpleRouterContext context, DataMetaData dataMetaData,
            Node node, boolean initialLoad) {
        IDataRouter router = getDataRouter(dataMetaData.getTriggerRouter());
        Set<Node> oneNodeSet = new HashSet<Node>(1);
        oneNodeSet.add(node);
        Collection<String> nodeIds = router.routeToNodes(context, dataMetaData, oneNodeSet,
                initialLoad);
        return nodeIds != null && nodeIds.contains(node.getNodeId());
    }

    public synchronized void stop() {
        if (readThread != null) {
            try {
                log.info("RouterService is shutting down");
                readThread.shutdown();
                readThread = null;
            } catch (Exception ex) {
                log.error(ex.getMessage(), ex);
            }
        }
    }

    /**
     * This method will route data to specific nodes.
     */
    synchronized public long routeData(boolean force) {
        long dataCount = -1l;
        if (engine.getNodeService().findIdentity() != null) {
            if (force || engine.getClusterService().lock(ClusterConstants.ROUTE)) {
                try {
                    engine.getOutgoingBatchService().updateAbandonedRoutingBatches();
                    insertInitialLoadEvents();
                    long ts = System.currentTimeMillis();
                    DataGapDetector gapDetector = new DataGapDetector(
                            engine.getDataService(), parameterService, symmetricDialect, 
                            this, engine.getStatisticManager(), engine.getNodeService());
                    gapDetector.beforeRouting();
                    dataCount = routeDataForEachChannel(gapDetector);
                    ts = System.currentTimeMillis() - ts;
                    if (dataCount > 0 || ts > Constants.LONG_OPERATION_THRESHOLD) {
                        log.info("Routed {} data events in {} ms", dataCount, ts);
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
     * If a load has been queued up by setting the initial load enabled or reverse initial load
     * enabled flags, then the router service will insert the reload events.  This process will
     * not run at the same time sync triggers is running.
     */
    protected void insertInitialLoadEvents() {
        if (engine.getClusterService().lock(ClusterConstants.SYNCTRIGGERS)) {
            try {
                synchronized (engine.getTriggerRouterService()) {
                    INodeService nodeService = engine.getNodeService();
                    Node identity = nodeService.findIdentity();
                    if (identity != null) {
                        NodeSecurity identitySecurity = nodeService.findNodeSecurity(identity
                                .getNodeId());
                        if (engine.getParameterService().isRegistrationServer() || 
                                (identitySecurity != null && !identitySecurity.isRegistrationEnabled()
                                && identitySecurity.getRegistrationTime() != null)) {
                            List<NodeSecurity> nodeSecurities = nodeService
                                    .findNodeSecurityWithLoadEnabled();
                            if (nodeSecurities != null) {
                                boolean reverseLoadFirst = parameterService
                                        .is(ParameterConstants.INTITAL_LOAD_REVERSE_FIRST);
                                for (NodeSecurity security : nodeSecurities) {
                                    boolean reverseLoadQueued = security.isRevInitialLoadEnabled();
                                    boolean initialLoadQueued = security.isInitialLoadEnabled();
                                    boolean thisMySecurityRecord = security.getNodeId().equals(
                                            identity.getNodeId());
                                    boolean registered = security.getRegistrationTime() != null;
                                    boolean parent = identity.getNodeId().equals(
                                            security.getCreatedAtNodeId());
                                    if (thisMySecurityRecord && reverseLoadQueued
                                            && (reverseLoadFirst || !initialLoadQueued)) {
                                        sendReverseInitialLoad();
                                    } else if (!thisMySecurityRecord && registered && parent
                                            && initialLoadQueued
                                            && (!reverseLoadFirst || !reverseLoadQueued)) {
                                        long ts = System.currentTimeMillis();
                                        engine.getDataService().insertReloadEvents(
                                                engine.getNodeService().findNode(
                                                        security.getNodeId()), false);
                                        ts = System.currentTimeMillis() - ts;
                                        if (ts > Constants.LONG_OPERATION_THRESHOLD) {
                                            log.warn("Inserted reload events for node {} in {} ms",
                                                    security.getNodeId(), ts);
                                        } else {
                                            log.info("Inserted reload events for node {} in {} ms",
                                                    security.getNodeId(), ts);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (Exception ex) {
                log.error(ex.getMessage(), ex);
            } finally {
                engine.getClusterService().unlock(ClusterConstants.SYNCTRIGGERS);
            }
        } else {
            log.info("Not attempting to insert reload events because sync trigger is currently running");
        }
    }

    protected void sendReverseInitialLoad() {
        INodeService nodeService = engine.getNodeService();
        boolean queuedLoad = false;
        List<Node> nodes = new ArrayList<Node>();
        nodes.addAll(nodeService.findTargetNodesFor(NodeGroupLinkAction.P));
        nodes.addAll(nodeService.findTargetNodesFor(NodeGroupLinkAction.W));
        for (Node node : nodes) {
            engine.getDataService().insertReloadEvents(node, true);
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
    protected int routeDataForEachChannel(DataGapDetector gapDetector) {
        int dataCount = 0;
        Node sourceNode = engine.getNodeService().findIdentity();
        ProcessInfo processInfo = engine.getStatisticManager().newProcessInfo(
                new ProcessInfoKey(sourceNode.getNodeId(), null, ProcessType.ROUTER_JOB));
        try {
            final List<NodeChannel> channels = engine.getConfigurationService().getNodeChannels(
                    false);

            Map<String, List<TriggerRouter>> triggerRouters = engine.getTriggerRouterService()
                    .getTriggerRoutersByChannel(engine.getParameterService().getNodeGroupId());

            for (NodeChannel nodeChannel : channels) {
                if (!nodeChannel.isSuspendEnabled() && nodeChannel.isEnabled()) {
                    processInfo.setCurrentChannelId(nodeChannel.getChannelId());
                    dataCount += routeDataForChannel(processInfo,
                            nodeChannel,
                            sourceNode,
                            producesCommonBatches(nodeChannel.getChannelId(),
                                    triggerRouters.get(nodeChannel.getChannelId())), gapDetector);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Not routing the {} channel.  It is either disabled or suspended.",
                                nodeChannel.getChannelId());
                    }
                }
            }
            processInfo.setStatus(ProcessInfo.Status.DONE);
        } catch (RuntimeException ex) {
            processInfo.setStatus(ProcessInfo.Status.ERROR);
            throw ex;
        }
        return dataCount;
    }

    protected boolean producesCommonBatches(String channelId,
            List<TriggerRouter> allTriggerRoutersForChannel) {
        Boolean producesCommonBatches = !Constants.CHANNEL_CONFIG.equals(channelId)
                && !Constants.CHANNEL_RELOAD.equals(channelId) 
                && !Constants.CHANNEL_HEARTBEAT.equals(channelId) ? true : false;
        String nodeGroupId = parameterService.getNodeGroupId();
        if (allTriggerRoutersForChannel != null) {
            for (TriggerRouter triggerRouter : allTriggerRoutersForChannel) {
                IDataRouter dataRouter = getDataRouter(triggerRouter);
                /*
                 * If the data router is not a default data router or there will
                 * be incoming data on the channel where sync_on_incoming_batch
                 * is on, then we can not do 'optimal' routing. When
                 * sync_on_incoming_batch is on, then we might not be sending
                 * data to all nodes in a node_group. We can only do 'optimal'
                 * routing if data is going to go to all nodes in a group.
                 */
                if (!(dataRouter instanceof DefaultDataRouter)) {
                    producesCommonBatches = false;
                    break;
                } else {
                    if (triggerRouter
                                .getTrigger().isSyncOnIncomingBatch()) {
                        String tableName = triggerRouter.getTrigger().getFullyQualifiedSourceTableName();
                        for (TriggerRouter triggerRouter2 : allTriggerRoutersForChannel) {
                            if (triggerRouter2.getTrigger().getFullyQualifiedSourceTableName().equals(tableName) &&
                                    triggerRouter2.getRouter()
                                    .getNodeGroupLink().getTargetNodeGroupId().equals(nodeGroupId)) {
                                        producesCommonBatches = false;
                                        break;
                            }
                        }
                    }
                }
            }
        }

        if (!producesCommonBatches.equals(defaultRouterOnlyLastState.get(channelId))) {
            if (producesCommonBatches) {
                log.info("The '{}' channel is in common batch mode", channelId);
            } else {
                log.info("The '{}' channel is NOT in common batch mode", channelId);
            }
            defaultRouterOnlyLastState.put(channelId, producesCommonBatches);
        }
        return producesCommonBatches;
    }

    protected int routeDataForChannel(ProcessInfo processInfo, final NodeChannel nodeChannel, final Node sourceNode,
            boolean produceCommonBatches, DataGapDetector gapDetector) {
        ChannelRouterContext context = null;
        long ts = System.currentTimeMillis();
        int dataCount = -1;
        try {
            context = new ChannelRouterContext(sourceNode.getNodeId(), nodeChannel,
                    symmetricDialect.getPlatform().getSqlTemplate().startSqlTransaction());
            context.setProduceCommonBatches(produceCommonBatches);
            dataCount = selectDataAndRoute(processInfo, context);
            return dataCount;
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
                    context.incrementStat(System.currentTimeMillis() - insertTs,
                            ChannelRouterContext.STAT_INSERT_DATA_EVENTS_MS);
                    Data lastDataProcessed = context.getLastDataProcessed();
                    if (lastDataProcessed != null && lastDataProcessed.getDataId() > 0) {
                        String channelId = nodeChannel.getChannelId();
                        long queryTs = System.currentTimeMillis();
                        long dataLeftToRoute = sqlTemplate.queryForInt(
                                getSql("selectUnroutedCountForChannelSql"), channelId,
                                lastDataProcessed.getDataId());
                        queryTs = System.currentTimeMillis() - queryTs;
                        if (queryTs > Constants.LONG_OPERATION_THRESHOLD) {
                            log.warn("Unrouted query for channel {} took {} ms", channelId, queryTs);
                        }
                        engine.getStatisticManager().setDataUnRouted(channelId, dataLeftToRoute);
                    }
                }
            } catch (Exception e) {
                if (context != null) {
                    context.rollback();
                }
                log.error(e.getMessage(), e);
            } finally {
                long totalTime = System.currentTimeMillis() - ts;
                context.incrementStat(totalTime, ChannelRouterContext.STAT_ROUTE_TOTAL_TIME);
                context.logStats(log, totalTime);                
                boolean detectGaps = context.isRequestGapDetection();
                context.cleanup();
                if (detectGaps) {
                    gapDetector.beforeRouting();
                }
            }
        }
    }

    protected void completeBatchesAndCommit(ChannelRouterContext context) {
        Set<IDataRouter> usedRouters = new HashSet<IDataRouter>(context.getUsedDataRouters());
        List<OutgoingBatch> batches = new ArrayList<OutgoingBatch>(context.getBatchesByNodes()
                .values());
        context.commit();
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
            engine.getOutgoingBatchService().updateOutgoingBatch(batch);
            context.getBatchesByNodes().remove(batch.getNodeId());
        }

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
                    router.getNodeGroupLink().getTargetNodeGroupId());
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
        if (readThread == null) {
            readThread = Executors.newCachedThreadPool(new ThreadFactory() {
                final AtomicInteger threadNumber = new AtomicInteger(1);
                final String namePrefix = parameterService.getEngineName().toLowerCase()
                        + "-router-reader-";

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

        IDataToRouteReader reader = new DataGapRouteReader(context, engine);
        readThread.execute(reader);
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
            nextData = reader.take();
            do {
                if (nextData != null) {
                    data = nextData;
                    nextData = reader.take();
                    if (data != null) {
                        processInfo.incrementDataCount();
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

                        context.setLastDataProcessed(data);
                    }
                } else {
                    data = null;
                }
            } while (data != null);

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

    protected int routeData(ProcessInfo processInfo, Data data, ChannelRouterContext context) {
        int numberOfDataEventsInserted = 0;
        List<TriggerRouter> triggerRouters = getTriggerRoutersForData(data);
        Table table = symmetricDialect.getTable(data.getTriggerHistory(), true);
        if (triggerRouters != null && triggerRouters.size() > 0) {
            for (TriggerRouter triggerRouter : triggerRouters) {
                DataMetaData dataMetaData = new DataMetaData(data, table, triggerRouter,
                        context.getChannel());
                Collection<String> nodeIds = null;
                if (!context.getChannel().isIgnoreEnabled()
                        && triggerRouter.isRouted(data.getDataEventType())) {
                    IDataRouter dataRouter = getDataRouter(triggerRouter);
                    context.addUsedDataRouter(dataRouter);
                    long ts = System.currentTimeMillis();
                    nodeIds = dataRouter.routeToNodes(context, dataMetaData,
                            findAvailableNodes(triggerRouter, context), false);
                    context.incrementStat(System.currentTimeMillis() - ts,
                            ChannelRouterContext.STAT_DATA_ROUTER_MS);

                    if (nodeIds != null) {
                        // should never route to self
                        nodeIds.remove(engine.getNodeService().findIdentityNodeId());
                        if (!triggerRouter.isPingBackEnabled() && data.getSourceNodeId() != null) {
                            nodeIds.remove(data.getSourceNodeId());
                        }
                    }
                }

                numberOfDataEventsInserted += insertDataEvents(processInfo, context, dataMetaData, nodeIds);
            }

        } else {
            log.warn(
                    "Could not find trigger routers for trigger history id of {}.  Not processing data with the data id of {}",
                    data.getTriggerHistory().getTriggerHistoryId(), data.getDataId());
            numberOfDataEventsInserted += insertDataEvents(processInfo, context, new DataMetaData(data, table,
                    null, context.getChannel()), new HashSet<String>());
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
                    engine.getOutgoingBatchService().insertOutgoingBatch(batch);
                    processInfo.incrementBatchCount();
                    context.getBatchesByNodes().put(nodeId, batch);

                    // if in reuse mode, then share the batch id
                    if (context.isProduceCommonBatches()) {
                        batchIdToReuse = batch.getBatchId();
                    }
                }
                batch.incrementEventCount(dataMetaData.getData().getDataEventType());
                batch.incrementDataEventCount();
                numberOfDataEventsInserted++;
                if (!context.isProduceCommonBatches()
                        || (context.isProduceCommonBatches() && !dataEventAdded)) {
                    TriggerRouter triggerRouter = dataMetaData.getTriggerRouter();
                    context.addDataEvent(dataMetaData.getData().getDataId(), batch.getBatchId(),
                            triggerRouter != null ? triggerRouter.getRouter().getRouterId()
                                    : Constants.UNKNOWN_ROUTER_ID);
                    dataEventAdded = true;
                }
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

    protected IDataRouter getDataRouter(TriggerRouter trigger) {
        IDataRouter router = null;
        if (!StringUtils.isBlank(trigger.getRouter().getRouterType())) {
            router = routers.get(trigger.getRouter().getRouterType());
            if (router == null) {
                log.warn(
                        "Could not find configured router '{}' for trigger with the id of {}. Defaulting the router",
                        trigger.getRouter().getRouterType(), trigger.getTrigger().getTriggerId());
            }
        }

        if (router == null) {
            return routers.get("default");
        }
        return router;
    }

    protected List<TriggerRouter> getTriggerRoutersForData(Data data) {
        List<TriggerRouter> triggerRouters = null;
        if (data != null) {
            if (data.getTriggerHistory() != null) {
                triggerRouters = engine.getTriggerRouterService()
                        .getTriggerRoutersForCurrentNode(false)
                        .get((data.getTriggerHistory().getTriggerId()));
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

    public void addDataRouter(String name, IDataRouter dataRouter) {
        routers.put(name, dataRouter);
    }

    public void addBatchAlgorithm(String name, IBatchAlgorithm algorithm) {
        batchAlgorithms.put(name, algorithm);
    }

    public long getUnroutedDataCount() {
        long maxDataIdAlreadyRouted = sqlTemplate
                .queryForLong(getSql("selectLastDataIdRoutedUsingDataGapSql"));
        long leftToRoute = engine.getDataService().findMaxDataId() - maxDataIdAlreadyRouted;
        if (leftToRoute > 0) {
            return leftToRoute;
        } else {
            return 0;
        }
    }

    public List<String> getAvailableBatchAlgorithms() {
        return new ArrayList<String>(batchAlgorithms.keySet());
    }

    public Map<String, IDataRouter> getRouters() {
        return routers;
    }

}
