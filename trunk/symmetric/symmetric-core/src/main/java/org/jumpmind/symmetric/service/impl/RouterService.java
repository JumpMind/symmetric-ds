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

import java.sql.SQLException;
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
import org.jumpmind.db.sql.AbstractSqlMap;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.TriggerRouter;
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
import org.jumpmind.symmetric.route.IDataToRouteGapDetector;
import org.jumpmind.symmetric.route.IDataToRouteReader;
import org.jumpmind.symmetric.route.LookupTableDataRouter;
import org.jumpmind.symmetric.route.NonTransactionalBatchAlgorithm;
import org.jumpmind.symmetric.route.SimpleRouterContext;
import org.jumpmind.symmetric.route.SubSelectDataRouter;
import org.jumpmind.symmetric.route.TransactionalBatchAlgorithm;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.ITransformService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticConstants;

/**
 * @see IRouterService
 */
public class RouterService extends AbstractService implements IRouterService {

    private IClusterService clusterService;

    private IDataService dataService;

    private IConfigurationService configurationService;

    private ITriggerRouterService triggerRouterService;

    private IOutgoingBatchService outgoingBatchService;

    private INodeService nodeService;

    private Map<String, IDataRouter> routers;

    private Map<String, IBatchAlgorithm> batchAlgorithms;

    private IStatisticManager statisticManager;

    transient ExecutorService readThread = null;

    public RouterService(IParameterService parameterService, ISymmetricDialect symmetricDialect,
            IClusterService clusterService, IDataService dataService,
            IConfigurationService configurationService, ITriggerRouterService triggerRouterService,
            IOutgoingBatchService outgoingBatchService, INodeService nodeService,
            IStatisticManager statisticManager, ITransformService transformService) {
        super(parameterService, symmetricDialect);
        this.clusterService = clusterService;
        this.dataService = dataService;
        this.configurationService = configurationService;
        this.triggerRouterService = triggerRouterService;
        this.outgoingBatchService = outgoingBatchService;
        this.nodeService = nodeService;
        this.statisticManager = statisticManager;

        this.batchAlgorithms = new HashMap<String, IBatchAlgorithm>();
        this.batchAlgorithms.put("default", new DefaultBatchAlgorithm());
        this.batchAlgorithms.put("nontransactional", new NonTransactionalBatchAlgorithm());
        this.batchAlgorithms.put("transactional", new TransactionalBatchAlgorithm());

        this.routers = new HashMap<String, IDataRouter>();
        this.routers.put("configurationChanged", new ConfigurationChangedDataRouter(
                configurationService, nodeService, triggerRouterService, parameterService,
                transformService));
        this.routers.put("bsh", new BshDataRouter(symmetricDialect));
        this.routers.put("subselect", new SubSelectDataRouter(symmetricDialect));
        this.routers.put("lookuptable", new LookupTableDataRouter(symmetricDialect));
        this.routers.put("default", new DefaultDataRouter());
        this.routers.put("column", new ColumnMatchDataRouter(configurationService));
    }

    /**
     * For use in data load events
     */
    public boolean shouldDataBeRouted(SimpleRouterContext context, DataMetaData dataMetaData,
            Set<Node> nodes, boolean initialLoad) {
        IDataRouter router = getDataRouter(dataMetaData.getTriggerRouter());
        Collection<String> nodeIds = router.routeToNodes(context, dataMetaData, nodes, initialLoad);
        for (Node node : nodes) {
            if (nodeIds != null && nodeIds.contains(node.getNodeId())) {
                return true;
            }
        }
        return false;
    }

    protected synchronized ExecutorService getReadService() {
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
        return readThread;
    }

    public synchronized void stop() {
        try {
            log.info("RouterShuttingDown");
            getReadService().shutdown();
            readThread = null;
        } catch (Exception ex) {
            log.error(ex);
        }
    }

    public synchronized void destroy() {

    }

    /**
     * This method will route data to specific nodes.
     */
    synchronized public long routeData() {
        long dataCount = -1l;
        if (clusterService.lock(ClusterConstants.ROUTE)) {
            try {
                insertInitialLoadEvents();
                long ts = System.currentTimeMillis();
                IDataToRouteGapDetector gapDetector = new DataGapDetector(log, dataService,
                        parameterService, symmetricDialect, getSqlMap());
                gapDetector.beforeRouting();
                dataCount = routeDataForEachChannel();
                gapDetector.afterRouting();
                ts = System.currentTimeMillis() - ts;
                if (dataCount > 0 || ts > Constants.LONG_OPERATION_THRESHOLD) {
                    log.info("RoutedDataInTime", dataCount, ts);
                }
            } finally {
                clusterService.unlock(ClusterConstants.ROUTE);
            }
        }
        return dataCount;
    }

    protected void insertInitialLoadEvents() {
        try {
            Node identity = nodeService.findIdentity();
            if (identity != null) {
                Map<String, NodeSecurity> nodeSecurities = nodeService.findAllNodeSecurity(false);
                if (nodeSecurities != null) {
                    for (NodeSecurity security : nodeSecurities.values()) {
                        if (!security.getNodeId().equals(identity.getNodeId())
                                && security.isInitialLoadEnabled()
                                && (security.getRegistrationTime() != null || security.getNodeId()
                                        .equals(identity.getCreatedAtNodeId()))) {
                            long ts = System.currentTimeMillis();
                            dataService.insertReloadEvents(nodeService.findNode(security
                                    .getNodeId()));
                            ts = System.currentTimeMillis() - ts;
                            if (ts > Constants.LONG_OPERATION_THRESHOLD) {
                                log.warn("ReloadedNode", security.getNodeId(), ts);
                            } else {
                                log.info("ReloadedNode", security.getNodeId(), ts);
                            }
                        }
                    }
                }
            }
        } catch (Exception ex) {
            log.error(ex);
        }
    }

    /**
     * We route data channel by channel for two reasons. One is that if/when we
     * decide to multi-thread the routing it is a simple matter of inserting a
     * thread pool here and waiting for all channels to be processed. The other
     * reason is to reduce the number of connections we are required to have.
     */
    protected int routeDataForEachChannel() {
        Node sourceNode = nodeService.findIdentity();
        final List<NodeChannel> channels = configurationService.getNodeChannels(false);
        int dataCount = 0;
        for (NodeChannel nodeChannel : channels) {
            if (!nodeChannel.isSuspendEnabled() && nodeChannel.isEnabled()) {
                dataCount += routeDataForChannel(nodeChannel, sourceNode);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("RouterSkippingChannel", nodeChannel.getChannelId());
                }
            }
        }
        return dataCount;
    }

    protected int routeDataForChannel(final NodeChannel nodeChannel, final Node sourceNode) {
        ChannelRouterContext context = null;
        long ts = System.currentTimeMillis();
        int dataCount = -1;
        try {
            context = new ChannelRouterContext(sourceNode.getNodeId(), nodeChannel,
                    symmetricDialect.getPlatform().getSqlTemplate().startSqlTransaction());
            dataCount = selectDataAndRoute(context);
            return dataCount;
        } catch (Exception ex) {
            log.error("RouterRoutingFailed", ex, nodeChannel.getChannelId());
            if (context != null) {
                context.rollback();
            }
            return 0;
        } finally {
            try {
                if (dataCount > 0) {
                    long insertTs = System.currentTimeMillis();
                    dataService.insertDataEvents(context.getSqlTransaction(),
                            context.getDataEventList());
                    context.clearDataEventsList();
                    completeBatchesAndCommit(context);
                    context.incrementStat(System.currentTimeMillis() - insertTs,
                            ChannelRouterContext.STAT_INSERT_DATA_EVENTS_MS);
                    if (context.getLastDataIdProcessed() > 0) {
                        String channelId = nodeChannel.getChannelId();
                        long queryTs = System.currentTimeMillis();
                        long dataLeftToRoute = sqlTemplate.queryForInt(
                                getSql("selectUnroutedCountForChannelSql"), channelId,
                                context.getLastDataIdProcessed());
                        queryTs = System.currentTimeMillis() - queryTs;
                        if (queryTs > Constants.LONG_OPERATION_THRESHOLD) {
                            log.warn("UnRoutedQueryTookLongTime", channelId, queryTs);
                        }
                        statisticManager.setDataUnRouted(channelId, dataLeftToRoute);
                    }
                }
            } catch (Exception e) {
                if (context != null) {
                    context.rollback();
                }
                log.error(e);
            } finally {
                long totalTime = System.currentTimeMillis() - ts;
                context.incrementStat(totalTime, ChannelRouterContext.STAT_ROUTE_TOTAL_TIME);
                context.logStats(log, totalTime);
                context.cleanup();
            }
        }
    }

    protected void completeBatchesAndCommit(ChannelRouterContext context) throws SQLException {
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
            outgoingBatchService.updateOutgoingBatch(batch);
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
            NodeGroupLink link = configurationService.getNodeGroupLinkFor(router.getNodeGroupLink()
                    .getSourceNodeGroupId(), router.getNodeGroupLink().getTargetNodeGroupId());
            if (link != null) {
                nodes.addAll(nodeService.findEnabledNodesFromNodeGroup(router.getNodeGroupLink()
                        .getTargetNodeGroupId()));
            } else {
                log.error("RouterIllegalNodeGroupLink", router.getRouterId(), router
                        .getNodeGroupLink().getSourceNodeGroupId(), router.getNodeGroupLink()
                        .getTargetNodeGroupId());
            }
            context.getAvailableNodes().put(triggerRouter, nodes);
        }
        return nodes;
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
    protected int selectDataAndRoute(ChannelRouterContext context) throws SQLException {
        IDataToRouteReader reader = new DataGapRouteReader(log, getSqlMap(), context, dataService,
                symmetricDialect, parameterService);
        getReadService().execute(reader);
        Data data = null;
        int totalDataCount = 0;
        int totalDataEventCount = 0;
        int statsDataCount = 0;
        int statsDataEventCount = 0;
        final int maxNumberOfEventsBeforeFlush = parameterService
                .getInt(ParameterConstants.ROUTING_FLUSH_JDBC_BATCH_SIZE);
        try {
            do {
                data = reader.take();
                if (data != null) {
                    context.setLastDataIdProcessed(data.getDataId());
                    statsDataCount++;
                    totalDataCount++;
                    int dataEventsInserted = routeData(data, context);
                    statsDataEventCount += dataEventsInserted;
                    totalDataEventCount += dataEventsInserted;
                    long insertTs = System.currentTimeMillis();
                    try {
                        if (maxNumberOfEventsBeforeFlush <= context.getDataEventList().size()
                                || context.isNeedsCommitted()) {
                            dataService.insertDataEvents(context.getSqlTransaction(),
                                    context.getDataEventList());
                            context.clearDataEventsList();
                        }
                        if (context.isNeedsCommitted()) {
                            completeBatchesAndCommit(context);
                        }
                    } finally {
                        context.incrementStat(System.currentTimeMillis() - insertTs,
                                ChannelRouterContext.STAT_INSERT_DATA_EVENTS_MS);

                        if (statsDataCount > StatisticConstants.FLUSH_SIZE_ROUTER_DATA) {
                            statisticManager.incrementDataRouted(context.getChannel()
                                    .getChannelId(), statsDataCount);
                            statsDataCount = 0;
                            statisticManager.incrementDataEventInserted(context.getChannel()
                                    .getChannelId(), statsDataEventCount);
                            statsDataEventCount = 0;
                        }
                    }
                }
            } while (data != null);

        } finally {
            reader.setReading(false);
            if (statsDataCount > 0) {
                statisticManager.incrementDataRouted(context.getChannel().getChannelId(),
                        statsDataCount);
            }
            if (statsDataEventCount > 0) {
                statisticManager.incrementDataEventInserted(context.getChannel().getChannelId(),
                        statsDataEventCount);
            }
        }
        context.incrementStat(totalDataCount, ChannelRouterContext.STAT_DATA_ROUTED_COUNT);
        return totalDataEventCount;

    }

    protected int routeData(Data data, ChannelRouterContext context) throws SQLException {
        int numberOfDataEventsInserted = 0;
        context.recordTransactionBoundaryEncountered(data);
        List<TriggerRouter> triggerRouters = getTriggerRoutersForData(data);
        if (triggerRouters != null && triggerRouters.size() > 0) {
            for (TriggerRouter triggerRouter : triggerRouters) {
                Table table = symmetricDialect.getTable(triggerRouter.getTrigger(), true);
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

                    if (!triggerRouter.isPingBackEnabled() && data.getSourceNodeId() != null
                            && nodeIds != null) {
                        nodeIds.remove(data.getSourceNodeId());
                    }
                }

                numberOfDataEventsInserted += insertDataEvents(context, dataMetaData, nodeIds,
                        triggerRouter);
            }

        } else {
            log.warn("TriggerProcessingFailedMissing", data.getTriggerHistory().getTriggerId(),
                    data.getDataId());
        }

        context.incrementStat(numberOfDataEventsInserted,
                ChannelRouterContext.STAT_DATA_EVENTS_INSERTED);
        return numberOfDataEventsInserted;

    }

    protected int insertDataEvents(ChannelRouterContext context, DataMetaData dataMetaData,
            Collection<String> nodeIds, TriggerRouter triggerRouter) {
        int numberOfDataEventsInserted = 0;
        if (nodeIds == null || nodeIds.size() == 0) {
            nodeIds = new HashSet<String>(1);
            nodeIds.add(Constants.UNROUTED_NODE_ID);
        }
        long ts = System.currentTimeMillis();
        for (String nodeId : nodeIds) {
            Map<String, OutgoingBatch> batches = context.getBatchesByNodes();
            OutgoingBatch batch = batches.get(nodeId);
            if (batch == null) {
                batch = new OutgoingBatch(nodeId, dataMetaData.getNodeChannel().getChannelId(),
                        Status.RT);
                outgoingBatchService.insertOutgoingBatch(batch);
                context.getBatchesByNodes().put(nodeId, batch);
            }
            batch.incrementEventCount(dataMetaData.getData().getDataEventType());
            batch.incrementDataEventCount();
            numberOfDataEventsInserted++;
            context.addDataEvent(dataMetaData.getData().getDataId(), batch.getBatchId(),
                    triggerRouter.getRouter().getRouterId());
            if (batchAlgorithms.get(context.getChannel().getBatchAlgorithm()).isBatchComplete(
                    batch, dataMetaData, context)) {
                context.setNeedsCommitted(true);
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
                log.warn("RouterMissing", trigger.getRouter().getRouterType(), trigger.getTrigger()
                        .getTriggerId());
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
                triggerRouters = triggerRouterService.getTriggerRoutersForCurrentNode(false).get(
                        (data.getTriggerHistory().getTriggerId()));
                if (triggerRouters == null || triggerRouters.size() == 0) {
                    triggerRouters = triggerRouterService.getTriggerRoutersForCurrentNode(true)
                            .get((data.getTriggerHistory().getTriggerId()));
                }
            } else {
                log.warn("TriggerHistMissing", data.getDataId());
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
        long leftToRoute = dataService.findMaxDataId() - maxDataIdAlreadyRouted;
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

    @Override
    protected AbstractSqlMap createSqlMap() {
        return new RouterServiceSqlMap(symmetricDialect.getPlatform(), createSqlReplacementTokens());
    }

}