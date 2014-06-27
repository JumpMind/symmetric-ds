/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.model.Table;
import org.hsqldb.Types;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.DataRef;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.route.DataToRouteReader;
import org.jumpmind.symmetric.route.IBatchAlgorithm;
import org.jumpmind.symmetric.route.IDataRouter;
import org.jumpmind.symmetric.route.IRouterContext;
import org.jumpmind.symmetric.route.RouterContext;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

/**
 * This service is responsible for routing data to specific nodes and managing
 * the batching of data to be delivered to each node.
 * 
 * @since 2.0
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

    transient ExecutorService readThread = Executors.newSingleThreadExecutor();

    public boolean shouldDataBeRouted(IRouterContext context, DataMetaData dataMetaData,
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

    /**
     * This method will route data to specific nodes.
     */
    synchronized public void routeData() {
        if (clusterService.lock(ClusterConstants.ROUTE)) {
            try {
                long databaseTimeAtRoutingStart = dbDialect.getDatabaseTime();
                long ts = System.currentTimeMillis();
                Node sourceNode = nodeService.findIdentity();
                DataRef ref = dataService.getDataRef();
                int dataCount = routeDataForEachChannel(ref, sourceNode);
                findAndSaveNextDataId(databaseTimeAtRoutingStart);
                ts = System.currentTimeMillis() - ts;
                if (dataCount > 0 || ts > 30000) {
                    log.info("RoutedDataInTime", dataCount, ts);
                }
            } finally {
                clusterService.unlock(ClusterConstants.ROUTE);
            }
        }
    }

    /**
     * We route data channel by channel for two reasons. One is that if/when we
     * decide to multi-thread the routing it is a simple matter of inserting a
     * thread pool here and waiting for all channels to be processed. The other
     * reason is to reduce the number of connections we are required to have.
     */
    protected int routeDataForEachChannel(DataRef ref, Node sourceNode) {
        final List<NodeChannel> channels = configurationService.getNodeChannels();
        int dataCount = 0;
        for (NodeChannel nodeChannel : channels) {
            if (!nodeChannel.isSuspendEnabled()) {
                dataCount += routeDataForChannel(ref, nodeChannel, sourceNode);
            }
        }
        return dataCount;
    }

    protected int routeDataForChannel(final DataRef ref, final NodeChannel nodeChannel,
            final Node sourceNode) {
        RouterContext context = null;
        long ts = System.currentTimeMillis();
        try {
            context = new RouterContext(sourceNode.getNodeId(), nodeChannel, dataSource);
            return selectDataAndRoute(ref, context);
        } catch (Exception ex) {
            if (context != null) {
                context.rollback();
            }
            log.error("RouterRoutingFailed", ex, nodeChannel.getChannelId());
            return 0;
        } finally {
            try {
                completeBatchesAndCommit(context);
            } catch (SQLException e) {
                log.error(e);
            } finally {
                context.logStats(log, System.currentTimeMillis() - ts > 30000);
                context.cleanup();
            }
        }
    }

    protected void completeBatchesAndCommit(RouterContext context) throws SQLException {
        List<OutgoingBatch> batches = new ArrayList<OutgoingBatch>(context.getBatchesByNodes()
                .values());
        for (OutgoingBatch batch : batches) {
            batch.setRouterMillis(System.currentTimeMillis()-batch.getCreateTime().getTime());
            Set<IDataRouter> usedRouters = context.getUsedDataRouters();
            for (IDataRouter dataRouter : usedRouters) {
                dataRouter.completeBatch(context, batch);
            }
            outgoingBatchService.updateOutgoingBatch(context.getJdbcTemplate(), batch);
            context.getBatchesByNodes().remove(batch.getNodeId());
        }
        context.commit();
        context.setNeedsCommitted(false);
    }

    protected void findAndSaveNextDataId(final long databaseTimeAtRoutingStart) {
        // reselect the DataRef just in case somebody updated it manually during routing
        final DataRef ref = dataService.getDataRef();
        long ts = System.currentTimeMillis();
        long lastDataId = (Long) jdbcTemplate.query(getSql("selectDistinctDataIdFromDataEventSql"),
                new Object[] { ref.getRefDataId() }, new int[] { Types.INTEGER },
                new ResultSetExtractor<Long>() {
                    public Long extractData(ResultSet rs) throws SQLException, DataAccessException {
                        long lastDataId = ref.getRefDataId();
                        while (rs.next()) {
                            long dataId = rs.getLong(1);
                            if (lastDataId == -1 || lastDataId + 1 == dataId
                                    || lastDataId == dataId) {
                                lastDataId = dataId;
                            } else {
                                if (dataService.countDataInRange(lastDataId, dataId) == 0) {
                                    if (dbDialect.supportsTransactionViews() && !dbDialect.areDatabaseTransactionsPendingSince(databaseTimeAtRoutingStart)) {
                                        log.info("RouterSkippingDataIdsNoTransactions", lastDataId, dataId);
                                        lastDataId = dataId;                                        
                                    }  else if (isDataGapExpired(dataId)) {
                                        log.info("RouterSkippingDataIdsGapExpired", lastDataId, dataId);
                                        lastDataId = dataId;
                                    }                                    
                                } else {
                                    // detected a gap!
                                    break;
                                }
                            }
                        }
                        return lastDataId;
                    }
                });
        long updateTimeInMs = System.currentTimeMillis() - ts;
        if (updateTimeInMs > 10000) {
            log.info("RoutedDataRefUpdateTime", updateTimeInMs);
        }
        if (ref.getRefDataId() != lastDataId) {
            dataService.saveDataRef(new DataRef(lastDataId, new Date()));
        }
    }

    protected boolean isDataGapExpired(long dataId) {
        long gapTimoutInMs = parameterService
                .getLong(ParameterConstants.ROUTING_STALE_DATA_ID_GAP_TIME);
        Date createTime = dataService.findCreateTimeOfEvent(dataId);
        if (System.currentTimeMillis() - createTime.getTime() > gapTimoutInMs) {
            return true;
        } else {
            return false;
        }
    }

    protected Set<Node> findAvailableNodes(TriggerRouter triggerRouter, RouterContext context) {
        Set<Node> nodes = context.getAvailableNodes().get(triggerRouter);
        if (nodes == null) {
            nodes = new HashSet<Node>();
            Router router = triggerRouter.getRouter();
            List<NodeGroupLink> links = configurationService.getGroupLinksFor(router
                    .getSourceNodeGroupId(), router.getTargetNodeGroupId());
            if (links.size() > 0) {
               nodes.addAll(nodeService.findEnabledNodesFromNodeGroup(router.getTargetNodeGroupId()));           
            } else {
               log.error("RouterIllegalNodeGroupLink", router.getRouterId(), router.getSourceNodeGroupId(), router.getTargetNodeGroupId());
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
    protected int selectDataAndRoute(DataRef ref, RouterContext context) throws SQLException {

        DataToRouteReader reader = new DataToRouteReader(dataSource, dbDialect
                .getRouterDataPeekAheadCount(), getSql(), dbDialect.getStreamingResultsFetchSize(),
                context, ref, dataService);
        readThread.execute(reader);
        Data data = null;
        int dataCount = 0;
        do {
            data = reader.take();
            if (data != null) {
                dataCount++;
                routeData(data, context);
                if (context.isNeedsCommitted()) {
                    completeBatchesAndCommit(context);
                    long maxDataToRoute = context.getChannel().getMaxDataToRoute();
                    if (maxDataToRoute > 0 && dataCount > maxDataToRoute) {
                        reader.setReading(false);
                        log.info("RoutedMaxNumberData", dataCount, context.getChannel()
                                .getChannelId());
                        break;
                    }
                }
            }
        } while (data != null);

        return dataCount;

    }

    protected void routeData(Data data, RouterContext context) throws SQLException {
        context.recordTransactionBoundaryEncountered(data);
        List<TriggerRouter> triggerRouters = getTriggerRoutersForData(data);
        if (triggerRouters != null && triggerRouters.size() > 0) {
            for (TriggerRouter triggerRouter : triggerRouters) {
                Table table = dbDialect.getTable(triggerRouter.getTrigger(), true);
                DataMetaData dataMetaData = new DataMetaData(data, table, triggerRouter, context
                        .getChannel());
                Collection<String> nodeIds = null;
                if (!context.getChannel().isIgnoreEnabled()
                        && triggerRouter.isRouted(data.getEventType())) {
                    IDataRouter dataRouter = getDataRouter(triggerRouter);
                    context.addUsedDataRouter(dataRouter);
                    long ts = System.currentTimeMillis();
                    nodeIds = dataRouter.routeToNodes(context, dataMetaData, findAvailableNodes(
                            triggerRouter, context), false);
                    context.incrementStat(System.currentTimeMillis() - ts,
                            RouterContext.STAT_DATA_ROUTER_MS);

                    if (data.getSourceNodeId() != null && nodeIds != null) {
                        nodeIds.remove(data.getSourceNodeId());
                    }
                }

                insertDataEvents(context, dataMetaData, nodeIds, triggerRouter);
            }

        } else {
            log.warn("TriggerProcessingFailedMissing", data.getTriggerHistory().getTriggerId(),
                    data.getDataId());
        }

    }

    protected void insertDataEvents(RouterContext context, DataMetaData dataMetaData,
            Collection<String> nodeIds, TriggerRouter triggerRouter) {
        if (nodeIds == null || nodeIds.size() == 0) {
            nodeIds = new HashSet<String>(1);
            nodeIds.add(Constants.UNROUTED_NODE_ID);
        }
        long ts = System.currentTimeMillis();
        for (String nodeId : nodeIds) {
            Map<String, OutgoingBatch> batches = context.getBatchesByNodes();
            OutgoingBatch batch = batches.get(nodeId);
            if (batch == null) {
                batch = new OutgoingBatch(nodeId, dataMetaData.getNodeChannel().getChannelId());
                if (Constants.UNROUTED_NODE_ID.equals(nodeId)) {
                    batch.setStatus(Status.OK);
                }
                outgoingBatchService.insertOutgoingBatch(context.getJdbcTemplate(), batch);
                context.getBatchesByNodes().put(nodeId, batch);
            }
            batch.incrementDataEventCount();
            dataService.insertDataEvent(context.getJdbcTemplate(), dataMetaData.getData()
                    .getDataId(), batch.getBatchId(), triggerRouter.getRouter().getRouterId());
            if (batchAlgorithms.get(context.getChannel().getBatchAlgorithm()).isBatchComplete(
                    batch, dataMetaData, context)) {
                context.setNeedsCommitted(true);
            }
        }
        context.incrementStat(System.currentTimeMillis() - ts,
                RouterContext.STAT_INSERT_DATA_EVENTS_MS);
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
        List<TriggerRouter> triggerRouters = triggerRouterService.getTriggerRoutersForCurrentNode(
                false).get((data.getTriggerHistory().getTriggerId()));
        if (triggerRouters == null || triggerRouters.size() == 0) {
            triggerRouters = triggerRouterService.getTriggerRoutersForCurrentNode(true).get(
                    (data.getTriggerHistory().getTriggerId()));
        }
        return triggerRouters;
    }

    public void addDataRouter(String name, IDataRouter dataRouter) {
        routers.put(name, dataRouter);
    }

    public void addBatchAlgorithm(String name, IBatchAlgorithm algorithm) {
        batchAlgorithms.put(name, algorithm);
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setOutgoingBatchService(IOutgoingBatchService outgoingBatchService) {
        this.outgoingBatchService = outgoingBatchService;
    }

    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setDataService(IDataService dataService) {
        this.dataService = dataService;
    }

    public void setRouters(Map<String, IDataRouter> routers) {
        this.routers = routers;
    }

    public void setBatchAlgorithms(Map<String, IBatchAlgorithm> batchAlgorithms) {
        this.batchAlgorithms = batchAlgorithms;
    }

    public void setTriggerRouterService(ITriggerRouterService triggerService) {
        this.triggerRouterService = triggerService;
    }
}
