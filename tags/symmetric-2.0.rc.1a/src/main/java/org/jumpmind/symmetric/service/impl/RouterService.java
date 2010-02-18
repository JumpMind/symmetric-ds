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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.support.JdbcUtils;

/**
 * This service is responsible for routing data to specific nodes and managing
 * the batching of data to be delivered to each node.
 * 
 * @since 2.0
 */
public class RouterService extends AbstractService implements IRouterService {

    private static final String STAT_INSERT_DATA_EVENTS_MS = "insert.data.events.ms";

    private static final String STAT_DATA_ROUTER_MS = "data.router.ms";

    private static final String STAT_READ_DATA_MS = "read.data.ms";

    private IClusterService clusterService;

    private IDataService dataService;

    private IConfigurationService configurationService;

    private ITriggerRouterService triggerRouterService;

    private IOutgoingBatchService outgoingBatchService;

    private INodeService nodeService;

    private Map<String, IDataRouter> routers;

    private Map<String, IBatchAlgorithm> batchAlgorithms;

    public boolean shouldDataBeRouted(IRouterContext context, DataMetaData dataMetaData, Set<Node> nodes,
            boolean initialLoad) {
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
     * 
     * @return true if data was routed
     */
    synchronized public void routeData() {
        if (clusterService.lock(ClusterConstants.ROUTE)) {
            try {
                long ts = System.currentTimeMillis();
                Node sourceNode = nodeService.findIdentity();
                DataRef ref = dataService.getDataRef();
                int dataCount = routeDataForEachChannel(ref, sourceNode);
                findAndSaveNextDataId(ref);
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

    protected int routeDataForChannel(final DataRef ref, final NodeChannel nodeChannel, final Node sourceNode) {
        return jdbcTemplate.execute(new ConnectionCallback<Integer>() {
            public Integer doInConnection(Connection c) throws SQLException, DataAccessException {
                RouterContext context = null;
                try {
                    context = new RouterContext(sourceNode.getNodeId(), nodeChannel, dataSource);
                    return selectDataAndRoute(c, ref, context);
                } catch (Exception ex) {
                    if (context != null) {
                        context.rollback();
                    }
                    log.error("RouterRoutingFailed", ex, nodeChannel.getChannelId());
                    return 0;
                } finally {
                    try {
                        List<OutgoingBatch> batches = new ArrayList<OutgoingBatch>(context.getBatchesByNodes().values());
                        int batchCount = batches.size();
                        for (OutgoingBatch batch : batches) {
                            completeBatch(batch, context, batchCount);
                        }
                        context.commit();
                    } catch (SQLException e) {
                        log.error(e);
                    } finally {
                        context.logStats(log);
                        context.cleanup();    
                    }
                }
            }
        });
    }

    protected void findAndSaveNextDataId(final DataRef ref) {
        long ts = System.currentTimeMillis();
        long lastDataId = (Long) jdbcTemplate.query(getSql("selectDistinctDataIdFromDataEventSql"), new Object[] { ref
                .getRefDataId() }, new int[] { Types.INTEGER }, new ResultSetExtractor<Long>() {
            public Long extractData(ResultSet rs) throws SQLException, DataAccessException {
                long lastDataId = ref.getRefDataId();
                while (rs.next()) {
                    long dataId = rs.getLong(1);
                    if (lastDataId == -1 || lastDataId + 1 == dataId || lastDataId == dataId) {
                        lastDataId = dataId;
                    } else {
                        if (isDataGapExpired(dataId)) {
                            lastDataId = dataId;
                        } else {
                            // detected a gap!
                            break;
                        }
                    }
                }
                return lastDataId;
            }
        });
        long updateTimeInMs = System.currentTimeMillis()-ts;        
        if (updateTimeInMs > 5000) {
            log.debug("RoutedDataRefUpdateTime", updateTimeInMs);
        }
        if (ref.getRefDataId() != lastDataId) {
            dataService.saveDataRef(new DataRef(lastDataId, new Date()));
        }
    }

    protected boolean isDataGapExpired(long dataId) {
        long gapTimoutInMs = parameterService.getLong(ParameterConstants.ROUTING_STALE_DATA_ID_GAP_TIME);
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
            List<NodeGroupLink> links = configurationService.getGroupLinksFor(router.getSourceNodeGroupId(), router.getTargetNodeGroupId());
            for (NodeGroupLink nodeGroupLink : links) {
                nodes.addAll(nodeService.findTargetNodesFor(nodeGroupLink.getDataEventAction()));
            }
            filterDisabledNodes(nodes);
            context.getAvailableNodes().put(triggerRouter, nodes);
        }
        return nodes;
    }

    protected void filterDisabledNodes(Set<Node> nodes) {
        for (Iterator<Node> iterator = nodes.iterator(); iterator.hasNext();) {
            Node node = iterator.next();
            if (!node.isSyncEnabled()) {
                iterator.remove();
            }
        }
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
    protected int selectDataAndRoute(Connection conn, DataRef ref, RouterContext context) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        int dataCount = 0;
        try {
            // TODO add a flag to sym_channel to indicate whether we need to
            // read the row_data and or old_data for routing. We will get better
            // performance if we don't read the data.
            ps = conn.prepareStatement(getSql("selectDataToBatchSql"), ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            ps.setFetchSize(dbDialect.getStreamingResultsFetchSize());
            ps.setString(1, context.getChannel().getChannelId());
            ps.setLong(2, ref.getRefDataId());
            rs = ps.executeQuery();
            int peekAheadLength = dbDialect.getRouterDataPeekAheadCount();
            Map<String, Long> transactionIdDataId = new HashMap<String, Long>();
            LinkedList<Data> dataQueue = new LinkedList<Data>();
            boolean hasNext = rs.next();
            for (int i = 0; i < peekAheadLength && hasNext; i++) {
                long ts = System.currentTimeMillis();
                if (readData(rs, dataQueue, transactionIdDataId)) {
                    dataCount++;
                    context.incrementStat(System.currentTimeMillis() - ts, STAT_READ_DATA_MS);
                } else {
                    // don't count the event if we didn't read it
                    i--;
                }
                
                if (hasNext) {
                    hasNext = rs.next();
                }
            }
            
            // Go ahead and close the resource if we don't need it anymore.
            if (!hasNext) {
                JdbcUtils.closeResultSet(rs);                
                JdbcUtils.closeStatement(ps);
                rs = null;
                ps = null;
            }

            while (dataQueue.size() > 0) {
                routeData(dataQueue.poll(), transactionIdDataId, context);
                if (hasNext) {
                    long ts = System.currentTimeMillis();
                    if (readData(rs, dataQueue, transactionIdDataId)) {
                        dataCount++;
                    }
                    context.incrementStat(System.currentTimeMillis() - ts, STAT_READ_DATA_MS);
                    if (hasNext) {
                        hasNext = rs.next();
                    }
                }
            }
            
            return dataCount;

        } finally {
            JdbcUtils.closeResultSet(rs);
            JdbcUtils.closeStatement(ps);
        }
    }

    protected void routeData(Data data, Map<String, Long> transactionIdDataId, RouterContext context)
            throws SQLException {
        Long dataId = transactionIdDataId.get(data.getTransactionId());
        context.setEncountedTransactionBoundary(dataId == null ? true : dataId == data.getDataId());
        List<TriggerRouter> triggerRouters = getTriggerRoutersForData(data);
        if (triggerRouters != null && triggerRouters.size() > 0) {
            for (TriggerRouter triggerRouter : triggerRouters) {
                Table table = dbDialect.getTable(triggerRouter.getTrigger(), true);
                DataMetaData dataMetaData = new DataMetaData(data, table, triggerRouter, context.getChannel());

                context.resetForNextData();
                if (!context.getChannel().isIgnoreEnabled() && triggerRouter.isRouted(data.getEventType())) {
                    IDataRouter dataRouter = getDataRouter(triggerRouter);
                    context.addUsedDataRouter(dataRouter);
                    long ts = System.currentTimeMillis();
                    Collection<String> nodeIds = dataRouter.routeToNodes(context, dataMetaData, findAvailableNodes(
                            triggerRouter, context), false);
                    context.incrementStat(System.currentTimeMillis() - ts, STAT_DATA_ROUTER_MS);
                    if (!insertDataEvents(context, dataMetaData, nodeIds, triggerRouter)) {                        
                        log.debug("NoNodesToRouteTo", data.getDataId());
                    }
                }

                if (!context.isRouted()) {
                    // mark as not routed anywhere
                    dataService.insertDataEvent(context.getJdbcTemplate(), data.getDataId(), Constants.UNROUTED_BATCH_ID, triggerRouter
                            .getRouter().getRouterId());
                }

                if (context.isNeedsCommitted()) {
                    context.commit();
                }

            }

        } else {
            log.warn("TriggerProcessingFailedMissing", data.getTriggerHistory().getTriggerId(), data.getDataId());
        }

    }

    protected boolean insertDataEvents(RouterContext context, DataMetaData dataMetaData, Collection<String> nodeIds,
            TriggerRouter triggerRouter) {
        if (nodeIds != null && nodeIds.size() > 0) {
            long ts = System.currentTimeMillis();
            for (String nodeId : nodeIds) {
                if (dataMetaData.getData().getSourceNodeId() == null
                        || !dataMetaData.getData().getSourceNodeId().equals(nodeId)) {
                    Map<String, OutgoingBatch> batches = context.getBatchesByNodes();
                    OutgoingBatch batch = batches.get(nodeId);
                    if (batch == null) {
                        batch = new OutgoingBatch(nodeId, dataMetaData.getNodeChannel().getChannelId());
                        outgoingBatchService.insertOutgoingBatch(context.getJdbcTemplate(), batch);
                        context.getBatchesByNodes().put(nodeId, batch);
                    }
                    batch.incrementDataEventCount();
                    context.setRouted(true);
                    dataService.insertDataEvent(context.getJdbcTemplate(), dataMetaData.getData().getDataId(), batch
                            .getBatchId(), triggerRouter.getRouter().getRouterId());
                    if (batchAlgorithms.get(context.getChannel().getBatchAlgorithm()).isBatchComplete(batch,
                            dataMetaData, context)) {
                        completeBatch(batch, context, batches.size());
                    }
                }
            }
            context.incrementStat(System.currentTimeMillis() - ts, STAT_INSERT_DATA_EVENTS_MS);
            return true;
        } else {
            return false;
        }

    }

    protected void completeBatch(OutgoingBatch batch, RouterContext context, int numberOfBatches) {
        batch.setRouterMillis((System.currentTimeMillis() - context.getCreatedTimeInMs())/numberOfBatches);
        Set<IDataRouter> usedRouters = context.getUsedDataRouters();
        for (IDataRouter dataRouter : usedRouters) {
            dataRouter.completeBatch(context, batch);
        }
        outgoingBatchService.updateOutgoingBatch(context.getJdbcTemplate(), batch);
        context.getBatchesByNodes().remove(batch.getNodeId());
        context.setNeedsCommitted(true);
    }

    protected IDataRouter getDataRouter(TriggerRouter trigger) {
        IDataRouter router = null;
        if (!StringUtils.isBlank(trigger.getRouter().getRouterType())) {
            router = routers.get(trigger.getRouter().getRouterType());
            if (router == null) {
                log.warn("RouterMissing", trigger.getRouter().getRouterType(), trigger.getTrigger().getTriggerId());
            }
        }

        if (router == null) {
            return routers.get("default");
        }
        return router;
    }

    protected boolean readData(ResultSet rs, LinkedList<Data> dataStack, Map<String, Long> transactionIdDataId)
            throws SQLException {
        if (rs.getString(13) == null) {
            Data data = dataService.readData(rs);
            dataStack.addLast(data);
            if (data.getTransactionId() != null) {
                transactionIdDataId.put(data.getTransactionId(), data.getDataId());
            }
            return true;
        } else {
            return false;
        }
    }

    protected List<TriggerRouter> getTriggerRoutersForData(Data data) {
        List<TriggerRouter> triggerRouters = triggerRouterService.getTriggerRoutersForCurrentNode(
                false).get(
                (data.getTriggerHistory().getTriggerId()));
        if (triggerRouters == null || triggerRouters.size() == 0) {
            triggerRouters = triggerRouterService.getTriggerRoutersForCurrentNode(
                    true).get(
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
