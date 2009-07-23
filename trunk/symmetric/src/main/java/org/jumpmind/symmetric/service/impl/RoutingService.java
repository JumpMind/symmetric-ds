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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.BatchType;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventAction;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.route.IBatchAlgorithm;
import org.jumpmind.symmetric.route.IDataRouter;
import org.jumpmind.symmetric.route.IRoutingContext;
import org.jumpmind.symmetric.route.RoutingContext;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IRoutingService;
import org.jumpmind.symmetric.service.LockActionConstants;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;

/**
 * This service is responsible for routing data to specific nodes and managing
 * the batching of data to be delivered to each node.
 * 
 * @since 2.0
 */
public class RoutingService extends AbstractService implements IRoutingService {

    private IClusterService clusterService;

    private IDataService dataService;

    private IConfigurationService configurationService;

    private IOutgoingBatchService outgoingBatchService;

    private IBootstrapService bootstrapService;

    private INodeService nodeService;

    private Map<String, IDataRouter> routers;

    private Map<String, IBatchAlgorithm> batchAlgorithms;

    public boolean shouldDataBeRouted(DataMetaData dataMetaData, Set<Node> nodes, boolean initialLoad) {
        IDataRouter router = getDataRouter(dataMetaData.getTrigger());
        Collection<String> nodeIds = router.routeToNodes(dataMetaData, nodes, initialLoad);
        for (Node node : nodes) {
            if (nodeIds == null || !nodeIds.contains(node.getNodeId())) {
                return false;
            }
        }
        return true;
    }

    /**
     * This method will route data to specific nodes.
     * 
     * @return true if data was routed
     */
    public void routeData() {
        if (clusterService.lock(LockActionConstants.ROUTE)) {
            try {
                routeDataForEachChannel();
            } finally {
                clusterService.unlock(LockActionConstants.ROUTE);
            }
        }
    }

    /**
     * We route data channel by channel for two reasons. One is that if/when we
     * decide to multi-thread the routing it is a simple matter of inserting a
     * thread pool here and waiting for all channels to be processed. The other
     * reason is to reduce the number of connections we are required to have.
     */
    protected void routeDataForEachChannel() {
        final List<NodeChannel> channels = configurationService.getChannels();
        for (NodeChannel nodeChannel : channels) {
            if (!nodeChannel.isSuspended()) {
                routeDataForChannel(nodeChannel);
            }
        }
    }

    protected void routeDataForChannel(final NodeChannel nodeChannel) {
        jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                IRoutingContext context = null;
                try {
                    context = new RoutingContext(nodeChannel, dataSource);
                    selectDataAndRoute(c, context);
                } catch (Exception ex) {
                    if (context != null) {
                        context.rollback();
                    }
                    logger.error(String.format("Failed to route and batch data on '%s' channel", nodeChannel.getId()),
                            ex);
                } finally {
                    try {
                        context.commit();
                    } catch (SQLException e) {
                        logger.error(e, e);
                    } finally {
                        context.cleanup();
                    }
                }
                return null;
            }
        });
    }

    protected Set<Node> findAvailableNodes(Trigger trigger, IRoutingContext context) {
        Set<Node> nodes = context.getAvailableNodes().get(trigger);
        if (nodes == null) {
            nodes = new HashSet<Node>();
            nodes.addAll(nodeService.findTargetNodesFor(DataEventAction.PUSH));
            nodes.addAll(nodeService.findTargetNodesFor(DataEventAction.WAIT_FOR_PULL));
            filterDisabledNodes(nodes);
            context.getAvailableNodes().put(trigger, nodes);
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
    protected void selectDataAndRoute(Connection conn, IRoutingContext context) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            // TODO Test select query on oracle with lots of data
            // TODO add a flag to sym_channel to indicate whether we need to
            // read the row_data and or old_data for
            // routing. We will get better performance if we don't read the
            // data.
            ps = conn.prepareStatement(getSql("selectDataToBatchSql"), ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            ps.setFetchSize(dbDialect.getStreamingResultsFetchSize());
            ps.setString(1, context.getChannel().getId());
            rs = ps.executeQuery();
            int peekAheadLength = parameterService.getInt(ParameterConstants.ROUTING_PEEK_AHEAD_WINDOW);
            Map<String, Long> transactionIdDataId = new HashMap<String, Long>();
            LinkedList<Data> dataQueue = new LinkedList<Data>();
            for (int i = 0; i < peekAheadLength && rs.next(); i++) {
                readData(rs, dataQueue, transactionIdDataId);
            }

            while (dataQueue.size() > 0) {
                routeData(dataQueue.poll(), transactionIdDataId, context);
                if (rs.next()) {
                    readData(rs, dataQueue, transactionIdDataId);
                }
            }

        } finally {
            JdbcUtils.closeResultSet(rs);
            JdbcUtils.closeStatement(ps);
        }
    }

    protected void routeData(Data data, Map<String, Long> transactionIdDataId, IRoutingContext routingContext)
            throws SQLException {
        Long dataId = transactionIdDataId.get(data.getTransactionId());
        routingContext.setEncountedTransactionBoundary(dataId == null ? true : dataId == data.getDataId());
        Trigger trigger = getTriggerForData(data);
        Table table = dbDialect.getMetaDataFor(trigger, true);
        DataMetaData dataMetaData = new DataMetaData(data, table, trigger, routingContext.getChannel());
        if (trigger != null) {
            routingContext.resetForNextData();
            if (!routingContext.getChannel().isIgnored()) {
                IDataRouter router = getDataRouter(trigger);
                Collection<String> nodeIds = router.routeToNodes(dataMetaData, findAvailableNodes(trigger,
                        routingContext), false);
                insertDataEvents(routingContext, dataMetaData, nodeIds);
            }

            if (!routingContext.isRouted()) {
                // mark as not routed anywhere
                dataService.insertDataEvent(routingContext.getJdbcTemplate(), data.getDataId(), "-1", -1);
            }

            if (routingContext.isNeedsCommitted()) {
                routingContext.commit();
            }
        } else {
            logger.warn(String.format(
                    "Could not find trigger for trigger id of %s.  Not processing data with the id of %s", data
                            .getTriggerHistory().getTriggerId(), data.getDataId()));
        }

    }

    protected void insertDataEvents(IRoutingContext routingContext, DataMetaData dataMetaData,
            Collection<String> nodeIds) {
        if (nodeIds != null && nodeIds.size() > 0) {
            for (String nodeId : nodeIds) {
                if (dataMetaData.getData().getSourceNodeId() == null
                        || !dataMetaData.getData().getSourceNodeId().equals(nodeId)) {
                    OutgoingBatch batch = routingContext.getBatchesByNodes().get(nodeId);
                    OutgoingBatchHistory history = routingContext.getBatchHistoryByNodes().get(nodeId);
                    if (batch == null) {
                        batch = createNewBatch(routingContext.getJdbcTemplate(), nodeId, dataMetaData.getChannel()
                                .getId());
                        routingContext.getBatchesByNodes().put(nodeId, batch);
                        history = new OutgoingBatchHistory(batch);
                        routingContext.getBatchHistoryByNodes().put(nodeId, history);
                    }
                    history.incrementDataEventCount();
                    routingContext.setRouted(true);
                    dataService.insertDataEvent(routingContext.getJdbcTemplate(), dataMetaData.getData().getDataId(),
                            nodeId, batch.getBatchId());
                    if (batchAlgorithms.get(routingContext.getChannel().getBatchAlgorithm()).isBatchComplete(history,
                            batch, dataMetaData, routingContext)) {
                        // TODO Add route_time_ms to history. Also fix outgoing
                        // batch so we don't end up
                        // with so many history records
                        outgoingBatchService.insertOutgoingBatchHistory(routingContext.getJdbcTemplate(), history);
                        routingContext.getBatchesByNodes().remove(nodeId);
                        routingContext.getBatchHistoryByNodes().remove(nodeId);
                        routingContext.setNeedsCommitted(true);
                    }
                }
            }
        }
    }

    protected IDataRouter getDataRouter(Trigger trigger) {
        IDataRouter router = null;
        if (!StringUtils.isBlank(trigger.getRouterName())) {
            router = routers.get(trigger.getRouterName());
            if (router == null) {
                logger.warn(String.format(
                        "Could not find configured router '%s' for trigger with the id of %s. Defaulting the router",
                        trigger.getRouterName(), trigger.getTriggerId()));
            }
        }

        if (router == null) {
            return routers.get("default");
        }
        return router;
    }

    protected Data readData(ResultSet rs, LinkedList<Data> dataStack, Map<String, Long> transactionIdDataId)
            throws SQLException {
        Data data = dataService.readData(rs);
        dataStack.addLast(data);
        if (data.getTransactionId() != null) {
            transactionIdDataId.put(data.getTransactionId(), data.getDataId());
        }
        return data;
    }

    protected Trigger getTriggerForData(Data data) {
        // TODO We really shouldn't be referencing the bootstrapService from
        // here ... maybe this method needs to move to the configurationService
        Trigger trigger = bootstrapService.getCachedTriggers(false).get((data.getTriggerHistory().getTriggerId()));
        if (trigger == null) {
            trigger = configurationService.getTriggerById(data.getTriggerHistory().getTriggerId());
            if (trigger == null) {
                throw new IllegalStateException(String.format("Could not find trigger with the id of %s", data
                        .getTriggerHistory().getTriggerId()));
            }
        }
        return trigger;
    }

    protected OutgoingBatch createNewBatch(JdbcTemplate template, String nodeId, String channelId) {
        OutgoingBatch batch = new OutgoingBatch(nodeId, channelId, BatchType.EVENTS);
        outgoingBatchService.insertOutgoingBatch(template, batch);
        return batch;
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

    public void setBootstrapService(IBootstrapService bootstrapService) {
        this.bootstrapService = bootstrapService;
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
}
