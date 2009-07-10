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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.route.IBatchAlgorithm;
import org.jumpmind.symmetric.route.IDataRouter;
import org.jumpmind.symmetric.route.IRoutingContext;
import org.jumpmind.symmetric.route.RoutingContext;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IRoutingService;
import org.jumpmind.symmetric.service.LockActionConstants;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.support.JdbcUtils;

/**
 * @since 2.0
 */
public class RoutingService extends AbstractService implements IRoutingService {

    protected IClusterService clusterService;

    protected IDataService dataService;

    protected IConfigurationService configurationService;

    protected IOutgoingBatchService outgoingBatchService;

    protected INodeService nodeService;

    protected Map<String, IDataRouter> routers;

    protected Map<String, IBatchAlgorithm> batchAlgorithms;

    public boolean routeInitialLoadData(Data data, Trigger trigger, Node node) {
        // TODO use IDataRouter
        return true;
    }

    /**
     * This method will route data to specific nodes.
     * 
     * @return true if data was routed
     */
    public boolean routeData() {
        if (clusterService.lock(LockActionConstants.ROUTE)) {
            final Map<String, IRoutingContext> routingContexts = initRoutingContexts();
            try {
                return (Boolean) jdbcTemplate.execute(new ConnectionCallback() {
                    public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                        selectDataAndRoute(c, routingContexts);
                        return null;
                    }
                });
            } finally {
                for (IRoutingContext batch : routingContexts.values()) {
                    try {
                        batch.commit();
                    } catch (SQLException e) {
                        logger.error(e, e);
                    } finally {
                        batch.cleanup();
                    }
                }
                clusterService.unlock(LockActionConstants.ROUTE);
            }
        } else {
            return false;
        }
    }

    public IDataRouter getDataRouter(String routingExpression, IRoutingContext routingContext) {
        IDataRouter router = routingContext.getDataRouters().get(routingExpression);
        if (router == null) {
            // TODO create router
            // clone and set properties?
        }
        return router;
    }

    protected Set<Node> findAvailableNodes(Trigger trigger, IRoutingContext batches) {
        Set<Node> nodes = batches.getAvailableNodes().get(trigger);
        if (nodes == null) {
            // TODO call the node service to get the appropriate nodes for this
            // trigger
            // nodes.addAll(nodeService.findNodesTargetedByTrigger(trigger);
            batches.getAvailableNodes().put(trigger, nodes);
        }
        return nodes;
    }

    protected void selectDataAndRoute(Connection conn, Map<String, IRoutingContext> batches) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(getSql("selectDataToBatchSql"), ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        ps.setFetchSize(dbDialect.getStreamingResultsFetchSize());
        ResultSet rs = ps.executeQuery();
        try {
            int peekAheadLength = parameterService.getInt(ParameterConstants.OUTGOING_BATCH_PEEK_AHEAD_WINDOW);
            Map<String, Long> transactionIdDataId = new HashMap<String, Long>();
            LinkedList<Data> dataQueue = new LinkedList<Data>();
            // pre-populate data queue so we can look ahead to see if a
            // transaction has finished.
            for (int i = 0; i < peekAheadLength && rs.next(); i++) {
                readData(rs, dataQueue, transactionIdDataId);
            }

            while (dataQueue.size() > 0) {
                routeData(dataQueue.poll(), transactionIdDataId, batches);
                if (rs.next()) {
                    readData(rs, dataQueue, transactionIdDataId);
                }
            }

        } finally {
            JdbcUtils.closeResultSet(rs);
            JdbcUtils.closeStatement(ps);
        }
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

    protected void routeData(Data data, Map<String, Long> transactionIdDataId,
            Map<String, IRoutingContext> routingContexts) throws SQLException {
        Long dataId = transactionIdDataId.get(data.getTransactionId());
        boolean databaseTransactionBoundary = dataId == null ? true : dataId == data.getDataId();
        Trigger trigger = configurationService.getTriggerById(data.getTriggerHistory().getTriggerHistoryId());
        String channelId = trigger.getChannelId();
        IRoutingContext routingContext = routingContexts.get(channelId);
        IDataRouter router = getDataRouter(trigger.getRoutingExpression(), routingContext);
        Set<String> nodeIds = router.routeToNodes(data, trigger, findAvailableNodes(trigger, routingContext),
                routingContext.getChannel(), false);
        boolean commit = false;
        boolean routed = false;
        if (nodeIds != null && nodeIds.size() > 0) {
            for (String nodeId : nodeIds) {
                if (data.getSourceNodeId() == null || !data.getSourceNodeId().equals(nodeId)) {
                    OutgoingBatch batch = routingContext.getBatchesByNodes().get(nodeId);
                    OutgoingBatchHistory history = routingContext.getBatchHistoryByNodes().get(nodeId);
                    if (batch == null) {
                        batch = createNewBatch(routingContext.getDataSource(), nodeId, channelId);
                        routingContext.getBatchesByNodes().put(nodeId, batch);
                        history = new OutgoingBatchHistory(batch);
                        routingContext.getBatchHistoryByNodes().put(nodeId, history);
                    }
                    history.incrementDataEventCount();
                    routed = true;
                    insertDataEvent(routingContext.getDataSource(), data, batch.getBatchId(), nodeId);
                    if (batchAlgorithms.get(routingContext.getChannel().getBatchAlgorithm()).completeBatch(
                            routingContext.getChannel(), history, batch, data, databaseTransactionBoundary)) {
                        insertOutgoingBatchHistory(routingContext.getDataSource(), history);
                        routingContext.getBatchesByNodes().remove(nodeId);
                        routingContext.getBatchHistoryByNodes().remove(nodeId);
                        commit = true;
                    }
                }
            }
        }

        if (!routed) {
            insertDataEvent(routingContext.getDataSource(), data, -1, "-1");
        }

        if (commit) {
            routingContext.commit();
        }

    }

    protected Map<String, IRoutingContext> initRoutingContexts() {
        final List<NodeChannel> channels = configurationService.getChannels();
        final Map<String, IRoutingContext> contexts = new HashMap<String, IRoutingContext>(channels.size());
        try {
            for (NodeChannel nodeChannel : channels) {
                IRoutingContext b = new RoutingContext(nodeChannel, dataSource.getConnection());
                contexts.put(nodeChannel.getId(), b);
            }
            return contexts;
        } catch (SQLException e) {
            for (IRoutingContext ctx : contexts.values()) {
                ctx.cleanup();
            }
            throw new CannotGetJdbcConnectionException(e.getMessage(), e);
        }
    }

    protected void insertOutgoingBatchHistory(DataSource dataSource, OutgoingBatchHistory history) {
        // TODO
    }

    protected void insertDataEvent(DataSource ds2use, Data data, long batchId, String nodeId) {
        // TODO
    }

    protected OutgoingBatch createNewBatch(DataSource ds2use, String nodeId, String channelId) {
        // TODO
        return null;
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
}
