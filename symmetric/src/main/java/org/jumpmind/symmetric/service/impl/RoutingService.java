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
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.BatchType;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventAction;
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

    public boolean routeInitialLoadData(Data data, Trigger trigger, Node node) {
        // TODO use IDataRouter
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
                jdbcTemplate.execute(new ConnectionCallback() {
                    public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                        routeDataForEachChannel(c);
                        return null;
                    }
                });
            } finally {
                clusterService.unlock(LockActionConstants.ROUTE);
            }
        }
    }

    protected void routeDataForEachChannel(Connection c) throws SQLException {
        final List<NodeChannel> channels = configurationService.getChannels();
        for (NodeChannel nodeChannel : channels) {
            IRoutingContext context = null;
            try {
                context = new RoutingContext(nodeChannel, dataSource.getConnection());
                selectDataAndRoute(c, context);
            } finally {
                try {
                    context.commit();
                } catch (SQLException e) {
                    logger.error(e, e);
                } finally {
                    context.cleanup();
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

    protected void selectDataAndRoute(Connection conn, IRoutingContext context) throws SQLException {
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

    protected Data readData(ResultSet rs, LinkedList<Data> dataStack, Map<String, Long> transactionIdDataId)
            throws SQLException {
        Data data = dataService.readData(rs);
        dataStack.addLast(data);
        if (data.getTransactionId() != null) {
            transactionIdDataId.put(data.getTransactionId(), data.getDataId());
        }
        return data;
    }

    protected void routeData(Data data, Map<String, Long> transactionIdDataId, IRoutingContext routingContext)
            throws SQLException {
        Long dataId = transactionIdDataId.get(data.getTransactionId());
        boolean databaseTransactionBoundary = dataId == null ? true : dataId == data.getDataId();
        Trigger trigger = bootstrapService.getCachedTriggers(false).get((data.getTriggerHistory().getTriggerId()));
        if (trigger != null) {
            String channelId = trigger.getChannelId();
            if (channelId.equals(routingContext.getChannel().getId())) {
                IDataRouter router = getDataRouter(trigger);
                Collection<String> nodeIds = router.routeToNodes(data, trigger, findAvailableNodes(trigger,
                        routingContext), routingContext.getChannel(), false);
                boolean commit = false;
                boolean routed = false;
                if (nodeIds != null && nodeIds.size() > 0) {
                    for (String nodeId : nodeIds) {
                        if (data.getSourceNodeId() == null || !data.getSourceNodeId().equals(nodeId)) {
                            OutgoingBatch batch = routingContext.getBatchesByNodes().get(nodeId);
                            OutgoingBatchHistory history = routingContext.getBatchHistoryByNodes().get(nodeId);
                            if (batch == null) {
                                batch = createNewBatch(routingContext.getJdbcTemplate(), nodeId, channelId);
                                routingContext.getBatchesByNodes().put(nodeId, batch);
                                history = new OutgoingBatchHistory(batch);
                                routingContext.getBatchHistoryByNodes().put(nodeId, history);
                            }
                            history.incrementDataEventCount();
                            routed = true;
                            insertDataEvent(routingContext.getJdbcTemplate(), data, batch.getBatchId(), nodeId);
                            if (batchAlgorithms.get(routingContext.getChannel().getBatchAlgorithm()).completeBatch(
                                    routingContext.getChannel(), history, batch, data, databaseTransactionBoundary)) {
                                insertOutgoingBatchHistory(routingContext.getJdbcTemplate(), history);
                                routingContext.getBatchesByNodes().remove(nodeId);
                                routingContext.getBatchHistoryByNodes().remove(nodeId);
                                commit = true;
                            }
                        }
                    }
                }

                if (!routed) {
                    insertDataEvent(routingContext.getJdbcTemplate(), data, -1, "-1");
                }

                if (commit) {
                    routingContext.commit();
                }
            }
        } else {
            logger.warn(String.format(
                    "Could not find trigger for trigger id of %s.  Not processing data with the id of %s", data
                            .getTriggerHistory().getTriggerId(), data.getDataId()));
        }

    }

    protected void insertOutgoingBatchHistory(JdbcTemplate template, OutgoingBatchHistory history) {
        outgoingBatchService.insertOutgoingBatchHistory(template, history);
    }

    protected void insertDataEvent(JdbcTemplate template, Data data, long batchId, String nodeId) {
        dataService.insertDataEvent(template, data.getDataId(), nodeId, batchId);
    }

    protected OutgoingBatch createNewBatch(JdbcTemplate template, String nodeId, String channelId) {
        OutgoingBatch batch = new OutgoingBatch(nodeId, channelId, BatchType.EVENTS);
        outgoingBatchService.insertOutgoingBatch(template, batch);
        return batch;
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
