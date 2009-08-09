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
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventAction;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.DataRef;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.route.IBatchAlgorithm;
import org.jumpmind.symmetric.route.IDataRouter;
import org.jumpmind.symmetric.route.IRouterContext;
import org.jumpmind.symmetric.route.RouterContext;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.ITriggerService;
import org.jumpmind.symmetric.service.LockActionConstants;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.support.JdbcUtils;

/**
 * This service is responsible for routing data to specific nodes and managing the batching of data to be delivered to
 * each node.
 * 
 * @since 2.0
 */
public class RouterService extends AbstractService implements IRouterService {

    private IClusterService clusterService;

    private IDataService dataService;

    private IConfigurationService configurationService;
    
    private ITriggerService triggerService;

    private IOutgoingBatchService outgoingBatchService;

    private INodeService nodeService;

    private Map<String, IDataRouter> routers;

    private Map<String, IBatchAlgorithm> batchAlgorithms;

    public boolean shouldDataBeRouted(IRouterContext context, DataMetaData dataMetaData, Set<Node> nodes,
            boolean initialLoad) {
        IDataRouter router = getDataRouter(dataMetaData.getTrigger());
        Collection<String> nodeIds = router.routeToNodes(context, dataMetaData, nodes, initialLoad);
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
                Node sourceNode = nodeService.findIdentity();
                DataRef ref = dataService.getDataRef();
                routeDataForEachChannel(ref, sourceNode);
                findAndSaveNextDataId(ref);
            } finally {
                clusterService.unlock(LockActionConstants.ROUTE);
            }
        }
    }

    /**
     * We route data channel by channel for two reasons. One is that if/when we decide to multi-thread the routing it is
     * a simple matter of inserting a thread pool here and waiting for all channels to be processed. The other reason is
     * to reduce the number of connections we are required to have.
     */
    protected void routeDataForEachChannel(DataRef ref, Node sourceNode) {
        final List<NodeChannel> channels = configurationService.getChannels();
        for (NodeChannel nodeChannel : channels) {
            if (!nodeChannel.isSuspended()) {
                routeDataForChannel(ref, nodeChannel, sourceNode);
            }
        }
    }

    protected void routeDataForChannel(final DataRef ref, final NodeChannel nodeChannel, final Node sourceNode) {
        jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                RouterContext context = null;
                try {
                    context = new RouterContext(sourceNode.getNodeId(), nodeChannel, dataSource);
                    selectDataAndRoute(c, ref, context);
                } catch (Exception ex) {
                    if (context != null) {
                        context.rollback();
                    }
                    logger.error(String.format("Failed to route and batch data on '%s' channel", nodeChannel.getId()),
                            ex);
                } finally {
                    try {
                        List<OutgoingBatch> batches = new ArrayList<OutgoingBatch>(context.getBatchesByNodes().values());
                        for (OutgoingBatch batch : batches) {
                            completeBatch(batch, context);
                        }
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

    protected void findAndSaveNextDataId(final DataRef ref) {
        jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    ps = c.prepareStatement(getSql("selectDistinctDataIdFromDataEventSql"),
                            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    ps.setFetchSize(dbDialect.getStreamingResultsFetchSize());
                    ps.setLong(1, ref.getRefDataId());
                    rs = ps.executeQuery();
                    long lastDataId = -1;
                    while (rs.next()) {
                        long dataId = rs.getLong(1);
                        if (lastDataId == -1 || lastDataId + 1 == dataId) {
                            lastDataId = dataId;
                        } else {
                            if (isDataGapExpired(dataId, ref)) {
                                lastDataId = dataId;
                            } else {
                                // detected a gap!
                                break;
                            }
                        }
                    }

                    dataService.saveDataRef(new DataRef(lastDataId, new Date()));

                } finally {
                    JdbcUtils.closeResultSet(rs);
                    JdbcUtils.closeStatement(ps);
                }
                return null;
            }
        });
    }

    protected boolean isDataGapExpired(long dataId, DataRef ref) {
        long gapTimoutInMs = parameterService.getLong(ParameterConstants.ROUTING_STALE_DATA_ID_GAP_TIME);
        Date createTime = dataService.findCreateTimeOfEvent(dataId);
        Date lastRecordedTime = ref.getRefTime();
        if (lastRecordedTime != null && createTime != null
                && lastRecordedTime.getTime() - createTime.getTime() > gapTimoutInMs) {
            return true;
        } else {
            return false;
        }
    }

    protected Set<Node> findAvailableNodes(Trigger trigger, RouterContext context) {
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
     * Pre-read data and fill up a queue so we can peek ahead to see if we have crossed a database transaction boundary.
     * Then route each {@link Data} while continuing to keep the queue filled until the result set is entirely read.
     * 
     * @param conn
     *            The connection to use for selecting the data.
     * @param context
     *            The current context of the routing process
     */
    protected void selectDataAndRoute(Connection conn, DataRef ref, RouterContext context) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            // TODO add a flag to sym_channel to indicate whether we need to
            // read the row_data and or old_data for
            // routing. We will get better performance if we don't read the
            // data.
            ps = conn.prepareStatement(getSql("selectDataToBatchSql"), ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            ps.setFetchSize(dbDialect.getStreamingResultsFetchSize());
            ps.setString(1, context.getChannel().getId());
            ps.setLong(2, ref.getRefDataId());
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

    protected void routeData(Data data, Map<String, Long> transactionIdDataId, RouterContext context)
            throws SQLException {
        Long dataId = transactionIdDataId.get(data.getTransactionId());
        context.setEncountedTransactionBoundary(dataId == null ? true : dataId == data.getDataId());
        Trigger trigger = getTriggerForData(data);
        Table table = dbDialect.getMetaDataFor(trigger, true);
        DataMetaData dataMetaData = new DataMetaData(data, table, trigger, context.getChannel());
        if (trigger != null) {
            context.resetForNextData();
            if (!context.getChannel().isIgnored()) {
                IDataRouter dataRouter = getDataRouter(trigger);
                context.addUsedDataRouter(dataRouter);
                Collection<String> nodeIds = dataRouter.routeToNodes(context, dataMetaData, findAvailableNodes(
                        trigger, context), false);
                insertDataEvents(context, dataMetaData, nodeIds);
            }

            if (!context.isRouted()) {
                // mark as not routed anywhere
                dataService.insertDataEvent(context.getJdbcTemplate(), data.getDataId(), -1);
            }

            if (context.isNeedsCommitted()) {
                context.commit();
            }
        } else {
            logger.warn(String.format(
                    "Could not find trigger for trigger id of %s.  Not processing data with the id of %s", data
                            .getTriggerHistory().getTriggerId(), data.getDataId()));
        }

    }

    protected void insertDataEvents(RouterContext context, DataMetaData dataMetaData, Collection<String> nodeIds) {
        if (nodeIds != null && nodeIds.size() > 0) {
            for (String nodeId : nodeIds) {
                if (dataMetaData.getData().getSourceNodeId() == null
                        || !dataMetaData.getData().getSourceNodeId().equals(nodeId)) {
                    OutgoingBatch batch = context.getBatchesByNodes().get(nodeId);
                    if (batch == null) {
                        batch = new OutgoingBatch(nodeId, dataMetaData.getChannel().getId());
                        outgoingBatchService.insertOutgoingBatch(context.getJdbcTemplate(), batch);
                        context.getBatchesByNodes().put(nodeId, batch);
                    }
                    batch.incrementDataEventCount();
                    context.setRouted(true);
                    dataService.insertDataEvent(context.getJdbcTemplate(), dataMetaData.getData().getDataId(),
                            batch.getBatchId());
                    if (batchAlgorithms.get(context.getChannel().getBatchAlgorithm()).isBatchComplete(batch,
                            dataMetaData, context)) {
                        completeBatch(batch, context);
                    }
                }
            }
        }
    }

    protected void completeBatch(OutgoingBatch batch, RouterContext context) {
        batch.setRouterMillis(System.currentTimeMillis() - batch.getCreateTime().getTime());
        Set<IDataRouter> usedRouters = context.getUsedDataRouters();
        for (IDataRouter dataRouter : usedRouters) {
            dataRouter.completeBatch(context);
        }
        outgoingBatchService.updateOutgoingBatch(context.getJdbcTemplate(), batch);
        context.getBatchesByNodes().remove(batch.getNodeId());
        context.setNeedsCommitted(true);
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
        Trigger trigger = triggerService.getActiveTriggersForSourceNodeGroup(parameterService
                                .getString(ParameterConstants.NODE_GROUP_ID), false).get((data.getTriggerHistory().getTriggerId()));
        if (trigger == null) {
            trigger = triggerService.getTriggerById(data.getTriggerHistory().getTriggerId());
            if (trigger == null) {
                throw new IllegalStateException(String.format("Could not find trigger with the id of %s", data
                        .getTriggerHistory().getTriggerId()));
            }
        }
        return trigger;
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
    
    public void setTriggerService(ITriggerService triggerService) {
        this.triggerService = triggerService;
    }
}
