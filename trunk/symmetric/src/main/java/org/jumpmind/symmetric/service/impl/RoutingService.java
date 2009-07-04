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
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.route.IChannelBatchController;
import org.jumpmind.symmetric.route.IDataRouter;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IRoutingService;
import org.jumpmind.symmetric.service.LockActionConstants;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.support.JdbcUtils;

/**
 * @since 2.0
 */
public class RoutingService extends AbstractService implements IRoutingService {

    class BatchesByChannel {
        NodeChannel channel;
        Map<String, OutgoingBatch> batchesByNodes = new HashMap<String, OutgoingBatch>();
        Map<String, OutgoingBatchHistory> batchHistoryByNodes = new HashMap<String, OutgoingBatchHistory>();
        IChannelBatchController controller;
        Connection connection;
        SingleConnectionDataSource dataSource;
    }

    IClusterService clusterService;

    IConfigurationService configurationService;

    IOutgoingBatchService outgoingBatchService;

    /**
     * This method will route data to specific nodes.
     * 
     * @since 2.0
     * @return true if data was routed
     */
    public boolean route() {
        if (clusterService.lock(LockActionConstants.ROUTE)) {
            final int peekAheadLength = parameterService.getInt(ParameterConstants.OUTGOING_BATCH_PEEK_AHEAD_WINDOW);
            final Map<String, BatchesByChannel> batches = initBatchesByChannel();
            try {
                return (Boolean) jdbcTemplate.execute(new ConnectionCallback() {
                    public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                        boolean batchedData = false;
                        PreparedStatement ps = conn.prepareStatement(getSql("selectDataToBatchSql"),
                                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                        ps.setFetchSize(dbDialect.getStreamingResultsFetchSize());
                        ResultSet rs = ps.executeQuery();
                        try {
                            Map<String, Long> transactionIdDataId = new HashMap<String, Long>();
                            LinkedList<Data> dataStack = new LinkedList<Data>();
                            for (int i = 0; i < peekAheadLength && rs.next(); i++) {
                                Data data = new Data(rs, configurationService);
                                dataStack.addLast(data);
                                transactionIdDataId.put("", data.getDataId()); // TODO
                                                                               // data.getTransactionId()
                            }

                            while (dataStack.size() > 0) {
                                Data data = dataStack.poll();
                                boolean databaseTransactionBoundary = transactionIdDataId.get("") == data.getDataId(); // TODO
                                                                                                                       // data.getTransactionId()
                                Trigger trigger = configurationService.getTriggerById(data.getTriggerHistory()
                                        .getTriggerHistoryId());
                                IDataRouter router = trigger.getDataRouter();
                                Set<String> nodeIds = router.routeToNodes(data);
                                String channelId = trigger.getChannelId();
                                BatchesByChannel batchInfo = batches.get(channelId);
                                boolean commit = false;
                                if (nodeIds != null && nodeIds.size() > 0) {
                                    for (String nodeId : nodeIds) {
                                        OutgoingBatch batch = batchInfo.batchesByNodes.get(nodeId);
                                        OutgoingBatchHistory history = batchInfo.batchHistoryByNodes.get(nodeId);
                                        if (batch == null) {
                                            batch = createNewBatch(batchInfo.dataSource, nodeId, channelId);
                                            batchInfo.batchesByNodes.put(nodeId, batch);
                                            history = new OutgoingBatchHistory(batch);
                                            batchInfo.batchHistoryByNodes.put(nodeId, history);
                                        }
                                        history.incrementDataEventCount();

                                        insertDataEvent(batchInfo.dataSource, data, batch.getBatchId(), nodeId);
                                        if (batchInfo.controller.completeBatch(history, batch, data,
                                                databaseTransactionBoundary)) {
                                            insertOutgoingBatchHistory(batchInfo.dataSource, history);
                                            batchInfo.batchesByNodes.remove(nodeId);
                                            batchInfo.batchHistoryByNodes.remove(nodeId);
                                            commit = true;
                                        }

                                        batchedData = true;

                                    }
                                } else {
                                    insertDataEvent(batchInfo.dataSource, data, -1, "-1");
                                }

                                if (commit) {
                                    batchInfo.connection.commit();
                                }

                                if (rs.next()) {
                                    data = new Data(rs, configurationService);
                                    dataStack.addLast(data);
                                    transactionIdDataId.put("", data.getDataId()); // TODO
                                                                                   // data.getTransactionId()
                                }
                            }
                        } finally {
                            JdbcUtils.closeResultSet(rs);
                            JdbcUtils.closeStatement(ps);
                        }
                        return batchedData;
                    }
                });
            } finally {
                for (BatchesByChannel batch : batches.values()) {
                    JdbcUtils.closeConnection(batch.connection);
                }
                clusterService.unlock(LockActionConstants.ROUTE);
            }
        } else {
            return false;
        }
    }

    protected Map<String, BatchesByChannel> initBatchesByChannel() {
        final List<NodeChannel> channels = configurationService.getChannels();
        final Map<String, BatchesByChannel> batches = new HashMap<String, BatchesByChannel>(channels.size());
        try {
            for (NodeChannel nodeChannel : channels) {
                BatchesByChannel b = new BatchesByChannel();
                b.channel = nodeChannel;
                b.controller = nodeChannel.createChannelBatchController();
                b.connection = dataSource.getConnection();
                b.connection.setAutoCommit(false);
                b.dataSource = new SingleConnectionDataSource(b.connection, true);
                batches.put(nodeChannel.getId(), b);
            }
            return batches;
        } catch (SQLException e) {
            for (BatchesByChannel batch : batches.values()) {
                JdbcUtils.closeConnection(batch.connection);
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
}
