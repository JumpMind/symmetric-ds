/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>
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
import java.sql.Types;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.BatchType;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticName;
import org.jumpmind.symmetric.util.MaxRowsStatementCreator;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.transaction.annotation.Transactional;

public class OutgoingBatchService extends AbstractService implements IOutgoingBatchService {

    final static Log logger = LogFactory.getLog(OutgoingBatchService.class);

    private INodeService nodeService;

    private IDbDialect dbDialect;
    
    private IStatisticManager statisticManager;

    /**
     * Create a batch and mark events as tied to that batch. We iterate through all the events so we can find
     * a transaction boundary to stop on. <p/> This method is currently non-transactional because of the fear
     * of having to deal with large numbers of events as part of the same batch. This shouldn't be an issue in
     * most cases other than possibly leaving a batch row w/out data every now and then or leaving a batch
     * w/out the associated history row.
     */
    @Transactional
    @Deprecated
    public void buildOutgoingBatches(final String nodeId, final List<NodeChannel> channels) {
        for (NodeChannel nodeChannel : channels) {
            buildOutgoingBatches(nodeId, nodeChannel);
        }
    }

    @Transactional
    public void buildOutgoingBatches(final String nodeId, final NodeChannel channel) {
        
        final int batchSizePeekAhead = parameterService.getInt(ParameterConstants.OUTGOING_BATCH_PEEK_AHEAD_WINDOW);
        
        jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {

                PreparedStatement update = null;
                try {
                    update = conn.prepareStatement(getSql("updateBatchedEventsSql"));

                    update.setQueryTimeout(jdbcTemplate.getQueryTimeout());

                    if (channel.isSuspended()) {
                        logger.warn(channel.getId() + " channel for " + nodeId + " is currently suspended.");
                    } else if (channel.isEnabled()) {
                        // determine which transactions will be part of this batch on this channel
                        PreparedStatement select = null;
                        ResultSet results = null;

                        try {

                            select = conn.prepareStatement(getSql("selectEventsToBatchSql"));

                            select.setQueryTimeout(jdbcTemplate.getQueryTimeout());

                            select.setString(1, nodeId);
                            select.setString(2, channel.getId());
                            results = select.executeQuery();

                            int count = 0;
                            long databaseMillis = 0;
                            int dataEventCount = 0;
                            boolean peekAheadMode = false;
                            int peekAheadCountDown = batchSizePeekAhead;
                            Set<String> transactionIds = new HashSet<String>();

                            OutgoingBatch newBatch = new OutgoingBatch();
                            newBatch.setBatchType(BatchType.EVENTS);
                            newBatch.setChannelId(channel.getId());
                            newBatch.setNodeId(nodeId);

                            // node channel is setup to ignore, just mark the batch as already processed.
                            if (channel.isIgnored()) {
                                newBatch.setStatus(Status.OK);
                            }

                            if (results.next()) {

                                databaseMillis = 0;
                                dataEventCount = 0;
                                insertOutgoingBatch(newBatch);
                                OutgoingBatchHistory history = new OutgoingBatchHistory(newBatch);

                                do {
                                    String trxId = results.getString(1);

                                    if (!peekAheadMode
                                            || (peekAheadMode && (trxId != null && transactionIds
                                                    .contains(trxId)))) {
                                        peekAheadCountDown = batchSizePeekAhead;
                                        
                                        if (trxId != null) {
                                            transactionIds.add(trxId);
                                        }

                                        int dataId = results.getInt(2);

                                        update.clearParameters();
                                        update.setLong(1, Long.valueOf(newBatch.getBatchId()));
                                        update.setString(2, nodeId);
                                        update.setLong(3, dataId);
                                        update.addBatch();

                                        count++;
                                        dataEventCount++;

                                    } else {
                                        peekAheadCountDown--;
                                    }

                                    if (count > channel.getMaxBatchSize()) {
                                        peekAheadMode = true;
                                    }

                                    // put this in so we don't build up too many
                                    // statements to send to the server.
                                    if (count % 10000 == 0) {
                                        long startTime = System.currentTimeMillis();
                                        update.executeBatch();
                                        databaseMillis += (System.currentTimeMillis() - startTime);
                                    }

                                } while (results.next() && peekAheadCountDown != 0);
                                
                                long startTime = System.currentTimeMillis();
                                update.executeBatch();
                                databaseMillis += (System.currentTimeMillis() - startTime);
                                
                                history.setEndTime(new Date());
                                history.setDataEventCount(dataEventCount);
                                history.setDatabaseMillis(databaseMillis);
                                insertOutgoingBatchHistory(history);
                                statisticManager.getStatistic(StatisticName.OUTGOING_MS_PER_EVENT_BATCHED).add(databaseMillis, dataEventCount);
                                statisticManager.getStatistic(StatisticName.OUTGOING_EVENTS_PER_BATCH).add(dataEventCount, 1);

                            }

                        } finally {
                            JdbcUtils.closeResultSet(results);
                            JdbcUtils.closeStatement(select);
                        }

                        
                    }
                } finally {
                    JdbcUtils.closeStatement(update);
                }
                return null;
            }
        });
    }

    public void insertOutgoingBatch(final OutgoingBatch outgoingBatch) {
        long batchId = dbDialect.insertWithGeneratedKey(getSql("createBatchSql"), "sym_outgoing_batch_batch_id",
                new PreparedStatementCallback() {
                    public Object doInPreparedStatement(PreparedStatement ps) throws SQLException,
                            DataAccessException {
                        ps.setString(1, outgoingBatch.getNodeId());
                        ps.setString(2, outgoingBatch.getChannelId());
                        ps.setString(3, outgoingBatch.getStatus().name());
                        ps.setString(4, outgoingBatch.getBatchType().getCode());
                        return null;
                    }
                });
        outgoingBatch.setBatchId(batchId);
    }

    /**
     * Select batches to process. Batches that are NOT in error will be returned first. They will be ordered
     * by batch id as the batches will have already been created by {@link #buildOutgoingBatches(String)} in
     * channel priority order.
     */
    @SuppressWarnings("unchecked")
    public List<OutgoingBatch> getOutgoingBatches(String nodeId) {
        return (List<OutgoingBatch>) jdbcTemplate.query(getSql("selectOutgoingBatchSql"), new Object[] { nodeId },
                new OutgoingBatchMapper());
    }

    @SuppressWarnings("unchecked")
    public List<OutgoingBatch> getOutgoingBatchRange(String startBatchId, String endBatchId) {
        return (List<OutgoingBatch>) jdbcTemplate.query(getSql("selectOutgoingBatchRangeSql"), new Object[] {
                startBatchId, endBatchId }, new OutgoingBatchMapper());
    }

    @SuppressWarnings("unchecked")
    public List<OutgoingBatch> getOutgoingBatcheErrors(int maxRows) {
        return (List<OutgoingBatch>) jdbcTemplate.query(new MaxRowsStatementCreator(
                getSql("selectOutgoingBatchErrorsSql"), maxRows), new OutgoingBatchMapper());
    }

    // Moving away from SENT status to reduce updates to outgoing_batch table
    @Deprecated
    public void markOutgoingBatchSent(OutgoingBatch batch) {
        setBatchStatus(batch.getBatchId(), batch.getStatus());
    }

    @Deprecated
    public void setBatchStatus(long batchId, Status status) {
        jdbcTemplate.update(getSql("changeBatchStatusSql"), new Object[] { status.name(), batchId });
    }

    // TODO Should this move to DataService?
    @SuppressWarnings("unchecked")
    public boolean isInitialLoadComplete(String nodeId) {

        NodeSecurity security = nodeService.findNodeSecurity(nodeId);
        if (security == null || security.isInitialLoadEnabled()) {
            return false;
        }

        List<String> statuses = (List<String>) jdbcTemplate.queryForList(getSql("initialLoadStatusSql"),
                new Object[] { nodeId }, String.class);
        if (statuses == null || statuses.size() == 0) {
            throw new RuntimeException("The initial load has not been started for " + nodeId);
        }

        for (String status : statuses) {
            if (!Status.OK.name().equals(status)) {
                return false;
            }
        }
        return true;
    }

    public void insertOutgoingBatchHistory(OutgoingBatchHistory history) {
        jdbcTemplate.update(getSql("insertOutgoingBatchHistorySql"), new Object[] { history.getBatchId(),
                history.getNodeId(), history.getStatus().toString(), history.getNetworkMillis(),
                history.getFilterMillis(), history.getDatabaseMillis(), history.getHostName(),
                history.getByteCount(), history.getDataEventCount(), history.getFailedDataId(),
                history.getStartTime(), history.getEndTime(), history.getSqlState(), history.getSqlCode(),
                StringUtils.abbreviate(history.getSqlMessage(), 50) }, new int[] { Types.INTEGER,
                Types.VARCHAR, Types.CHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR,
                Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.TIMESTAMP, Types.TIMESTAMP, Types.VARCHAR,
                Types.INTEGER, Types.VARCHAR });
    }

    @SuppressWarnings("unchecked")
    public List<OutgoingBatchHistory> findOutgoingBatchHistory(long batchId, String nodeId) {
        return (List<OutgoingBatchHistory>) jdbcTemplate.query(getSql("findOutgoingBatchHistorySql"), new Object[] {
                batchId, nodeId }, new OutgoingBatchHistoryMapper());
    }

    class OutgoingBatchMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int num) throws SQLException {
            OutgoingBatch batch = new OutgoingBatch();
            batch.setBatchId(rs.getLong(1));
            batch.setNodeId(rs.getString(2));
            batch.setChannelId(rs.getString(3));
            batch.setStatus(rs.getString(4));
            batch.setBatchType(rs.getString(5));
            batch.setCreateTime(rs.getTimestamp(6));
            return batch;
        }
    }

    class OutgoingBatchHistoryMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int num) throws SQLException {
            OutgoingBatchHistory history = new OutgoingBatchHistory();
            history.setBatchId(rs.getLong(1));
            history.setNodeId(rs.getString(2));
            history.setStatus(OutgoingBatchHistory.Status.valueOf(rs.getString(3)));
            history.setNetworkMillis(rs.getLong(4));
            history.setFilterMillis(rs.getLong(5));
            history.setDatabaseMillis(rs.getLong(6));
            history.setHostName(rs.getString(7));
            history.setByteCount(rs.getLong(8));
            history.setDataEventCount(rs.getLong(9));
            history.setFailedDataId(rs.getLong(10));
            history.setStartTime(rs.getTime(11));
            history.setEndTime(rs.getTime(12));
            history.setSqlState(rs.getString(13));
            history.setSqlCode(rs.getInt(14));
            history.setSqlMessage(rs.getString(15));
            return history;
        }
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }

}
