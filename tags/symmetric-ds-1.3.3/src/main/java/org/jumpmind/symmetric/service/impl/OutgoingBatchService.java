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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.BatchType;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchHistoryService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.util.MaxRowsStatementCreator;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.transaction.annotation.Transactional;

public class OutgoingBatchService extends AbstractService implements IOutgoingBatchService {

    final static Log logger = LogFactory.getLog(OutgoingBatchService.class);

    IConfigurationService configurationService;
    
    INodeService nodeService;

    private int batchSizePeekAhead = 100;

    private String selectEventsToBatchSql;

    private String updateBatchedEventsSql;

    private String createBatchSql;

    private String selectOutgoingBatchSql;

    private String selectOutgoingBatchRangeSql;

    private String selectOutgoingBatchErrorsSql;

    private String changeBatchStatusSql;

    private String initialLoadStatusSql;

    private JdbcTemplate outgoingBatchQueryTemplate;

    private IOutgoingBatchHistoryService historyService;

    private IDbDialect dbDialect;

    /**
     * Create a batch and mark events as tied to that batch.  We iterate through 
     * all the events so we can find a transaction boundary to stop on.
     * <p/>
     * This method is currently non-transactional because of the fear of having to deal with
     * large numbers of events as part of the same batch.  This shouldn't be an issue in most cases
     * other than possibly leaving a batch row w/out data every now and then or leaving a batch w/out the
     * associated history row.
     */
    @Transactional
    @Deprecated
    public void buildOutgoingBatches(final String nodeId, final List<NodeChannel> channels) {
        for (NodeChannel nodeChannel : channels)  {
            buildOutgoingBatches(nodeId, nodeChannel);
        }
    }
    
    @Transactional
    public void buildOutgoingBatches(final String nodeId, final NodeChannel channel) {
        
        jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {

                PreparedStatement update = null;
                try {
                    update = conn.prepareStatement(updateBatchedEventsSql);

                    update.setQueryTimeout(jdbcTemplate.getQueryTimeout());

                        if (channel.isSuspended()) {
                            logger.warn(channel.getId() + " channel for " + nodeId + " is currently suspended.");
                        } else if (channel.isEnabled()) {
                            // determine which transactions will be part of this batch on this channel
                            PreparedStatement select = null;
                            ResultSet results = null;

                            try {

                                select = conn.prepareStatement(selectEventsToBatchSql);

                                select.setQueryTimeout(jdbcTemplate.getQueryTimeout());

                                select.setString(1, nodeId);
                                select.setString(2, channel.getId());
                                results = select.executeQuery();

                                int count = 0;
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

                                    insertOutgoingBatch(newBatch);

                                    do {
                                        String trxId = results.getString(1);
                                        if (trxId != null) {
                                            transactionIds.add(trxId);
                                        }

                                        if (!peekAheadMode
                                                || (peekAheadMode && (trxId != null && transactionIds.contains(trxId)))) {
                                            peekAheadCountDown = batchSizePeekAhead;

                                            int dataId = results.getInt(2);

                                            update.clearParameters();
                                            update.setLong(1, Long.valueOf(newBatch.getBatchId()));
                                            update.setString(2, nodeId);
                                            update.setLong(3, dataId);
                                            update.addBatch();

                                            count++;

                                        } else {
                                            peekAheadCountDown--;
                                        }

                                        if (count > channel.getMaxBatchSize()) {
                                            peekAheadMode = true;
                                        }

                                        // put this in so we don't build up too many
                                        // statements to send to the server.
                                        if (count % 10000 == 0) {
                                            update.executeBatch();
                                        }

                                    } while (results.next() && peekAheadCountDown != 0);

                                    historyService.created(new Integer(newBatch.getBatchId()), count);
                                }

                            } finally {

                                JdbcUtils.closeResultSet(results);
                                JdbcUtils.closeStatement(select);

                            }

                        update.executeBatch();
                    }
                } finally {
                    JdbcUtils.closeStatement(update);
                }
                return null;
            }
        });
    }

    public void insertOutgoingBatch(final OutgoingBatch outgoingBatch)  {
        long batchId = dbDialect.insertWithGeneratedKey(createBatchSql, "sym_outgoing_batch_batch_id_seq",
                new PreparedStatementCallback() {
                    public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                        ps.setString(1, outgoingBatch.getNodeId());
                        ps.setString(2, outgoingBatch.getChannelId());
                        ps.setString(3, outgoingBatch.getStatus().name());
                        ps.setString(4, outgoingBatch.getBatchType().getCode());
                        return null;
                    }
                });
        outgoingBatch.setBatchId(String.valueOf(batchId));
    }

    /**
     * Select batches to process.  Batches that are NOT in error will be returned first.  They will be ordered
     * by batch id as the batches will have already been created by {@link #buildOutgoingBatches(String)} in channel
     * priority order.
     */
    @SuppressWarnings("unchecked")
    public List<OutgoingBatch> getOutgoingBatches(String nodeId) {
        return (List<OutgoingBatch>) outgoingBatchQueryTemplate.query(selectOutgoingBatchSql,
                new Object[] { nodeId }, new OutgoingBatchMapper());
    }

    @SuppressWarnings("unchecked")
    public List<OutgoingBatch> getOutgoingBatchRange(String startBatchId, String endBatchId) {
        return (List<OutgoingBatch>) outgoingBatchQueryTemplate.query(selectOutgoingBatchRangeSql, new Object[] {
                startBatchId, endBatchId }, new OutgoingBatchMapper());
    }

    @SuppressWarnings("unchecked")
    public List<OutgoingBatch> getOutgoingBatcheErrors(int maxRows) {
        return (List<OutgoingBatch>) outgoingBatchQueryTemplate.query(new MaxRowsStatementCreator(
                selectOutgoingBatchErrorsSql, maxRows), new OutgoingBatchMapper());
    }

    public void markOutgoingBatchSent(OutgoingBatch batch) {
        setBatchStatus(batch.getBatchId(), Status.SE);
    }

    public void setBatchStatus(String batchId, Status status) {
        jdbcTemplate.update(changeBatchStatusSql, new Object[] { status.name(), batchId });

        if (status == Status.SE) {
            historyService.sent(new Integer(batchId));
        } else if (status == Status.ER) {
            historyService.error(new Integer(batchId), 0L);
        } else if (status == Status.OK) {
            historyService.ok(new Integer(batchId));
        }
    }

    // TODO Should this move to DataService?
    @SuppressWarnings("unchecked")
    public boolean isInitialLoadComplete(String nodeId) {

        NodeSecurity security = nodeService.findNodeSecurity(nodeId);
        if (security == null || security.isInitialLoadEnabled()) {
            return false;
        }

        List<String> statuses = (List<String>) jdbcTemplate.queryForList(initialLoadStatusSql, new Object[] { nodeId },
                String.class);
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


    class OutgoingBatchMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int num) throws SQLException {
            OutgoingBatch batch = new OutgoingBatch();
            batch.setBatchId(rs.getString(1));
            batch.setNodeId(rs.getString(2));
            batch.setChannelId(rs.getString(3));
            batch.setStatus(rs.getString(4));
            batch.setBatchType(rs.getString(5));
            batch.setCreateTime(rs.getTimestamp(6));
            return batch;
        }
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setCreateBatchSql(String createBatchSql) {
        this.createBatchSql = createBatchSql;
    }

    public void setSelectEventsToBatchSql(String selectEventsToBatchSql) {
        this.selectEventsToBatchSql = selectEventsToBatchSql;
    }

    public void setUpdateBatchedEventsSql(String updateBatchedEventsSql) {
        this.updateBatchedEventsSql = updateBatchedEventsSql;
    }

    public void setSelectOutgoingBatchSql(String selectOutgoingBatch) {
        this.selectOutgoingBatchSql = selectOutgoingBatch;
    }

    public void setChangeBatchStatusSql(String markBatchSentSql) {
        this.changeBatchStatusSql = markBatchSentSql;
    }

    public void setHistoryService(IOutgoingBatchHistoryService historyService) {
        this.historyService = historyService;
    }

    public void setInitialLoadStatusSql(String initialLoadStatusSql) {
        this.initialLoadStatusSql = initialLoadStatusSql;
    }

    public void setOutgoingBatchQueryTemplate(JdbcTemplate outgoingBatchQueryTemplate) {
        this.outgoingBatchQueryTemplate = outgoingBatchQueryTemplate;
    }

    public void setSelectOutgoingBatchRangeSql(String selectOutgoingBatchRangeSql) {
        this.selectOutgoingBatchRangeSql = selectOutgoingBatchRangeSql;
    }

    public void setSelectOutgoingBatchErrorsSql(String selectOutgoingBatchErrorsSql) {
        this.selectOutgoingBatchErrorsSql = selectOutgoingBatchErrorsSql;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setBatchSizePeekAhead(int batchSizePeekAhead) {
        this.batchSizePeekAhead = batchSizePeekAhead;
    }

    public void setNodeService(INodeService nodeService)
    {
        this.nodeService = nodeService;
    }

}
