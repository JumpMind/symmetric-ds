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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.util.MaxRowsStatementCreator;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

public class OutgoingBatchService extends AbstractService implements IOutgoingBatchService {

    private INodeService nodeService;

    private IConfigurationService configurationService;

    @Transactional
    public void markAllAsSentForNode(String nodeId) {
        List<OutgoingBatch> batches = null;
        do {
            batches = getOutgoingBatches(nodeId);
            for (OutgoingBatch outgoingBatch : batches) {
                setBatchStatus(outgoingBatch.getBatchId(), Status.OK);
            }
        } while (batches.size() > 0);
    }

    public void insertOutgoingBatch(final OutgoingBatch outgoingBatch) {
        insertOutgoingBatch(jdbcTemplate, outgoingBatch);
    }

    public void insertOutgoingBatch(JdbcTemplate jdbcTemplate, final OutgoingBatch outgoingBatch) {
        long batchId = dbDialect.insertWithGeneratedKey(jdbcTemplate, getSql("createBatchSql"),
                SequenceIdentifier.OUTGOING_BATCH, new PreparedStatementCallback() {
                    public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
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
     * Select batches to process. Batches that are NOT in error will be returned first. They will be ordered by batch id
     * as the batches will have already been created by {@link #buildOutgoingBatches(String)} in channel priority order.
     */
    @SuppressWarnings("unchecked")
    public List<OutgoingBatch> getOutgoingBatches(String nodeId) {
        List<OutgoingBatch> list = (List<OutgoingBatch>) jdbcTemplate.query(getSql("selectOutgoingBatchSql"),
                new Object[] { nodeId, OutgoingBatch.Status.NE.toString(), OutgoingBatch.Status.SE.toString(),
                        OutgoingBatch.Status.ER.toString() }, new OutgoingBatchMapper());
        final HashSet<String> errorChannels = new HashSet<String>();
        for (OutgoingBatch batch : list) {
            if (batch.getStatus().equals(OutgoingBatch.Status.ER)) {
                errorChannels.add(batch.getChannelId());
            }
        }

        List<NodeChannel> channels = configurationService.getChannels();
        Collections.sort(channels, new Comparator<NodeChannel>() {
            public int compare(NodeChannel b1, NodeChannel b2) {
                boolean isError1 = errorChannels.contains(b1.getId());
                boolean isError2 = errorChannels.contains(b2.getId());
                if (isError1 == isError2) {
                    return b1.getProcessingOrder() < b2.getProcessingOrder() ? -1 : 1;
                } else if (!isError1 && isError2) {
                    return -1;
                } else {
                    return 1;
                }
            }
        });

        return filterMaxNumberOfOutgoingBatchesByChannel(list, channels);
    }

    /**
     * Filter out the maximum number of batches to send.
     */
    private List<OutgoingBatch> filterMaxNumberOfOutgoingBatchesByChannel(List<OutgoingBatch> batches,
            List<NodeChannel> channels) {
        if (batches != null && batches.size() > 0) {
            List<OutgoingBatch> filtered = new ArrayList<OutgoingBatch>(batches.size());
            for (NodeChannel channel : channels) {
                if (channel.isEnabled()) {
                    int max = channel.getMaxBatchToSend();
                    int count = 0;
                    for (OutgoingBatch outgoingBatch : batches) {
                        if (channel.getId().equals(outgoingBatch.getChannelId()) && count < max) {
                            filtered.add(outgoingBatch);
                            count++;
                        }
                    }
                }
            }
            return filtered;
        } else {
            return batches;
        }
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

    public void setBatchStatus(long batchId, Status status) {
        jdbcTemplate.update(getSql("changeBatchStatusSql"), new Object[] { status.name(), batchId });
    }

    @SuppressWarnings("unchecked")
    public boolean isInitialLoadComplete(String nodeId) {

        NodeSecurity security = nodeService.findNodeSecurity(nodeId);
        if (security == null || security.isInitialLoadEnabled()) {
            return false;
        }

        List<String> statuses = (List<String>) jdbcTemplate.queryForList(getSql("initialLoadStatusSql"), new Object[] {
                nodeId, Constants.CHANNEL_RELOAD }, String.class);
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

    public boolean isUnsentDataOnChannelForNode(String channelId, String nodeId) {
        int unsentCount = jdbcTemplate.queryForInt(getSql("unsentBatchesForNodeIdChannelIdSql"), new Object[] { nodeId,
                channelId });
        if (unsentCount > 0) {
            return true;
        }

        // Do we need to check for unbatched data?
        // int unbatchedCount = jdbcTemplate.queryForInt(getSql("unbatchedCountForNodeIdChannelIdSql"), new Object[] {
        // nodeId, channelId });
        // if (unbatchedCount > 0) {
        // return true;
        // }

        return false;
    }

    public void insertOutgoingBatchHistory(OutgoingBatchHistory history) {
        insertOutgoingBatchHistory(jdbcTemplate, history);
    }

    public void insertOutgoingBatchHistory(JdbcTemplate template, OutgoingBatchHistory history) {
        template.update(getSql("insertOutgoingBatchHistorySql"), new Object[] { history.getBatchId(),
                history.getNodeId(), history.getStatus().toString(), history.getNetworkMillis(),
                history.getFilterMillis(), history.getDatabaseMillis(), history.getHostName(), history.getByteCount(),
                history.getDataEventCount(), history.getFailedDataId(), history.getStartTime(), history.getEndTime(),
                history.getSqlState(), history.getSqlCode(), StringUtils.abbreviate(history.getSqlMessage(), 1000) },
                new int[] { Types.INTEGER, Types.VARCHAR, Types.CHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER,
                        Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.TIMESTAMP, Types.TIMESTAMP,
                        Types.VARCHAR, Types.INTEGER, Types.VARCHAR });
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

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

}
