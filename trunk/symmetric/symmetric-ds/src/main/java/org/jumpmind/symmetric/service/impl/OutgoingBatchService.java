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
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.util.AppUtils;
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
    public void markAllAsSentForNode(Node node) {
        OutgoingBatches batches = null;
        do {
            batches = getOutgoingBatches(node);
            for (OutgoingBatch outgoingBatch : batches.getBatches()) {
                outgoingBatch.setStatus(Status.OK);
                updateOutgoingBatch(outgoingBatch);
            }
        } while (batches.getBatches().size() > 0);
    }

    public void updateOutgoingBatch(OutgoingBatch outgoingBatch) {
        updateOutgoingBatch(jdbcTemplate, outgoingBatch);
    }

    public void updateOutgoingBatches(List<OutgoingBatch> outgoingBatches) {
        for (OutgoingBatch batch : outgoingBatches) {
            updateOutgoingBatch(jdbcTemplate, batch);
        }
    }

    public void updateOutgoingBatch(JdbcTemplate template, OutgoingBatch outgoingBatch) {
        outgoingBatch.setLastUpdatedTime(new Date());
        outgoingBatch.setLastUpdatedHostName(AppUtils.getServerId());
        template.update(getSql("updateOutgoingBatchSql"), new Object[] { outgoingBatch.getStatus().name(), outgoingBatch.isLoadFlag() ? 1 : 0,
                outgoingBatch.getByteCount(), outgoingBatch.getSentCount(), outgoingBatch.getDataEventCount(),
                outgoingBatch.getReloadEventCount(), outgoingBatch.getInsertEventCount(), outgoingBatch.getUpdateEventCount(),
                outgoingBatch.getDeleteEventCount(), outgoingBatch.getOtherEventCount(),
                outgoingBatch.getRouterMillis(), outgoingBatch.getNetworkMillis(), outgoingBatch.getFilterMillis(),
                outgoingBatch.getLoadMillis(), outgoingBatch.getExtractMillis(), outgoingBatch.getSqlState(),
                outgoingBatch.getSqlCode(), StringUtils.abbreviate(outgoingBatch.getSqlMessage(), 1000),
                outgoingBatch.getFailedDataId(), outgoingBatch.getLastUpdatedHostName(),
                outgoingBatch.getLastUpdatedTime(), outgoingBatch.getBatchId() }, new int[] { Types.CHAR, Types.INTEGER,
                Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER,
                Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER,
                Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.INTEGER,
                Types.VARCHAR, Types.TIMESTAMP, Types.INTEGER });
    }

    public void insertOutgoingBatch(OutgoingBatch outgoingBatch) {
        insertOutgoingBatch(jdbcTemplate, outgoingBatch);
    }

    public void insertOutgoingBatch(JdbcTemplate jdbcTemplate, final OutgoingBatch outgoingBatch) {
        outgoingBatch.setLastUpdatedHostName(AppUtils.getServerId());
        long batchId = dbDialect.insertWithGeneratedKey(jdbcTemplate, getSql("insertOutgoingBatchSql"),
                SequenceIdentifier.OUTGOING_BATCH, new PreparedStatementCallback<Object>() {
                    public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                        ps.setString(1, outgoingBatch.getNodeId());
                        ps.setString(2, outgoingBatch.getChannelId());
                        ps.setString(3, outgoingBatch.getStatus().name());
                        ps.setInt(4, outgoingBatch.isLoadFlag() ? 1 : 0);
                        ps.setLong(5, outgoingBatch.getReloadEventCount()); 
                        ps.setLong(6, outgoingBatch.getOtherEventCount());
                        ps.setString(7, outgoingBatch.getLastUpdatedHostName());
                        return null;
                    }
                });
        outgoingBatch.setBatchId(batchId);
    }

    public OutgoingBatch findOutgoingBatch(long batchId) {
        List<OutgoingBatch> list = (List<OutgoingBatch>) jdbcTemplate.query(getSql("findOutgoingBatchSql"),
                new Object[] { batchId }, new int[] { Types.INTEGER }, new OutgoingBatchMapper());
        if (list != null && list.size() > 0) {
            return list.get(0);
        } else {
            return null;
        }
    }
    
    public int countOutgoingBatchesWithStatus(Status status) {
        return jdbcTemplate.queryForInt(getSql("countOutgoingBatchesSql"), status.name());
    }

    /**
     * Select batches to process. Batches that are NOT in error will be returned
     * first. They will be ordered by batch id as the batches will have already
     * been created by {@link #buildOutgoingBatches(String)} in channel priority
     * order.
     */
    public OutgoingBatches getOutgoingBatches(Node node) {
        List<OutgoingBatch> list = (List<OutgoingBatch>) jdbcTemplate.query(getSql("selectOutgoingBatchSql"),
                new Object[] { node.getNodeId(), 
            OutgoingBatch.Status.NE.toString(), 
            OutgoingBatch.Status.QY.toString(),
            OutgoingBatch.Status.SE.toString(),
            OutgoingBatch.Status.LD.toString(),
            OutgoingBatch.Status.ER.toString() }, new OutgoingBatchMapper());


        OutgoingBatches batches = new OutgoingBatches(list);

        List<NodeChannel> channels = configurationService.getNodeChannels(node.getNodeId(), true);
        batches.sortChannels(channels);

        List<OutgoingBatch> keepers = new ArrayList<OutgoingBatch>();

        for (NodeChannel channel : channels) {
            if (parameterService.is(ParameterConstants.DATA_EXTRACTOR_ENABLED) || channel.getChannelId().equals(Constants.CHANNEL_CONFIG)) {
                keepers.addAll(batches
                    .getBatchesForChannelWindows(node, channel, configurationService
                            .getNodeGroupChannelWindows(parameterService.getNodeGroupId(), channel.getChannelId())));
            }
        }
        batches.setBatches(keepers);
        return batches;
    }

    public OutgoingBatches getOutgoingBatchRange(String startBatchId, String endBatchId) {
        OutgoingBatches batches = new OutgoingBatches();
        batches.setBatches(jdbcTemplate.query(getSql("selectOutgoingBatchRangeSql"), new Object[] { startBatchId,
                endBatchId }, new OutgoingBatchMapper()));
        return batches;
    }

    public OutgoingBatches getOutgoingBatchErrors(int maxRows) {
        OutgoingBatches batches = new OutgoingBatches();
        batches.setBatches(jdbcTemplate.query(new MaxRowsStatementCreator(getSql("selectOutgoingBatchErrorsSql"),
                maxRows), new OutgoingBatchMapper()));
        return batches;
    }
    
    public boolean isInitialLoadComplete(String nodeId) {
        return areAllLoadBatchesComplete(nodeId) && !isUnsentDataOnChannelForNode(Constants.CHANNEL_CONFIG, nodeId);
    }

    public boolean areAllLoadBatchesComplete(String nodeId) {

        NodeSecurity security = nodeService.findNodeSecurity(nodeId);
        if (security == null || security.isInitialLoadEnabled()) {
            return false;
        }

        List<String> statuses = (List<String>) jdbcTemplate.queryForList(getSql("initialLoadStatusSql"), new Object[] {
                nodeId, 1 }, String.class);
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
        // int unbatchedCount =
        // jdbcTemplate.queryForInt(getSql("unbatchedCountForNodeIdChannelIdSql"),
        // new Object[] {
        // nodeId, channelId });
        // if (unbatchedCount > 0) {
        // return true;
        // }

        return false;
    }

    class OutgoingBatchMapper implements RowMapper<OutgoingBatch> {
        public OutgoingBatch mapRow(ResultSet rs, int num) throws SQLException {
            OutgoingBatch batch = new OutgoingBatch();
            batch.setNodeId(rs.getString(1));
            batch.setChannelId(rs.getString(2));
            batch.setStatus(rs.getString(3));
            batch.setByteCount(rs.getLong(4));
            batch.setSentCount(rs.getLong(5));
            batch.setDataEventCount(rs.getLong(6));
            batch.setReloadEventCount(rs.getLong(7));
            batch.setInsertEventCount(rs.getLong(8));
            batch.setUpdateEventCount(rs.getLong(9));
            batch.setDeleteEventCount(rs.getLong(10));
            batch.setOtherEventCount(rs.getLong(11));            
            batch.setRouterMillis(rs.getLong(12));
            batch.setNetworkMillis(rs.getLong(13));
            batch.setFilterMillis(rs.getLong(14));
            batch.setLoadMillis(rs.getLong(15));
            batch.setExtractMillis(rs.getLong(16));
            batch.setSqlState(rs.getString(17));
            batch.setSqlCode(rs.getInt(18));
            batch.setSqlMessage(rs.getString(19));
            batch.setFailedDataId(rs.getLong(20));
            batch.setLastUpdatedHostName(rs.getString(21));
            batch.setLastUpdatedTime(rs.getTimestamp(22));
            batch.setCreateTime(rs.getTimestamp(22));
            batch.setBatchId(rs.getLong(24));
            batch.setLoadFlag(rs.getBoolean(25));
            return batch;
        }
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

}
