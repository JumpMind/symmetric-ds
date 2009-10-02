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
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.SequenceIdentifier;
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
    public void markAllAsSentForNode(String nodeId) {
        OutgoingBatches batches = null;
        do {
            batches = getOutgoingBatches(nodeId);
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
        template.update(getSql("updateOutgoingBatchSql"), new Object[] { outgoingBatch.getStatus().name(),
                outgoingBatch.getByteCount(), outgoingBatch.getSentCount(), outgoingBatch.getDataEventCount(),
                outgoingBatch.getRouterMillis(), outgoingBatch.getNetworkMillis(), outgoingBatch.getFilterMillis(),
                outgoingBatch.getLoadMillis(), outgoingBatch.getExtractMillis(), outgoingBatch.getSqlState(),
                outgoingBatch.getSqlCode(), StringUtils.abbreviate(outgoingBatch.getSqlMessage(), 1000),
                outgoingBatch.getFailedDataId(), outgoingBatch.getLastUpdatedHostName(),
                outgoingBatch.getLastUpdatedTime(), outgoingBatch.getBatchId() }, new int[] { Types.CHAR,
                Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER,
                Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.INTEGER,
                Types.VARCHAR, Types.TIMESTAMP, Types.INTEGER });
    }

    public void insertOutgoingBatch(OutgoingBatch outgoingBatch) {
        insertOutgoingBatch(jdbcTemplate, outgoingBatch);
    }

    public void insertOutgoingBatch(JdbcTemplate jdbcTemplate, final OutgoingBatch outgoingBatch) {
        outgoingBatch.setLastUpdatedTime(new Date());
        outgoingBatch.setLastUpdatedHostName(AppUtils.getServerId());
        long batchId = dbDialect.insertWithGeneratedKey(jdbcTemplate, getSql("insertOutgoingBatchSql"),
                SequenceIdentifier.OUTGOING_BATCH, new PreparedStatementCallback() {
                    public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                        ps.setString(1, outgoingBatch.getNodeId());
                        ps.setString(2, outgoingBatch.getChannelId());
                        ps.setString(3, outgoingBatch.getStatus().name());
                        ps.setString(4, outgoingBatch.getLastUpdatedHostName());
                        ps.setTimestamp(5, new Timestamp(outgoingBatch.getLastUpdatedTime().getTime()));
                        return null;
                    }
                });
        outgoingBatch.setBatchId(batchId);
    }

    @SuppressWarnings("unchecked")
    public OutgoingBatch findOutgoingBatch(long batchId) {
        List<OutgoingBatch> list = (List<OutgoingBatch>) jdbcTemplate.query(getSql("findOutgoingBatchSql"),
                new Object[] { batchId }, new int[] { Types.INTEGER }, new OutgoingBatchMapper());
        if (list != null && list.size() > 0) {
            return list.get(0);
        } else {
            return null;
        }

    }

    /**
     * Select batches to process. Batches that are NOT in error will be returned
     * first. They will be ordered by batch id as the batches will have already
     * been created by {@link #buildOutgoingBatches(String)} in channel priority
     * order.
     */
    @SuppressWarnings("unchecked")
    public OutgoingBatches getOutgoingBatches(String targetNodeId) {
        List<OutgoingBatch> list = (List<OutgoingBatch>) jdbcTemplate.query(getSql("selectOutgoingBatchSql"),
                new Object[] { targetNodeId, OutgoingBatch.Status.NE.toString(), OutgoingBatch.Status.SE.toString(),
                        OutgoingBatch.Status.ER.toString() }, new OutgoingBatchMapper());
        final HashSet<String> errorChannels = new HashSet<String>();
        for (OutgoingBatch batch : list) {
            if (batch.getStatus().equals(OutgoingBatch.Status.ER)) {
                errorChannels.add(batch.getChannelId());
            }
        }

        List<NodeChannel> channels = configurationService.getNodeChannels();

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

        OutgoingBatches batches = new OutgoingBatches(list);

        for (NodeChannel nodeChannel : channels) {
            long extractPeriodMillis = nodeChannel.getExtractPeriodMillis();
            Date lastExtractedTime = nodeChannel.getLastExtractedTime();

            if ((extractPeriodMillis < 1) || (lastExtractedTime == null)
                    || (Calendar.getInstance().getTimeInMillis() - lastExtractedTime.getTime() >= extractPeriodMillis)) {
                batches.addActiveChannel(nodeChannel);
            }
        }

        batches.filterBatchesForInactiveChannels();

        List<OutgoingBatch> keepers = new ArrayList<OutgoingBatch>();

        for (NodeChannel channel : channels) {
            keepers.addAll(batches
                    .getBatchesForChannelWindows(nodeService.findNode(targetNodeId), channel, configurationService
                            .getNodeGroupChannelWindows(parameterService.getNodeGroupId(), channel.getId())));
        }
        batches.setBatches(keepers);
        return batches;
    }

    @SuppressWarnings("unchecked")
    public OutgoingBatches getOutgoingBatchRange(String startBatchId, String endBatchId) {
        OutgoingBatches batches = new OutgoingBatches();
        batches.setBatches(jdbcTemplate.query(getSql("selectOutgoingBatchRangeSql"), new Object[] { startBatchId,
                endBatchId }, new OutgoingBatchMapper()));
        return batches;
    }

    @SuppressWarnings("unchecked")
    public OutgoingBatches getOutgoingBatchErrors(int maxRows) {
        OutgoingBatches batches = new OutgoingBatches();
        batches.setBatches(jdbcTemplate.query(new MaxRowsStatementCreator(getSql("selectOutgoingBatchErrorsSql"),
                maxRows), new OutgoingBatchMapper()));
        return batches;
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
        // int unbatchedCount =
        // jdbcTemplate.queryForInt(getSql("unbatchedCountForNodeIdChannelIdSql"),
        // new Object[] {
        // nodeId, channelId });
        // if (unbatchedCount > 0) {
        // return true;
        // }

        return false;
    }

    class OutgoingBatchMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int num) throws SQLException {
            OutgoingBatch batch = new OutgoingBatch();
            batch.setNodeId(rs.getString(1));
            batch.setChannelId(rs.getString(2));
            batch.setStatus(rs.getString(3));
            batch.setByteCount(rs.getLong(4));
            batch.setSentCount(rs.getLong(5));
            batch.setDataEventCount(rs.getLong(6));
            batch.setRouterMillis(rs.getLong(7));
            batch.setNetworkMillis(rs.getLong(8));
            batch.setFilterMillis(rs.getLong(9));
            batch.setLoadMillis(rs.getLong(10));
            batch.setExtractMillis(rs.getLong(11));
            batch.setSqlState(rs.getString(12));
            batch.setSqlCode(rs.getInt(13));
            batch.setSqlMessage(rs.getString(14));
            batch.setFailedDataId(rs.getLong(15));
            batch.setLastUpdatedHostName(rs.getString(16));
            batch.setLastUpdatedTime(rs.getTimestamp(17));
            batch.setCreateTime(rs.getTimestamp(18));
            batch.setBatchId(rs.getLong(19));
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
