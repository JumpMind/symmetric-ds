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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.hsqldb.Types;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.NodeStatus;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.LockActionConstants;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

public class PurgeService extends AbstractService implements IPurgeService {

    private static final String PARAM_MAX = "MAX";

    private static final String PARAM_MIN = "MIN";

    private static final String PARAM_CUTOFF_TIME = "CUTOFF_TIME";

    private IClusterService clusterService;

    private INodeService nodeService;

    public void purge() {
        if (nodeService.isRegistrationServer() || nodeService.getNodeStatus() == NodeStatus.DATA_LOAD_COMPLETED) {
            Calendar retentionCutoff = Calendar.getInstance();
            retentionCutoff.add(Calendar.MINUTE, -parameterService.getInt(ParameterConstants.PURGE_RETENTION_MINUTES));
            purgeOutgoing(retentionCutoff);
            purgeIncoming(retentionCutoff);

            retentionCutoff = Calendar.getInstance();
            retentionCutoff.add(Calendar.MINUTE, -parameterService
                    .getInt(ParameterConstants.STATISTIC_RETENTION_MINUTES));
            purgeStatistic(retentionCutoff);
        } else {
            log.warn("DataPurgeSkippingNoInitialLoad");
        }
    }

    private void purgeStatistic(Calendar retentionCutoff) {
        try {
            if (clusterService.lock(LockActionConstants.PURGE_STATISTICS)) {
                try {
                    log.info("DataPurgeStatsRunning");
                    int count = jdbcTemplate.update(getSql("deleteFromStatisticSql"), new Object[] { retentionCutoff
                            .getTime() });
                    log.info("DataPurgeStatsRun", count);
                } finally {
                    clusterService.unlock(LockActionConstants.PURGE_STATISTICS);
                    log.info("DataPurgeStatsCompleted");
                }

            } else {
                log.warn("DataPurgeStatsRunningFailedLock");
            }
        } catch (Exception ex) {
            log.error(ex);
        }
    }

    private void purgeOutgoing(Calendar retentionCutoff) {
        try {
            if (clusterService.lock(LockActionConstants.PURGE_OUTGOING)) {
                try {
                    log.info("DataPurgeOutgoingRunning", SimpleDateFormat.getDateTimeInstance().format(
                            retentionCutoff.getTime()));
                    purgeDataRows(retentionCutoff);
                    purgeOutgoingBatch(retentionCutoff);
                } finally {
                    clusterService.unlock(LockActionConstants.PURGE_OUTGOING);
                    log.info("DataPurgeOutgoingCompleted");
                }
            } else {
                log.info("DataPurgeOutgoingRunningFailedLock");
            }
        } catch (Exception ex) {
            log.error(ex);
        }
    }

    private void purgeOutgoingBatch(final Calendar time) {
        log.info("DataPurgeOutgoingRange");
        long[] minMax = queryForMinMax(getSql("selectOutgoingBatchRangeSql"), new Object[] { time.getTime() });
        int maxNumOfBatchIdsToPurgeInTx = parameterService.getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_BATCH_IDS);
        int maxNumOfDataEventsToPurgeInTx = parameterService
                .getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_EVENT_BATCH_IDS);
        purgeByMinMax(minMax, getSql("deleteDataEventSql"), time.getTime(), maxNumOfDataEventsToPurgeInTx);
        purgeByMinMax(minMax, getSql("deleteOutgoingBatchSql"), time.getTime(), maxNumOfBatchIdsToPurgeInTx);
        purgeUnroutedDataEvents(time.getTime());
    }

    private void purgeUnroutedDataEvents(Date time) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put(PARAM_CUTOFF_TIME, new Timestamp(time.getTime()));
        int unroutedDataEventCount = getSimpleTemplate().update(getSql("deleteUnroutedDataEventSql"), params);
        log.info("DataPurgeTableCompleted", unroutedDataEventCount, "unrouted data_event");
    }

    private void purgeDataRows(final Calendar time) {
        log.info("DataPurgeRowsRange");
        long[] minMax = queryForMinMax(getSql("selectDataRangeSql"), new Object[0]);
        int maxNumOfDataIdsToPurgeInTx = parameterService.getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_DATA_IDS);
        purgeByMinMax(minMax, getSql("deleteDataSql"), time.getTime(), maxNumOfDataIdsToPurgeInTx);
    }

    private long[] queryForMinMax(String sql, Object[] params) {
        long[] minMax = (long[]) jdbcTemplate.queryForObject(sql, params, new RowMapper() {
            public Object mapRow(ResultSet rs, int row) throws SQLException {
                return new long[] { rs.getLong(1), rs.getLong(2) };
            }
        });
        return minMax;
    }

    private void purgeByMinMax(long[] minMax, String deleteSql, Date retentionTime, int maxNumtoPurgeinTx) {
        long minId = minMax[0];
        long purgeUpToId = minMax[1];
        long ts = System.currentTimeMillis();
        int totalCount = 0;
        String tableName = deleteSql.trim().split("\\s")[2];
        log.info("DataPurgeTableStarting", tableName);

        MapSqlParameterSource parameterSource = new MapSqlParameterSource();
        parameterSource.registerSqlType(PARAM_CUTOFF_TIME, Types.TIMESTAMP);
        parameterSource.registerSqlType(PARAM_MIN, Types.INTEGER);
        parameterSource.registerSqlType(PARAM_MAX, Types.INTEGER);
        parameterSource.addValue(PARAM_CUTOFF_TIME, new Timestamp(retentionTime.getTime()));

        while (minId <= purgeUpToId) {
            long maxId = minId + maxNumtoPurgeinTx;
            if (maxId > purgeUpToId) {
                maxId = purgeUpToId;
            }

            parameterSource.addValue(PARAM_MIN, minId);
            parameterSource.addValue(PARAM_MAX, maxId);

            totalCount += getSimpleTemplate().update(deleteSql, parameterSource);

            if (totalCount > 0 && (System.currentTimeMillis() - ts > DateUtils.MILLIS_PER_MINUTE * 5)) {
                log.info("DataPurgeTableRunning", totalCount, tableName);
                ts = System.currentTimeMillis();
            }
            minId = maxId + 1;
        }
        log.info("DataPurgeTableCompleted", totalCount, tableName);
    }

    private void purgeIncoming(Calendar retentionCutoff) {
        try {
            if (clusterService.lock(LockActionConstants.PURGE_INCOMING)) {
                try {
                    log.info("DataPurgeIncomingRunning");
                    purgeIncomingBatch(retentionCutoff);
                } finally {
                    clusterService.unlock(LockActionConstants.PURGE_INCOMING);
                    log.info("DataPurgeIncomingCompleted");
                }
            } else {
                log.info("DataPurgeIncomingRunningFailed");
            }
        } catch (Exception ex) {
            log.error(ex);
        }
    }

    @SuppressWarnings( { "unchecked" })
    private void purgeIncomingBatch(final Calendar time) {
        log.info("DataPurgeIncomingRange");
        List<NodeBatchRange> nodeBatchRangeList = jdbcTemplate.query(getSql("selectIncomingBatchRangeSql"),
                new Object[] { time.getTime() }, new RowMapper() {
                    public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                        return new NodeBatchRange(rs.getString(1), rs.getLong(2), rs.getLong(3));
                    }
                });
        purgeByNodeBatchRangeList(getSql("deleteIncomingBatchSql"), nodeBatchRangeList);
    }

    private void purgeByNodeBatchRangeList(String deleteSql, List<NodeBatchRange> nodeBatchRangeList) {
        long ts = System.currentTimeMillis();
        int totalCount = 0;
        String tableName = deleteSql.trim().split("\\s")[2];
        log.info("DataPurgeTableStarting", tableName);

        for (NodeBatchRange nodeBatchRange : nodeBatchRangeList) {
            totalCount += purgeByNodeBatchRange(deleteSql, nodeBatchRange);
            if (totalCount > 0 && (System.currentTimeMillis() - ts > DateUtils.MILLIS_PER_MINUTE * 5)) {
                log.info("DataPurgeTableRunning", totalCount, tableName);
                ts = System.currentTimeMillis();
            }
        }
        log.info("DataPurgeTableCompleted", totalCount, tableName);
    }

    private int purgeByNodeBatchRange(String deleteSql, NodeBatchRange nodeBatchRange) {
        int maxNumOfDataIdsToPurgeInTx = parameterService.getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_BATCH_IDS);
        long minBatchId = nodeBatchRange.getMinBatchId();
        long purgeUpToBatchId = nodeBatchRange.getMaxBatchId();
        int totalCount = 0;

        while (minBatchId <= purgeUpToBatchId) {
            long maxBatchId = minBatchId + maxNumOfDataIdsToPurgeInTx;
            if (maxBatchId > purgeUpToBatchId) {
                maxBatchId = purgeUpToBatchId;
            }
            totalCount += jdbcTemplate.update(deleteSql, new Object[] { minBatchId, maxBatchId,
                    nodeBatchRange.getNodeId() });
            minBatchId = maxBatchId + 1;
        }

        return totalCount;
    }

    class NodeBatchRange {
        private String nodeId;

        private long minBatchId;

        private long maxBatchId;

        public NodeBatchRange(String nodeId, long minBatchId, long maxBatchId) {
            this.nodeId = nodeId;
            this.minBatchId = minBatchId;
            this.maxBatchId = maxBatchId;
        }

        public String getNodeId() {
            return nodeId;
        }

        public long getMaxBatchId() {
            return maxBatchId;
        }

        public long getMinBatchId() {
            return minBatchId;
        }
    }

    public void purgeAllIncomingEventsForNode(String nodeId) {
        int count = jdbcTemplate.update(getSql("deleteIncomingBatchByNodeSql"), new Object[] { nodeId });
        log.info("DataPurgeIncomingAllCompleted", count, nodeId);
    }

    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }
}
