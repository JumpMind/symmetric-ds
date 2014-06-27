/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */

package org.jumpmind.symmetric.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

/**
 * @see IPurgeService
 */
public class PurgeService extends AbstractService implements IPurgeService {

    private static final String PARAM_MAX = "MAX";

    private static final String PARAM_MIN = "MIN";

    private static final String PARAM_CUTOFF_TIME = "CUTOFF_TIME";

    private IClusterService clusterService;

    private IStatisticManager statisticManager;

    public long purgeOutgoing() {
        Calendar retentionCutoff = Calendar.getInstance();
        retentionCutoff.add(Calendar.MINUTE,
                -parameterService.getInt(ParameterConstants.PURGE_RETENTION_MINUTES));
        return purgeOutgoing(retentionCutoff);
    }

    public long purgeIncoming() {
        Calendar retentionCutoff = Calendar.getInstance();
        retentionCutoff.add(Calendar.MINUTE,
                -parameterService.getInt(ParameterConstants.PURGE_RETENTION_MINUTES));
        return purgeIncoming(retentionCutoff);
    }

    public long purgeDataGaps() {
        long rowsPurged = 0;
        Calendar retentionCutoff = Calendar.getInstance();
        retentionCutoff.add(Calendar.MINUTE, -parameterService
                .getInt(ParameterConstants.ROUTING_DATA_READER_TYPE_GAP_RETENTION_MINUTES));
        rowsPurged += purgeDataGaps(retentionCutoff);
        return rowsPurged;
    }

    public long purgeDataGaps(Calendar retentionCutoff) {
        long rowsPurged = -1l;
        try {
            if (clusterService.lock(ClusterConstants.PURGE_DATA_GAPS)) {
                try {
                    log.info("DataPurgeDataGapsRunning");
                    rowsPurged = jdbcTemplate.update(getSql("deleteFromDataGapsSql"),
                            new Object[] { retentionCutoff.getTime() });
                    log.info("DataPurgeDataGapsRun", rowsPurged);
                } finally {
                    clusterService.unlock(ClusterConstants.PURGE_DATA_GAPS);
                    log.info("DataPurgeDataGapsCompleted");
                }

            } else {
                log.warn("DataPurgeDataGapsRunningFailedLock");
            }
        } catch (Exception ex) {
            log.error(ex);
        }
        return rowsPurged;
    }

    public long purgeOutgoing(Calendar retentionCutoff) {
        long rowsPurged = 0;
        try {
            if (clusterService.lock(ClusterConstants.PURGE_OUTGOING)) {
                try {
                    log.info("DataPurgeOutgoingRunning", SimpleDateFormat.getDateTimeInstance()
                            .format(retentionCutoff.getTime()));
                    rowsPurged += purgeStrandedBatches();
                    rowsPurged += purgeDataRows(retentionCutoff);
                    rowsPurged += purgeOutgoingBatch(retentionCutoff);
                } finally {
                    clusterService.unlock(ClusterConstants.PURGE_OUTGOING);
                    log.info("DataPurgeOutgoingCompleted");
                }
            } else {
                log.info("DataPurgeOutgoingRunningFailedLock");
            }
        } catch (Exception ex) {
            log.error(ex);
        }
        return rowsPurged;
    }

    private long purgeOutgoingBatch(final Calendar time) {
        log.info("DataPurgeOutgoingRange");
        long[] minMax = queryForMinMax(getSql("selectOutgoingBatchRangeSql"),
                new Object[] { time.getTime() });
        int maxNumOfBatchIdsToPurgeInTx = parameterService
                .getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_BATCH_IDS);
        int maxNumOfDataEventsToPurgeInTx = parameterService
                .getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_EVENT_BATCH_IDS);
        int dataEventsPurgedCount = purgeByMinMax(minMax, getSql("deleteDataEventSql"),
                time.getTime(), maxNumOfDataEventsToPurgeInTx);
        statisticManager.incrementPurgedDataEventRows(dataEventsPurgedCount);
        int outgoingbatchPurgedCount = purgeByMinMax(minMax, getSql("deleteOutgoingBatchSql"),
                time.getTime(), maxNumOfBatchIdsToPurgeInTx);
        statisticManager.incrementPurgedBatchOutgoingRows(outgoingbatchPurgedCount);
        long unroutedPurgedCount = purgeUnroutedDataEvents(time.getTime());
        return dataEventsPurgedCount + outgoingbatchPurgedCount + unroutedPurgedCount;
    }

    private long purgeStrandedBatches() {
        int updateStrandedBatchesCount = getSimpleTemplate()
                .update(getSql("updateStrandedBatches"));
        if (updateStrandedBatchesCount > 0) {
            log.info("DataPurgeUpdatedStrandedBatches", updateStrandedBatchesCount);
            statisticManager.incrementPurgedBatchOutgoingRows(updateStrandedBatchesCount);
        }
        return updateStrandedBatchesCount;
    }

    private long purgeUnroutedDataEvents(Date time) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put(PARAM_CUTOFF_TIME, new Timestamp(time.getTime()));
        int unroutedDataEventCount = getSimpleTemplate().update(
                getSql("deleteUnroutedDataEventSql"), params);
        if (unroutedDataEventCount > 0) {
            statisticManager.incrementPurgedDataEventRows(unroutedDataEventCount);
            log.info("DataPurgeTableCompleted", unroutedDataEventCount, "unrouted data_event");
        }
        return unroutedDataEventCount;
    }

    private long purgeDataRows(final Calendar time) {
        log.info("DataPurgeRowsRange");
        long[] minMax = queryForMinMax(getSql("selectDataRangeSql"), new Object[0]);
        int maxNumOfDataIdsToPurgeInTx = parameterService
                .getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_DATA_IDS);
        int dataDeletedCount = purgeByMinMax(minMax, getSql("deleteDataSql"), time.getTime(),
                maxNumOfDataIdsToPurgeInTx);
        statisticManager.incrementPurgedDataRows(dataDeletedCount);
        int strandedDeletedCount = purgeByMinMax(minMax, getSql("deleteStrandedData"),
                time.getTime(), maxNumOfDataIdsToPurgeInTx);
        statisticManager.incrementPurgedDataRows(strandedDeletedCount);
        return dataDeletedCount + strandedDeletedCount;

    }

    private long[] queryForMinMax(String sql, Object[] params) {
        long[] minMax = (long[]) jdbcTemplate.queryForObject(sql, params, new RowMapper<long[]>() {
            public long[] mapRow(ResultSet rs, int row) throws SQLException {
                return new long[] { rs.getLong(1), rs.getLong(2) };
            }
        });
        return minMax;
    }

    private int purgeByMinMax(long[] minMax, String deleteSql, Date retentionTime,
            int maxNumtoPurgeinTx) {
        long minId = minMax[0];
        long purgeUpToId = minMax[1];
        long ts = System.currentTimeMillis();
        int totalCount = 0;
        int totalDeleteStmts = 0;
        String tableName = deleteSql.trim().split("\\s")[2];
        log.info("DataPurgeTableStarting", tableName);

        MapSqlParameterSource parameterSource = new MapSqlParameterSource();
        parameterSource.registerSqlType(PARAM_CUTOFF_TIME, Types.TIMESTAMP);
        parameterSource.registerSqlType(PARAM_MIN, Types.NUMERIC);
        parameterSource.registerSqlType(PARAM_MAX, Types.NUMERIC);
        parameterSource.addValue(PARAM_CUTOFF_TIME, new Timestamp(retentionTime.getTime()));

        while (minId <= purgeUpToId) {
            totalDeleteStmts++;
            long maxId = minId + maxNumtoPurgeinTx;
            if (maxId > purgeUpToId) {
                maxId = purgeUpToId;
            }

            parameterSource.addValue(PARAM_MIN, minId);
            parameterSource.addValue(PARAM_MAX, maxId);

            totalCount += getSimpleTemplate().update(deleteSql, parameterSource);

            if (totalCount > 0
                    && (System.currentTimeMillis() - ts > DateUtils.MILLIS_PER_MINUTE * 5)) {
                log.info("DataPurgeTableRunning", totalCount, tableName, totalDeleteStmts);
                ts = System.currentTimeMillis();
            }
            minId = maxId + 1;
        }
        log.info("DataPurgeTableCompleted", totalCount, tableName);
        return totalCount;
    }

    public long purgeIncoming(Calendar retentionCutoff) {
        long purgedRowCount = 0;
        try {
            if (clusterService.lock(ClusterConstants.PURGE_INCOMING)) {
                try {
                    log.info("DataPurgeIncomingRunning");
                    purgedRowCount = purgeIncomingBatch(retentionCutoff);
                } finally {
                    clusterService.unlock(ClusterConstants.PURGE_INCOMING);
                    log.info("DataPurgeIncomingCompleted");
                }
            } else {
                log.info("DataPurgeIncomingRunningFailed");
            }
        } catch (Exception ex) {
            log.error(ex);
        }
        return purgedRowCount;
    }

    private long purgeIncomingBatch(final Calendar time) {
        log.info("DataPurgeIncomingRange");
        List<NodeBatchRange> nodeBatchRangeList = jdbcTemplate.query(
                getSql("selectIncomingBatchRangeSql"), new Object[] { time.getTime() },
                new RowMapper<NodeBatchRange>() {
                    public NodeBatchRange mapRow(ResultSet rs, int rowNum) throws SQLException {
                        return new NodeBatchRange(rs.getString(1), rs.getLong(2), rs.getLong(3));
                    }
                });
        int incomingBatchesPurgedCount = purgeByNodeBatchRangeList(
                getSql("deleteIncomingBatchSql"), nodeBatchRangeList);
        statisticManager.incrementPurgedBatchIncomingRows(incomingBatchesPurgedCount);
        return incomingBatchesPurgedCount;
    }

    private int purgeByNodeBatchRangeList(String deleteSql, List<NodeBatchRange> nodeBatchRangeList) {
        long ts = System.currentTimeMillis();
        int totalCount = 0;
        int totalDeleteStmts = 0;
        String tableName = deleteSql.trim().split("\\s")[2];
        log.info("DataPurgeTableStarting", tableName);

        for (NodeBatchRange nodeBatchRange : nodeBatchRangeList) {
            int maxNumOfDataIdsToPurgeInTx = parameterService
                    .getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_BATCH_IDS);
            long minBatchId = nodeBatchRange.getMinBatchId();
            long purgeUpToBatchId = nodeBatchRange.getMaxBatchId();

            while (minBatchId <= purgeUpToBatchId) {
                totalDeleteStmts++;
                long maxBatchId = minBatchId + maxNumOfDataIdsToPurgeInTx;
                if (maxBatchId > purgeUpToBatchId) {
                    maxBatchId = purgeUpToBatchId;
                }
                totalCount += jdbcTemplate.update(deleteSql, new Object[] { minBatchId, maxBatchId,
                        nodeBatchRange.getNodeId() });
                minBatchId = maxBatchId + 1;
            }

            if (totalCount > 0
                    && (System.currentTimeMillis() - ts > DateUtils.MILLIS_PER_MINUTE * 5)) {
                log.info("DataPurgeTableRunning", totalCount, tableName, totalDeleteStmts);
                ts = System.currentTimeMillis();
            }
        }
        log.info("DataPurgeTableCompleted", totalCount, tableName);
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
        int count = jdbcTemplate.update(getSql("deleteIncomingBatchByNodeSql"),
                new Object[] { nodeId });
        log.info("DataPurgeIncomingAllCompleted", count, nodeId);
    }

    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }

}