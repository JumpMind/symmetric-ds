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

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.statistic.IStatisticManager;

/**
 * @see IPurgeService
 */
public class PurgeService extends AbstractService implements IPurgeService {

    enum MinMaxDeleteSql {
        DATA, DATA_EVENT, OUTGOING_BATCH, STRANDED_DATA
    };

    private IClusterService clusterService;

    private IStatisticManager statisticManager;

    public PurgeService(IParameterService parameterService, ISymmetricDialect symmetricDialect,
            IClusterService clusterService, IStatisticManager statisticManager) {
        super(parameterService, symmetricDialect);
        this.clusterService = clusterService;
        this.statisticManager = statisticManager;
        setSqlMap(new PurgeServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    public long purgeOutgoing() {
        long rowsPurged = 0;
        Calendar retentionCutoff = Calendar.getInstance();
        retentionCutoff.add(Calendar.MINUTE,
                -parameterService.getInt(ParameterConstants.PURGE_RETENTION_MINUTES));
        rowsPurged += purgeOutgoing(retentionCutoff);
        return rowsPurged;
    }

    public long purgeIncoming() {
        long rowsPurged = 0;
        Calendar retentionCutoff = Calendar.getInstance();
        retentionCutoff.add(Calendar.MINUTE,
                -parameterService.getInt(ParameterConstants.PURGE_RETENTION_MINUTES));
        rowsPurged += purgeIncoming(retentionCutoff);
        return rowsPurged;
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
                    log.info("The data gap purge process is about to run");
                    rowsPurged = sqlTemplate.update(getSql("deleteFromDataGapsSql"),
                            new Object[] { retentionCutoff.getTime(), DataGap.Status.GP.name() });
                    log.info("Purged {} data gap rows", rowsPurged);
                } finally {
                    clusterService.unlock(ClusterConstants.PURGE_DATA_GAPS);
                    log.info("The data gap purge process has completed");
                }

            } else {
                log.warn("Did not run the data gap purge process because the cluster service has it locked");
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
        return rowsPurged;
    }

    public long purgeOutgoing(Calendar retentionCutoff) {
        long rowsPurged = 0;
        try {
            if (clusterService.lock(ClusterConstants.PURGE_OUTGOING)) {
                try {
                    log.info("The outgoing purge process is about to run for data older than {}",
                            SimpleDateFormat.getDateTimeInstance()
                                    .format(retentionCutoff.getTime()));
                    rowsPurged += purgeStrandedBatches();
                    rowsPurged += purgeDataRows(retentionCutoff);
                    rowsPurged += purgeOutgoingBatch(retentionCutoff);
                } finally {
                    clusterService.unlock(ClusterConstants.PURGE_OUTGOING);
                    log.info("The outgoing purge process has completed");
                }
            } else {
                log.info("Could not get a lock to run an outgoing purge");
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
        return rowsPurged;
    }

    private long purgeOutgoingBatch(final Calendar time) {
        log.info("Getting range for outgoing batch");
        long[] minMax = queryForMinMax(getSql("selectOutgoingBatchRangeSql"),
                new Object[] { time.getTime(), OutgoingBatch.Status.OK.name() });
        int maxNumOfBatchIdsToPurgeInTx = parameterService
                .getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_BATCH_IDS);
        int maxNumOfDataEventsToPurgeInTx = parameterService
                .getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_EVENT_BATCH_IDS);
        int dataEventsPurgedCount = purgeByMinMax(minMax, MinMaxDeleteSql.DATA_EVENT,
                time.getTime(), maxNumOfDataEventsToPurgeInTx);
        statisticManager.incrementPurgedDataEventRows(dataEventsPurgedCount);
        int outgoingbatchPurgedCount = purgeByMinMax(minMax, MinMaxDeleteSql.OUTGOING_BATCH,
                time.getTime(), maxNumOfBatchIdsToPurgeInTx);
        statisticManager.incrementPurgedBatchOutgoingRows(outgoingbatchPurgedCount);
        return dataEventsPurgedCount + outgoingbatchPurgedCount;
    }

    private long purgeStrandedBatches() {
        int updateStrandedBatchesCount = sqlTemplate.update(getSql("updateStrandedBatches"),
                OutgoingBatch.Status.OK.name(), 1, OutgoingBatch.Status.OK.name());
        if (updateStrandedBatchesCount > 0) {
            log.info(
                    "Set the status to {} for {} batches that no longer are associated with valid nodes",
                    OutgoingBatch.Status.OK.name(), updateStrandedBatchesCount);
            statisticManager.incrementPurgedBatchOutgoingRows(updateStrandedBatchesCount);
        }
        return updateStrandedBatchesCount;
    }

    private long purgeDataRows(final Calendar time) {
        log.info("Getting range for data");
        long[] minMax = queryForMinMax(getSql("selectDataRangeSql"), new Object[0]);
        int maxNumOfDataIdsToPurgeInTx = parameterService
                .getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_DATA_IDS);
        int dataDeletedCount = purgeByMinMax(minMax, MinMaxDeleteSql.DATA, time.getTime(),
                maxNumOfDataIdsToPurgeInTx);
        statisticManager.incrementPurgedDataRows(dataDeletedCount);
        int strandedDeletedCount = purgeByMinMax(minMax, MinMaxDeleteSql.STRANDED_DATA,
                time.getTime(), maxNumOfDataIdsToPurgeInTx);
        statisticManager.incrementPurgedDataRows(strandedDeletedCount);
        return dataDeletedCount + strandedDeletedCount;

    }

    private long[] queryForMinMax(String sql, Object... params) {
        long[] minMax = sqlTemplate.queryForObject(sql, new ISqlRowMapper<long[]>() {
            public long[] mapRow(Row rs) {
                return new long[] { rs.getLong("min_id"), rs.getLong("max_id") };
            }
        }, params);
        return minMax;
    }

    private int purgeByMinMax(long[] minMax, MinMaxDeleteSql identifier, Date retentionTime,
            int maxNumtoPurgeinTx) {
        long minId = minMax[0];
        long purgeUpToId = minMax[1];
        long ts = System.currentTimeMillis();
        int totalCount = 0;
        int totalDeleteStmts = 0;
        Timestamp cutoffTime = new Timestamp(retentionTime.getTime());
        log.info("About to purge {}", identifier.toString().toLowerCase());
        while (minId <= purgeUpToId) {
            totalDeleteStmts++;
            long maxId = minId + maxNumtoPurgeinTx;
            if (maxId > purgeUpToId) {
                maxId = purgeUpToId;
            }

            String deleteSql = null;
            Object[] args = null;

            switch (identifier) {
                case DATA:
                    deleteSql = getSql("deleteDataSql");
                    args = new Object[] { minId, maxId, cutoffTime, minId, maxId, minId, maxId, OutgoingBatch.Status.OK.name() };
                    break;
                case DATA_EVENT:
                    deleteSql = getSql("deleteDataEventSql");
                    args = new Object[] { minId, maxId, OutgoingBatch.Status.OK.name(), minId,
                            maxId };
                    break;
                case OUTGOING_BATCH:
                    deleteSql = getSql("deleteOutgoingBatchSql");
                    args = new Object[] { OutgoingBatch.Status.OK.name(), minId, maxId, minId,
                            maxId };
                    break;
                case STRANDED_DATA:
                    deleteSql = getSql("deleteStrandedData");
                    args = new Object[] { minId, maxId, cutoffTime, minId, maxId };
                    break;
            }

            totalCount += sqlTemplate.update(deleteSql, args);

            if (totalCount > 0
                    && (System.currentTimeMillis() - ts > DateUtils.MILLIS_PER_MINUTE * 5)) {
                log.info("Purged {} of {} rows so far using {} statements", new Object[] {
                        totalCount, identifier.toString().toLowerCase(), totalDeleteStmts });
                ts = System.currentTimeMillis();
            }
            minId = maxId + 1;
        }
        log.info("Done purging {} of {} rows", totalCount, identifier.toString().toLowerCase());
        return totalCount;
    }

    public long purgeIncoming(Calendar retentionCutoff) {
        long purgedRowCount = 0;
        try {
            if (clusterService.lock(ClusterConstants.PURGE_INCOMING)) {
                try {
                    log.info("The incoming purge process is about to run");
                    purgedRowCount = purgeIncomingBatch(retentionCutoff);
                } finally {
                    clusterService.unlock(ClusterConstants.PURGE_INCOMING);
                    log.info("The incoming purge process has completed");
                }
            } else {
                log.info("Could not get a lock to run an incoming purge");
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
        return purgedRowCount;
    }

    private long purgeIncomingBatch(final Calendar time) {
        log.info("Getting range for incoming batch");
        List<NodeBatchRange> nodeBatchRangeList = sqlTemplate.query(
                getSql("selectIncomingBatchRangeSql"), new ISqlRowMapper<NodeBatchRange>() {
                    public NodeBatchRange mapRow(Row rs) {
                        return new NodeBatchRange(rs.getString("node_id"), rs.getLong("min_id"), rs
                                .getLong("max_id"));
                    }
                }, time.getTime(), IncomingBatch.Status.OK.name());
        int incomingBatchesPurgedCount = purgeByNodeBatchRangeList(nodeBatchRangeList);
        statisticManager.incrementPurgedBatchIncomingRows(incomingBatchesPurgedCount);
        return incomingBatchesPurgedCount;
    }

    private int purgeByNodeBatchRangeList(List<NodeBatchRange> nodeBatchRangeList) {
        long ts = System.currentTimeMillis();
        int totalCount = 0;
        int totalDeleteStmts = 0;
        log.info("About to purge incoming batch");

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
                totalCount += sqlTemplate.update(getSql("deleteIncomingBatchSql"), new Object[] { minBatchId, maxBatchId,
                        nodeBatchRange.getNodeId(), IncomingBatch.Status.OK.name() });
                minBatchId = maxBatchId + 1;
            }

            if (totalCount > 0
                    && (System.currentTimeMillis() - ts > DateUtils.MILLIS_PER_MINUTE * 5)) {
                log.info("Purged {} incoming batch rows so far using {} statements", new Object[] {
                        totalCount, totalDeleteStmts });
                ts = System.currentTimeMillis();
            }
        }
        log.info("Done purging {} incoming batch rows", totalCount);
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
        int count = sqlTemplate.update(getSql("deleteIncomingBatchByNodeSql"),
                new Object[] { nodeId });
        log.info("Purged all {} incoming batch for node {}", count, nodeId);
    }

}
