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
import java.util.Calendar;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.NodeStatus;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.LockActionConstants;
import org.springframework.jdbc.core.RowMapper;

public class PurgeService extends AbstractService implements IPurgeService {

    private IClusterService clusterService;
    
    private INodeService nodeService;   

    public void purge() {
        if (nodeService.isRegistrationServer() || nodeService.getNodeStatus() == NodeStatus.DATA_LOAD_COMPLETED) {
            Calendar retentionCutoff = Calendar.getInstance();
            retentionCutoff.add(Calendar.MINUTE, -parameterService.getInt(ParameterConstants.PURGE_RETENTION_MINUTES));
            purgeOutgoing(retentionCutoff);
            purgeIncoming(retentionCutoff);
            purgeStatistic(retentionCutoff);
        } else {
            logger.warn("No sense running the purge service because an initial load has not been completed.");
        }
    }

    // TODO have a different retention time for stats
    private void purgeStatistic(Calendar retentionCutoff) {
        try {
            if (clusterService.lock(LockActionConstants.PURGE_STATISTICS)) {
                try {
                    logger.info("The statistic purge process is about to run.");
                    int count = jdbcTemplate.update(getSql("deleteFromStatisticSql"),
                            new Object[] { retentionCutoff.getTime() });
                    logger.info("Purged " + count + " statistic rows.");
                } finally {
                    clusterService.unlock(LockActionConstants.PURGE_STATISTICS);
                    logger.info("The statistic purge process has completed.");
                }

            } else {
                logger.warn("Did not run the purge process because the cluster service has it locked.");
            }
        } catch (Exception ex) {
            logger.error(ex, ex);
        }
    }

    private void purgeOutgoing(Calendar retentionCutoff) {
        try {
            if (clusterService.lock(LockActionConstants.PURGE_OUTGOING)) {
                try {
                    logger.info("The outgoing purge process is about to run.");
                    purgeDataRows(retentionCutoff);
                    purgeOutgoingBatch(retentionCutoff);
                } finally {
                    clusterService.unlock(LockActionConstants.PURGE_OUTGOING);
                    logger.info("The outgoing purge process has completed.");
                }
            } else {
                logger.info("Could not get a lock to run an outgoing purge.");
            }
        } catch (Exception ex) {
            logger.error(ex, ex);
        }
    }

    private void purgeOutgoingBatch(final Calendar time) {
        logger.info("Getting range for outgoing batch");
        long[] minMax = queryForMinMax(getSql("selectOutgoingBatchRangeSql"), new Object[] { time.getTime() });
        int maxNumOfBatchIdsToPurgeInTx = parameterService
                .getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_BATCH_IDS);
        int maxNumOfDataEventsToPurgeInTx = parameterService
                .getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_EVENT_BATCH_IDS);
        purgeByMinMax(minMax, getSql("deleteDataEventSql"), true, maxNumOfDataEventsToPurgeInTx);
        purgeByMinMax(minMax, getSql("deleteOutgoingBatchSql"), true, maxNumOfBatchIdsToPurgeInTx);
        purgeByMinMax(minMax, getSql("deleteOutgoingBatchHistSql"), true, maxNumOfBatchIdsToPurgeInTx);
    }

    private void purgeDataRows(final Calendar time) {
        logger.info("Getting range for data");
        long[] minMax = queryForMinMax(getSql("selectDataRangeSql"), new Object[] { time.getTime() });
        int maxNumOfDataIdsToPurgeInTx = parameterService
                .getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_DATA_IDS);
        purgeByMinMax(minMax, getSql("deleteDataSql"), true, maxNumOfDataIdsToPurgeInTx);
    }

    private long[] queryForMinMax(String sql, Object[] params) {
        long[] minMax = (long[]) jdbcTemplate.queryForObject(sql, params, new RowMapper() {
            public Object mapRow(ResultSet rs, int row) throws SQLException {
                return new long[] { rs.getLong(1), rs.getLong(2) };
            }
        });
        return minMax;
    }

    private void purgeByMinMax(long[] minMax, String deleteSql, boolean useRangeTwice, int maxNumtoPurgeinTx) {
        long minId = minMax[0];
        long purgeUpToId = minMax[1];
        long ts = System.currentTimeMillis();
        int totalCount = 0;
        String tableName = deleteSql.trim().split("\\s")[2];
        logger.info("About to purge " + tableName);

        while (minId <= purgeUpToId) {
            long maxId = minId + maxNumtoPurgeinTx;
            if (maxId > purgeUpToId) {
                maxId = purgeUpToId;
            }

            Object[] param = null;
            if (useRangeTwice) {
                param = new Object[] { minId, maxId, minId, maxId };
            } else {
                param = new Object[] { minId, maxId };
            }

            totalCount += jdbcTemplate.update(deleteSql, param);

            if (totalCount > 0 && (System.currentTimeMillis() - ts > DateUtils.MILLIS_PER_MINUTE * 5)) {
                logger.info("Purged " + totalCount + " of " + tableName + " rows so far.");
                ts = System.currentTimeMillis();
            }
            minId = maxId + 1;
        }
        logger.info("Done purging " + totalCount + " of " + tableName + " rows.");
    }

    private void purgeIncoming(Calendar retentionCutoff) {
        try {
            if (clusterService.lock(LockActionConstants.PURGE_INCOMING)) {
                try {
                    logger.info("The incoming purge process is about to run.");

                    purgeIncomingBatch(retentionCutoff);
                } finally {
                    clusterService.unlock(LockActionConstants.PURGE_INCOMING);
                    logger.info("The incoming purge process has completed.");
                }
            } else {
                logger.info("Could not get a lock to run an incoming purge.");
            }
        } catch (Exception ex) {
            logger.error(ex, ex);
        }
    }

    @SuppressWarnings( { "unchecked" })
    private void purgeIncomingBatch(final Calendar time) {
        logger.info("Getting range for incoming batch");
        List<NodeBatchRange> nodeBatchRangeList = jdbcTemplate.query(getSql("selectIncomingBatchRangeSql"),
                new Object[] { time.getTime() }, new RowMapper() {
                    public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                        return new NodeBatchRange(rs.getString(1), rs.getLong(2), rs.getLong(3));
                    }
                });
        purgeByNodeBatchRangeList(getSql("deleteIncomingBatchHistSql"), nodeBatchRangeList);
        purgeByNodeBatchRangeList(getSql("deleteIncomingBatchSql"), nodeBatchRangeList);
    }

    private void purgeByNodeBatchRangeList(String deleteSql, List<NodeBatchRange> nodeBatchRangeList) {
        long ts = System.currentTimeMillis();
        int totalCount = 0;
        String tableName = deleteSql.trim().split("\\s")[2];
        logger.info("About to purge " + tableName);

        for (NodeBatchRange nodeBatchRange : nodeBatchRangeList) {
            totalCount += purgeByNodeBatchRange(deleteSql, nodeBatchRange);
            if (totalCount > 0 && (System.currentTimeMillis() - ts > DateUtils.MILLIS_PER_MINUTE * 5)) {
                logger.info("Purged " + totalCount + " of " + tableName + " rows so far.");
                ts = System.currentTimeMillis();
            }
        }
        logger.info("Done purging " + totalCount + " of " + tableName + " rows.");
    }

    private int purgeByNodeBatchRange(String deleteSql, NodeBatchRange nodeBatchRange) {
        int maxNumOfDataIdsToPurgeInTx = parameterService
                .getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_BATCH_IDS);
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
        logger.info("Purged all " + count + " incoming batch for node " + nodeId);
        count = jdbcTemplate.update(getSql("deleteIncomingBatchHistByNodeSql"), new Object[] { nodeId });
        logger.info("Purged all " + count + " incoming batch hist for node " + nodeId);
    }

    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
    }


    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }
}
