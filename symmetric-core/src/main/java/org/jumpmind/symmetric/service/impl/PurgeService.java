/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
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
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.mapper.StringMapper;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.ext.IPurgeListener;
import org.jumpmind.symmetric.model.ExtractRequest;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.RegistrationRequest;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.statistic.IStatisticManager;

/**
 * @see IPurgeService
 */
public class PurgeService extends AbstractService implements IPurgeService {

    enum MinMaxDeleteSql {
        DATA, DATA_RANGE, DATA_EVENT, DATA_EVENT_RANGE, OUTGOING_BATCH, OUTGOING_BATCH_RANGE, STRANDED_DATA, STRANDED_DATA_EVENT
    };

    private IClusterService clusterService;

    private IStatisticManager statisticManager;

    private IExtensionService extensionService;
    
    public PurgeService(IParameterService parameterService, ISymmetricDialect symmetricDialect,
            IClusterService clusterService, IStatisticManager statisticManager, IExtensionService extensionService) {
        super(parameterService, symmetricDialect);
        this.clusterService = clusterService;
        this.statisticManager = statisticManager;
        this.extensionService = extensionService;
        
        setSqlMap(new PurgeServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    public long purgeOutgoing(boolean force) {
        long rowsPurged = 0;
        Calendar retentionCutoff = Calendar.getInstance();
        retentionCutoff.add(Calendar.MINUTE,
                -parameterService.getInt(ParameterConstants.PURGE_RETENTION_MINUTES));

        try {
            rowsPurged += purgeOutgoing(retentionCutoff, force);
        } catch (Exception ex) {
            try {
                log.info("Failed to execute purge, but will try again,", ex);
                rowsPurged += purgeOutgoing(retentionCutoff, force);
            } catch (Exception e) {
                log.error("Failed to execute purge, so aborting,", ex);    
            }
        } finally {
            log.info("The outgoing purge process has completed");
        }
        
        List<IPurgeListener> purgeListeners = extensionService.getExtensionPointList(IPurgeListener.class);
        for (IPurgeListener purgeListener : purgeListeners) {
            rowsPurged += purgeListener.purgeOutgoing(force);
        }        
        return rowsPurged;
    }

    public long purgeIncoming(boolean force) {
        long rowsPurged = 0;
        Calendar retentionCutoff = Calendar.getInstance();
        retentionCutoff.add(Calendar.MINUTE,
                -parameterService.getInt(ParameterConstants.PURGE_RETENTION_MINUTES));
        rowsPurged += purgeIncoming(retentionCutoff, force);
        
        List<IPurgeListener> purgeListeners = extensionService.getExtensionPointList(IPurgeListener.class);
        for (IPurgeListener purgeListener : purgeListeners) {
            rowsPurged += purgeListener.purgeIncoming(force);
        }
               
        return rowsPurged;
    }

    public long purgeOutgoing(Calendar retentionCutoff, boolean force) {
        long rowsPurged = 0;
        if (force || clusterService.lock(ClusterConstants.PURGE_OUTGOING)) {
            try {
                log.info("The outgoing purge process is about to run for data older than {}",
                        SimpleDateFormat.getDateTimeInstance()
                                .format(retentionCutoff.getTime()));
                // VoltDB doesn't support capture, or subselects.  So we'll just be purging heartbeats
                // by date here.
                if (getSymmetricDialect().getName().equalsIgnoreCase(DatabaseNamesConstants.VOLTDB)) { 
                    rowsPurged += purgeOutgoingByRetentionCutoff(retentionCutoff);
                } else {
                    rowsPurged += purgeStrandedBatches();
                    rowsPurged += purgeDataRows(retentionCutoff);
                    rowsPurged += purgeOutgoingBatch(retentionCutoff);
                    rowsPurged += purgeStranded(retentionCutoff);
                    rowsPurged += purgeExtractRequests();
                    rowsPurged += purgeStrandedChannels();
                }
            } finally {
                if (!force) {
                    clusterService.unlock(ClusterConstants.PURGE_OUTGOING);
                }
            }
        } else {
            log.debug("Could not get a lock to run an outgoing purge");
        }
        return rowsPurged;
    }

    protected long purgeOutgoingByRetentionCutoff(Calendar retentionCutoff) {
        int totalCount = 0;
        totalCount += executePurgeDelete(getSql("deleteOutgoingBatchByCreateTimeSql"), retentionCutoff.getTime());
        totalCount += executePurgeDelete(getSql("deleteDataEventByCreateTimeSql"), retentionCutoff.getTime());
        totalCount += executePurgeDelete(getSql("deleteDataByCreateTimeSql"), retentionCutoff.getTime());
        totalCount += executePurgeDelete(getSql("deleteExtractRequestByCreateTimeSql"), retentionCutoff.getTime());

        log.info("Done purging {} rows", totalCount);        
        return totalCount;
    }

    protected int executePurgeDelete(String deleteSql, Object argument) {
        log.debug("Running the following statement: {} with the following arguments: {}", deleteSql, argument);
        int count = sqlTemplate.update(deleteSql, argument);
        log.debug("Deleted {} rows", count);
        return count;
    }

    private long purgeOutgoingBatch(final Calendar time) {
        log.info("Getting range for outgoing batch");
        long[] minMax = queryForMinMax(getSql("selectOutgoingBatchRangeSql"),
                new Object[] { time.getTime(), OutgoingBatch.Status.OK.name() });
        long minGapStartId = sqlTemplateDirty.queryForLong(getSql("minDataGapStartId"));
        int maxNumOfBatchIdsToPurgeInTx = parameterService
                .getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_BATCH_IDS);
        int maxNumOfDataEventsToPurgeInTx = parameterService
                .getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_EVENT_BATCH_IDS);        
        int dataEventsPurgedCount = 0;
        int outgoingbatchPurgedCount = 0;

        if (parameterService.is(ParameterConstants.PURGE_FIRST_PASS)) {
            log.info("Getting first batch_id for outstanding batches");
            long minBatchId = sqlTemplateDirty.queryForLong(getSql("minOutgoingBatchNotStatusSql"), OutgoingBatch.Status.OK.name());
            long rangeMinMax[] = new long[] { minMax[0], Math.min(minBatchId > 0 ? minBatchId - 1 : minMax[1], minMax[1]) };
            if (rangeMinMax[1] == minMax[1]) {
                minMax[1] = -1;
            } else {
                minMax[0] = minBatchId + 1;
            }
            dataEventsPurgedCount = purgeByMinMax(rangeMinMax, minGapStartId, MinMaxDeleteSql.DATA_EVENT_RANGE,
                    time.getTime(), maxNumOfDataEventsToPurgeInTx);
            outgoingbatchPurgedCount = purgeByMinMax(rangeMinMax, minGapStartId, MinMaxDeleteSql.OUTGOING_BATCH_RANGE,
                    time.getTime(), maxNumOfBatchIdsToPurgeInTx);
        }

        dataEventsPurgedCount += purgeByMinMax(minMax, minGapStartId, MinMaxDeleteSql.DATA_EVENT,
                time.getTime(), maxNumOfDataEventsToPurgeInTx);
        statisticManager.incrementPurgedDataEventRows(dataEventsPurgedCount);

        outgoingbatchPurgedCount += purgeByMinMax(minMax, minGapStartId, MinMaxDeleteSql.OUTGOING_BATCH,
                time.getTime(), maxNumOfBatchIdsToPurgeInTx);
        statisticManager.incrementPurgedBatchOutgoingRows(outgoingbatchPurgedCount);
        
        return dataEventsPurgedCount + outgoingbatchPurgedCount;
    }

    private long purgeStrandedBatches() {
        int totalRowsPurged = 0;
        log.info("Looking for old nodes in batches");
        List<String> nodes = sqlTemplateDirty.query(getSql("selectNodesWithStrandedBatches"), 
                new StringMapper(), 1, OutgoingBatch.Status.OK.name());
        if (nodes.size() > 0) {
            log.info("Found {} old nodes in batches", nodes.size());
            for (String nodeId : nodes) {
                int rowsPurged = sqlTemplate.update(getSql("updateStrandedBatches"),
                        OutgoingBatch.Status.OK.name(), nodeId, OutgoingBatch.Status.OK.name());
                log.info("Set the status to {} for {} batches associated with node ID {}",
                        OutgoingBatch.Status.OK.name(), rowsPurged, nodeId);
                totalRowsPurged += rowsPurged;
                statisticManager.incrementPurgedBatchOutgoingRows(rowsPurged);
            }
        }

        log.info("Looking for old channels in batches");
        List<String> channels = sqlTemplateDirty.query(getSql("selectChannelsWithStrandedBatches"), 
                new StringMapper(), OutgoingBatch.Status.OK.name());
        if (channels.size() > 0) {
            log.info("Found {} old channels in batches", channels.size());
            for (String channelId : channels) {
                int rowsPurged = sqlTemplate.update(getSql("updateStrandedBatchesByChannel"),
                        OutgoingBatch.Status.OK.name(), channelId, OutgoingBatch.Status.OK.name());
                log.info("Set the status to {} for {} batches associated with channel ID {}",
                        OutgoingBatch.Status.OK.name(), rowsPurged, channelId);
                totalRowsPurged += rowsPurged;
                statisticManager.incrementPurgedBatchOutgoingRows(rowsPurged);
            }
        }
        
        return totalRowsPurged;
    }

    private long purgeStrandedChannels() {
        int rowsPurged = 0;
        log.info("Looking for old channels in data");
        List<String> channels = sqlTemplateDirty.query(getSql("selectOldChannelsForData"), new StringMapper());
        if (channels.size() > 0) {
            log.info("Found {} old channels", channels.size());
            for (String channelId : channels) {
                log.info("Purging data for channel {}", channelId);
                rowsPurged += sqlTemplate.update(getSql("deleteDataByChannel"), channelId);
            }
            statisticManager.incrementPurgedDataRows(rowsPurged);
            log.info("Done purging {} rows", rowsPurged);
        }
        return rowsPurged;
    }

    private long purgeDataRows(final Calendar time) {
        log.info("Getting range for data");
        long[] minMax = queryForMinMax(getSql("selectDataRangeSql"), new Object[0]);
        long minGapStartId = sqlTemplateDirty.queryForLong(getSql("minDataGapStartId"));
        int maxNumOfDataIdsToPurgeInTx = parameterService.getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_DATA_IDS);
        long dataDeletedCount = 0;

        if (parameterService.is(ParameterConstants.PURGE_FIRST_PASS)) {
            log.info("Getting count of outstanding batches");
            long outstandingCount = sqlTemplateDirty.queryForLong(getSql("countOutgoingBatchNotStatusSql"), 
                    OutgoingBatch.Status.OK.name());
            long maxOutstandingCount = parameterService.getLong(ParameterConstants.PURGE_FIRST_PASS_OUTSTANDING_BATCHES_THRESHOLD);
            log.info("Found "+ outstandingCount + " outstanding batches, threshold is " + maxOutstandingCount);
            
            if (outstandingCount <= maxOutstandingCount) {
                long minDataId = 0;
                if (outstandingCount > 0) {
                    log.info("Getting first data_id for outstanding batches");
                    minDataId = sqlTemplateDirty.queryForLong(getSql("selectDataEventMinNotStatusSql"), 
                            OutgoingBatch.Status.OK.name());
                }
                long rangeMinMax[] = new long[] { minMax[0], Math.min(Math.min(minDataId > 0 ? minDataId - 1 : minMax[1], 
                        minMax[1]), minGapStartId - 1) };
                if (rangeMinMax[1] == minMax[1]) {
                    minMax[1] = -1;
                } else if (rangeMinMax[1] == minDataId - 1) {
                    minMax[0] = minDataId + 1;
                } else if (rangeMinMax[1] == minGapStartId - 1) {
                    minMax[0] = minGapStartId + 1;
                }
                dataDeletedCount = purgeByMinMax(rangeMinMax, minGapStartId, MinMaxDeleteSql.DATA_RANGE,
                        time.getTime(), maxNumOfDataIdsToPurgeInTx);
            }
        }

        dataDeletedCount += purgeByMinMax(minMax, minGapStartId, MinMaxDeleteSql.DATA, time.getTime(),
                maxNumOfDataIdsToPurgeInTx);
        statisticManager.incrementPurgedDataRows(dataDeletedCount);

        return dataDeletedCount;
    }
    
    private long purgeStranded(final Calendar time) {        
        log.info("Getting range for stranded data events");
        int maxNumOfDataEventsToPurgeInTx = parameterService
                .getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_EVENT_BATCH_IDS);
        long minGapStartId = sqlTemplateDirty.queryForLong(getSql("minDataGapStartId"));
        long[] minMaxEvent = queryForMinMax(getSql("selectStrandedDataEventRangeSql"), new Object[] { time.getTime() });
        int strandedEventDeletedCount = purgeByMinMax(minMaxEvent, minGapStartId, MinMaxDeleteSql.STRANDED_DATA_EVENT,
                time.getTime(), maxNumOfDataEventsToPurgeInTx);
        statisticManager.incrementPurgedDataEventRows(strandedEventDeletedCount);
        
        log.info("Getting range for stranded data");
        int maxNumOfDataIdsToPurgeInTx = parameterService
                .getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_DATA_IDS);
        long[] minMax = queryForMinMax(getSql("selectDataRangeSql"), new Object[0]);
        int strandedDeletedCount = purgeByMinMax(minMax, minGapStartId, MinMaxDeleteSql.STRANDED_DATA,
                time.getTime(), maxNumOfDataIdsToPurgeInTx);
        statisticManager.incrementPurgedDataRows(strandedDeletedCount);
        return strandedEventDeletedCount + strandedDeletedCount;
    }

    private long[] queryForMinMax(String sql, Object... params) {
        long[] minMax = sqlTemplateDirty.queryForObject(sql, new ISqlRowMapper<long[]>() {
            public long[] mapRow(Row rs) {
                // Max - 1 so we always leave 1 row behind, which keeps MySQL autoinc from resetting
                return new long[] { rs.getLong("min_id"), rs.getLong("max_id")-1 };
            }
        }, params);
        return minMax;
    }
    
    private long purgeExtractRequests() {
        Calendar retentionCutoff = Calendar.getInstance();
        retentionCutoff.add(Calendar.MINUTE, -parameterService
                .getInt(ParameterConstants.PURGE_EXTRACT_REQUESTS_RETENTION_MINUTES));
        log.info("Purging extract requests that are older than {}", retentionCutoff.getTime());
        long count = sqlTemplate.update(getSql("deleteExtractRequestSql"),
                ExtractRequest.ExtractStatus.OK.name(), retentionCutoff.getTime());
        if (count > 0) {
            log.info("Purged {} extract requests", count);
        }        	
        return count;

    }

    private long purgeRegistrationRequests() {
        Calendar retentionCutoff = Calendar.getInstance();
        retentionCutoff.add(Calendar.MINUTE, -parameterService
                .getInt(ParameterConstants.PURGE_REGISTRATION_REQUEST_RETENTION_MINUTES));
        log.info("Purging registration requests that are older than {}", retentionCutoff.getTime());
        long count = sqlTemplate.update(getSql("deleteRegistrationRequestSql"),
                RegistrationRequest.RegistrationStatus.OK.name(),
                RegistrationRequest.RegistrationStatus.IG.name(),
                RegistrationRequest.RegistrationStatus.RR.name(), retentionCutoff.getTime());
        if (count > 0) {
            log.info("Purged {} registration requests", count);
        }    
        return count;
    }

    private long purgeMonitorEvents() {
        Calendar retentionCutoff = Calendar.getInstance();
        retentionCutoff.add(Calendar.MINUTE, -parameterService.getInt(ParameterConstants.PURGE_RETENTION_MINUTES));
        log.info("Purging monitor events that are older than {}", retentionCutoff.getTime());
        long count = sqlTemplate.update(getSql("deleteMonitorEventSql"), retentionCutoff.getTime());
        if (count > 0) {
            log.info("Purged {} monitor events", count);
        }    
        return count;
    }

    private int purgeByMinMax(long[] minMax, long minGapStartId, MinMaxDeleteSql identifier, Date retentionTime,
            int maxNumtoPurgeinTx) {
        long minId = minMax[0];
        long purgeUpToId = minMax[1];
        long ts = System.currentTimeMillis();
        int totalCount = 0;
        int totalDeleteStmts = 0;
        int idSqlType = symmetricDialect.getSqlTypeForIds();
        Timestamp cutoffTime = new Timestamp(retentionTime.getTime());
        log.info("About to purge {} using range {} through {}", identifier.toString().toLowerCase(), minMax[0], minMax[1]);
        
        while (minId <= purgeUpToId) {
            totalDeleteStmts++;
            long maxId = minId + maxNumtoPurgeinTx;
            if (maxId > purgeUpToId) {
                maxId = purgeUpToId;
            }

            String deleteSql = null;
            Object[] args = null;
            int[] argTypes = null;

            switch (identifier) {
                case DATA:
                    deleteSql = getSql("deleteDataSql");
                    args = new Object[] { minId, maxId, cutoffTime, minId, maxId, minId, maxId,
                            OutgoingBatch.Status.OK.name() };
                    argTypes = new int[] { idSqlType, idSqlType, Types.TIMESTAMP, 
                            idSqlType, idSqlType, idSqlType, idSqlType, Types.VARCHAR};
                    break;
                case DATA_RANGE:
                    deleteSql = getSql("deleteDataByRangeSql");
                    args = new Object[] { minId, maxId, cutoffTime };
                    argTypes = new int[] { idSqlType, idSqlType, Types.TIMESTAMP };
                    break;
                case DATA_EVENT:
                    deleteSql = getSql("deleteDataEventSql");
                    args = new Object[] { minId, maxId, OutgoingBatch.Status.OK.name(), minId,
                            maxId };
                    argTypes = new int[] { idSqlType, idSqlType, Types.VARCHAR, idSqlType, idSqlType};

                    break;
                case DATA_EVENT_RANGE:
                    deleteSql = getSql("deleteDataEventByRangeSql");
                    args = new Object[] { minId, maxId };
                    argTypes = new int[] { idSqlType, idSqlType };
                    break;
                case OUTGOING_BATCH:
                    deleteSql = getSql("deleteOutgoingBatchSql");
                    args = new Object[] { OutgoingBatch.Status.OK.name(), minId, maxId, minId,
                            maxId };
                    argTypes = new int[] {Types.VARCHAR, idSqlType, idSqlType, idSqlType, idSqlType};

                    break;
                case OUTGOING_BATCH_RANGE:
                    deleteSql = getSql("deleteOutgoingBatchByRangeSql");
                    args = new Object[] { minId, maxId };
                    argTypes = new int[] { idSqlType, idSqlType };
                    break;
                case STRANDED_DATA:
                    deleteSql = getSql("deleteStrandedData");
                    args = new Object[] { minId, maxId, minGapStartId, cutoffTime, minId, maxId };
                    argTypes = new int[] { idSqlType, idSqlType, idSqlType, Types.TIMESTAMP, idSqlType, idSqlType};
                    break;
                case STRANDED_DATA_EVENT:
                    deleteSql = getSql("deleteStrandedDataEvent");
                    args = new Object[] { minId, maxId, cutoffTime, minId, maxId };
                    argTypes = new int[] { idSqlType, idSqlType, Types.TIMESTAMP, idSqlType, idSqlType };
                    break;
            }

            log.debug("Running the following statement: {} with the following arguments: {}", deleteSql, Arrays.toString(args));
            int count = sqlTemplate.update(deleteSql, args, argTypes);
            log.debug("Deleted {} rows", count);
            totalCount += count;

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

    public long purgeIncoming(Calendar retentionCutoff, boolean force) {
        long purgedRowCount = 0;
        try {
            if (force || clusterService.lock(ClusterConstants.PURGE_INCOMING)) {
                try {
                    log.info("The incoming purge process is about to run");
                    purgedRowCount = purgeIncomingBatch(retentionCutoff);
                    purgedRowCount += purgeIncomingError();
                    purgedRowCount += purgeRegistrationRequests();
                    purgedRowCount += purgeMonitorEvents();
                } finally {
                    if (!force) {
                        clusterService.unlock(ClusterConstants.PURGE_INCOMING);
                    }
                    log.info("The incoming purge process has completed");
                }

            } else {
                log.debug("Could not get a lock to run an incoming purge");
            }
        } catch (Exception ex) {
            log.error("", ex);
        }
        return purgedRowCount;
    }

    private long purgeIncomingError() {
        log.info("Purging incoming error rows");
        long rowCount = 0;
        
        if (getSymmetricDialect().supportsSubselectsInDelete()) {
            rowCount = sqlTemplate.update(getSql("deleteIncomingErrorsSql"));
        } else {
            rowCount = selectIdsAndDelete(getSql("selectIncomingErrorsBatchIdsSql"), 
                    "batch_id", getSql("deleteIncomingErrorsBatchIdsSql"));
        }
        
        log.info("Purged {} incoming error rows", rowCount);
        return rowCount;
    }

    private long purgeIncomingBatch(final Calendar time) {
        log.info("Getting range for incoming batch");
        List<NodeBatchRange> nodeBatchRangeList = sqlTemplateDirty.query(
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
                totalCount += sqlTemplate.update(getSql("deleteIncomingBatchSql"),
                        new Object[] { minBatchId, maxBatchId, nodeBatchRange.getNodeId(),
                                IncomingBatch.Status.OK.name() });
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
    
    public void purgeStats(boolean force) {
        Calendar retentionCutoff = Calendar.getInstance();
        retentionCutoff.add(Calendar.MINUTE,
                -parameterService.getInt(ParameterConstants.PURGE_STATS_RETENTION_MINUTES));
        if (force || clusterService.lock(ClusterConstants.PURGE_STATISTICS)) {
            try {
                int purgedCount = sqlTemplate.update(getSql("purgeNodeHostChannelStatsSql"),
                        retentionCutoff.getTime());
                purgedCount += sqlTemplate.update(getSql("purgeNodeHostStatsSql"),
                        retentionCutoff.getTime());
                purgedCount += sqlTemplate.update(getSql("purgeNodeHostJobStatsSql"),
                        retentionCutoff.getTime());
                if (purgedCount > 0) {
                    log.debug("{} stats rows were purged", purgedCount);
                }
            } finally {
                if (!force) {
                    clusterService.unlock(ClusterConstants.PURGE_STATISTICS);
                }
            }
        }
    }

    static class NodeBatchRange {
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
    
    protected int selectIdsAndDelete(String selectSql, String fieldName, String deleteSql) {
        List<Row> results = sqlTemplate.query(selectSql);
        int rowCount = 0;
        if (! results.isEmpty()) {            
            List<Integer> ids = new ArrayList<Integer>(results.size());
            for (Row row : results) {
                ids.add(row.getInt(fieldName));
            }
            
            results = null;
            
            StringBuilder placeHolders = new StringBuilder(ids.size()*2);
            for (int i = 0; i < ids.size(); i++) {
                placeHolders.append("?,");
            }
            placeHolders.setLength(placeHolders.length()-1);
            
            String deleteStatement = deleteSql.replace("?", placeHolders);
            
            rowCount = sqlTemplate.update(deleteStatement, ids.toArray());
        }
        return rowCount;
    }
    
    

}
