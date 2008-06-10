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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.LockAction;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.support.JdbcUtils;

public class PurgeService extends AbstractService implements IPurgeService {

    private final static Log logger = LogFactory.getLog(PurgeService.class);

    private IDbDialect dbDialect;

    private List<String> incomingPurgeSql;

    private List<String> deleteIncomingBatchesByNodeIdSql;

    private IClusterService clusterService;

    @SuppressWarnings("unchecked")
    public void purge() {
        Calendar retentionCutoff = Calendar.getInstance();
        retentionCutoff.add(Calendar.MINUTE, -parameterService.getInt(ParameterConstants.PURGE_RETENTION_MINUTES));
        purgeOutgoing(retentionCutoff);
        purgeIncoming(retentionCutoff);
        purgeStatistic(retentionCutoff);
    }
    
    private void purgeStatistic(Calendar retentionCutoff) {
        try {
            if (clusterService.lock(LockAction.PURGE_STATISTICS)) {
                try {
                    logger.info("The statistic purge process is about to run.");
                    int count = jdbcTemplate.update(
                            getSql("deleteFromStatisticSql"),
                            new Object[] { retentionCutoff.getTime() });
                    logger.info("Purged " + count + " statistic rows.");
                } finally {
                    clusterService.unlock(LockAction.PURGE_STATISTICS);
                    logger.info("The statistic purge process has completed.");
                }

            } else {
                logger.info("Could not get a lock to run an statistic purge.");
            }
        } catch (Exception ex) {
            logger.error(ex, ex);
        }
    }

    private void purgeOutgoing(Calendar retentionCutoff) {
        try {
            if (clusterService.lock(LockAction.PURGE_OUTGOING)) {
                try {
                    logger.info("The outgoing purge process is about to run.");

                    purgeBatchesOlderThan(retentionCutoff);
                    purgeDataRows(retentionCutoff);

                } finally {
                    clusterService.unlock(LockAction.PURGE_OUTGOING);
                    logger.info("The outgoing purge process has completed.");
                }

            } else {
                logger.info("Could not get a lock to run an outgoing purge.");
            }
        } catch (Exception ex) {
            logger.error(ex, ex);
        }
    }

    private void purgeIncoming(final Calendar retentionCutoff) {
        try {
            if (clusterService.lock(LockAction.PURGE_INCOMING)) {
                try {
                    logger.info("The incoming purge process is about to run.");

                    jdbcTemplate.execute(new ConnectionCallback() {
                        public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                            List<Integer> dataIds = new ArrayList<Integer>();
                            PreparedStatement st = null;
                            ResultSet rs = null;

                            try {
                                long ts = System.currentTimeMillis();
                                st = conn.prepareStatement(getSql("selectIncomingBatchOrderByCreateTimeSql"),
                                        java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
                                st.setFetchSize(dbDialect.getStreamingResultsFetchSize());
                                rs = st.executeQuery();
                                int totalRowsPurged = 0;
                                while (rs.next()) {
                                    int batchId = rs.getInt(1);
                                    String nodeId = rs.getString(2);
                                    String status = rs.getString(3);
                                    Date createTime = rs.getTimestamp(4);
                                    if (createTime.after(retentionCutoff.getTime())) {
                                        logger.info("Done purging incoming.  Purged " + totalRowsPurged
                                                + " total batch and hist rows up through " + createTime);
                                        break;
                                    }
                                    if ("OK".equals(status)) {
                                        for (String sql : incomingPurgeSql) {
                                            totalRowsPurged += jdbcTemplate.update(sql,
                                                    new Object[] { batchId, nodeId });
                                        }
                                    }

                                    if (totalRowsPurged > 0
                                            && (System.currentTimeMillis() - ts > DateUtils.MILLIS_PER_MINUTE * 5)) {
                                        logger.info("Purged " + totalRowsPurged
                                                + " total incoming batch and hist rows up through " + createTime);
                                        ts = System.currentTimeMillis();
                                    }
                                }
                            } finally {
                                JdbcUtils.closeResultSet(rs);
                                JdbcUtils.closeStatement(st);
                            }

                            return dataIds;
                        }
                    });

                } finally {
                    clusterService.unlock(LockAction.PURGE_INCOMING);
                    logger.info("The incoming purge process has completed.");
                }

            } else {
                logger.info("Could not get a lock to run an incoming purge.");
            }
        } catch (Exception ex) {
            logger.error(ex, ex);
        }
    }

    public void purgeAllIncomingEventsForNode(String nodeId) {
        if (deleteIncomingBatchesByNodeIdSql != null)
            for (String sql : deleteIncomingBatchesByNodeIdSql) {
                int count = jdbcTemplate.update(sql, new Object[] { nodeId });
                logger.info("Purged " + count + " rows for node " + nodeId + " after running: " + cleanSql(sql));
            }
    }

    private void purgeDataRows(final Calendar time) {
        int maxNumOfDataIdsToPurgeInTx = parameterService.getInt(ParameterConstants.PURGE_MAX_NUMBER_OF_DATA_IDS);
        logger.info("About to purge data rows.");
        int minDataId = jdbcTemplate.queryForInt(getSql("selectMinDataIdSql"));
        int purgeUpToDataId = jdbcTemplate.queryForInt(getSql("selectMaxDataIdSql"), new Object[] { time.getTime()});
        int maxDataId = minDataId + maxNumOfDataIdsToPurgeInTx;
        int deletedCount = 0;
        long ts = System.currentTimeMillis();
        int totalCount = 0;

        do {
            deletedCount = jdbcTemplate.update(getSql("deleteFromDataSql"), new Object[] { minDataId, maxDataId, minDataId,
                    maxDataId });
            totalCount += deletedCount;
            if (totalCount > 0 && (System.currentTimeMillis() - ts > DateUtils.MILLIS_PER_MINUTE * 5)) {
                logger.info("Purged a total of " + totalCount + " data rows so far.");
                ts = System.currentTimeMillis();
            }
            minDataId += maxNumOfDataIdsToPurgeInTx;
            maxDataId += maxNumOfDataIdsToPurgeInTx;
        } while (maxDataId <= purgeUpToDataId);

        logger.info("Done purging " + totalCount + " data rows.");

    }

    @SuppressWarnings("unchecked")
    private void purgeBatchesOlderThan(final Calendar time) {
        jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                PreparedStatement st = null;
                ResultSet rs = null;
                int eventRowCount = 0;
                int batchesPurged = 0;
                long ts = System.currentTimeMillis();

                try {
                    st = conn.prepareStatement(getSql("selectOutgoingBatchIdsToPurgeSql"), java.sql.ResultSet.TYPE_FORWARD_ONLY,
                            java.sql.ResultSet.CONCUR_READ_ONLY);
                    st.setFetchSize(dbDialect.getStreamingResultsFetchSize());
                    st.setTimestamp(1, new Timestamp(time.getTime().getTime()));
                    rs = st.executeQuery();
                    while (rs.next()) {
                        final int batchId = rs.getInt(1);
                        final String nodeId = rs.getString(2);

                        eventRowCount += jdbcTemplate
                                .update(getSql("deleteFromEventDataIdSql"), new Object[] { batchId, nodeId });
                        batchesPurged += jdbcTemplate.update(getSql("deleteFromOutgoingBatchSql"), new Object[] { batchId });
                        jdbcTemplate.update(getSql("deleteFromOutgoingBatchHistSql"), new Object[] { batchId, nodeId });
                        
                        if (System.currentTimeMillis() - ts > DateUtils.MILLIS_PER_MINUTE * 5) {
                            logger.info("Purged " + batchesPurged + " batches and " + eventRowCount
                                    + " data_events so far.");
                            ts = System.currentTimeMillis();
                        }

                    }

                    logger.info("Purged a total of " + batchesPurged + " batches and " + eventRowCount
                            + " data_events.");
                } finally {
                    JdbcUtils.closeResultSet(rs);
                    JdbcUtils.closeStatement(st);
                }
                return null;
            }
        });
    }

    private String cleanSql(String sql) {
        return StringUtils.replace(StringUtils.replace(StringUtils.replace(sql, "\r", " "), "\n", " "), "  ", "");
    }

    public void setIncomingPurgeSql(List<String> purgeSql) {
        this.incomingPurgeSql = purgeSql;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void setDeleteIncomingBatchesByNodeIdSql(List<String> deleteIncomingBatchesByNodeIdSql) {
        this.deleteIncomingBatchesByNodeIdSql = deleteIncomingBatchesByNodeIdSql;
    }



}
