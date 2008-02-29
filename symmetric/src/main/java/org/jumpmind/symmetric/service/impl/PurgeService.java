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
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.LockAction;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

public class PurgeService extends AbstractService implements IPurgeService {

    private final static Log logger = LogFactory.getLog(PurgeService.class);

    private int maxNumOfDataIdsToPurgeInTx = 5000;

    private IDbDialect dbDialect;

    private String[] incomingPurgeSql;

    private String[] deleteIncomingBatchesByNodeIdSql;

    private int retentionInMinutes = 7200;

    private String selectOutgoingBatchIdsToPurgeSql;

    private String deleteFromOutgoingBatchHistSql;

    private String deleteFromOutgoingBatchSql;

    private String deleteFromEventDataIdSql;

    private String deleteFromDataSql;

    private String selectMinDataIdSql;

    private String selectIncomingBatchOrderByCreateTimeSql;

    private String selectOutgoingBatchHistoryRangeSql;

    private TransactionTemplate transactionTemplate;

    private IClusterService clusterService;

    @SuppressWarnings("unchecked")
    public void purge() {
        Calendar retentionCutoff = Calendar.getInstance();
        retentionCutoff.add(Calendar.MINUTE, -retentionInMinutes);

        purgeOutgoing(retentionCutoff);
        purgeIncoming(retentionCutoff);

    }

    private void purgeOutgoing(Calendar retentionCutoff) {
        try {
            if (clusterService.lock(LockAction.PURGE_OUTGOING)) {
                try {
                    logger.info("The outgoing purge process is about to run.");

                    purgeBatchesOlderThan(retentionCutoff);
                    purgeOutgoingBatchHistory(retentionCutoff);
                    purgeDataRows();

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

    private void purgeOutgoingBatchHistory(Calendar retentionCutoff) {
        logger.info("About to purge outgoing batch history.");
        int[] minMax = (int[]) jdbcTemplate.queryForObject(selectOutgoingBatchHistoryRangeSql,
                new Object[] { retentionCutoff.getTime() }, new RowMapper() {
                    public Object mapRow(ResultSet rs, int row) throws SQLException {
                        return new int[] { rs.getInt(1), rs.getInt(2) };
                    }
                });
        if (minMax != null) {
            int currentHistoryId = minMax[0];
            int max = minMax[1];
            long ts = System.currentTimeMillis();
            int totalCount = 0;
            while (currentHistoryId < max) {
                currentHistoryId = currentHistoryId + maxNumOfDataIdsToPurgeInTx > max ? max : currentHistoryId
                        + maxNumOfDataIdsToPurgeInTx;
                totalCount += jdbcTemplate.update(deleteFromOutgoingBatchHistSql, new Object[] { currentHistoryId });
                
                if (totalCount > 0 && (System.currentTimeMillis() - ts > DateUtils.MILLIS_PER_MINUTE * 5)) {
                    logger.info("Purged " + totalCount + " a total of outgoing batch history rows so far.");
                    ts = System.currentTimeMillis();
                }
            }
            
            logger.info("Finished purging " + totalCount + " outgoing batch history rows.");

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
                                st = conn.prepareStatement(selectIncomingBatchOrderByCreateTimeSql,
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

    private void purgeDataRows() {
        logger.info("About to purge data rows.");
        int minDataId = jdbcTemplate.queryForInt(selectMinDataIdSql);
        int maxDataId = minDataId + maxNumOfDataIdsToPurgeInTx;
        int deletedCount = 0;
        long ts = System.currentTimeMillis();
        int totalCount = 0;

        do {
            deletedCount = jdbcTemplate.update(deleteFromDataSql, new Object[] { minDataId, maxDataId, minDataId, maxDataId });
            totalCount += deletedCount;            
            if (totalCount > 0 && (System.currentTimeMillis() - ts > DateUtils.MILLIS_PER_MINUTE * 5)) {
                logger.info("Purged " + totalCount + " total of data rows so far.");
                ts = System.currentTimeMillis();
            }
            minDataId += maxNumOfDataIdsToPurgeInTx;
            maxDataId += maxNumOfDataIdsToPurgeInTx;
        } while (deletedCount > 0);

        logger.info("Done purging " + totalCount + " data rows.");

    }

    @SuppressWarnings("unchecked")
    private void purgeBatchesOlderThan(final Calendar time) {
        jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                PreparedStatement st = null;
                ResultSet rs = null;
                int eventRowCount = 0;
                int dataIdCount = 0;
                int batchesPurged = 0;
                long ts = System.currentTimeMillis();

                try {
                    st = conn.prepareStatement(selectOutgoingBatchIdsToPurgeSql, java.sql.ResultSet.TYPE_FORWARD_ONLY,
                            java.sql.ResultSet.CONCUR_READ_ONLY);
                    st.setFetchSize(dbDialect.getStreamingResultsFetchSize());
                    st.setTimestamp(1, new Timestamp(time.getTime().getTime()));
                    rs = st.executeQuery();
                    while (rs.next()) {
                        final int batchId = rs.getInt(1);
                        final String nodeId = rs.getString(2);

                        do {
                            dataIdCount = (Integer) transactionTemplate.execute(new TransactionCallback() {
                                public Object doInTransaction(final TransactionStatus s) {
                                    int eventCount = jdbcTemplate.update(deleteFromEventDataIdSql, new Object[] {
                                            batchId, nodeId });

                                    jdbcTemplate.update(deleteFromOutgoingBatchSql, new Object[] { batchId });
                                    return eventCount;
                                }
                            });
                            eventRowCount += dataIdCount;

                            if (System.currentTimeMillis() - ts > DateUtils.MILLIS_PER_MINUTE * 5) {
                                logger.info("Purged " + batchesPurged + " batches and " + eventRowCount
                                        + " data_events so far.");
                                ts = System.currentTimeMillis();
                            }
                        } while (dataIdCount > 0);

                        batchesPurged++;
                    }

                    logger.info("Done purging a total of " + batchesPurged + " batches and " + eventRowCount
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

    public void setIncomingPurgeSql(String[] purgeSql) {
        this.incomingPurgeSql = purgeSql;
    }

    public void setRetentionInMinutes(int retentionInMinutes) {
        this.retentionInMinutes = retentionInMinutes;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setSelectOutgoingBatchIdsToPurgeSql(String selectOutgoingBatchIdsToPurgeSql) {
        this.selectOutgoingBatchIdsToPurgeSql = selectOutgoingBatchIdsToPurgeSql;
    }

    public void setDeleteFromOutgoingBatchHistSql(String deleteFromOutgoingBatchHistSql) {
        this.deleteFromOutgoingBatchHistSql = deleteFromOutgoingBatchHistSql;
    }

    public void setDeleteFromOutgoingBatchSql(String deleteFromOutgoingBatchSql) {
        this.deleteFromOutgoingBatchSql = deleteFromOutgoingBatchSql;
    }

    public void setDeleteFromEventDataIdSql(String selectDataIdToPurgeSql) {
        this.deleteFromEventDataIdSql = selectDataIdToPurgeSql;
    }

    public void setTransactionTemplate(TransactionTemplate transactionTemplate) {
        this.transactionTemplate = transactionTemplate;
    }

    public void setDeleteFromDataSql(String selectDataIdToDeleteSql) {
        this.deleteFromDataSql = selectDataIdToDeleteSql;
    }

    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void setMaxNumOfDataIdsToPurgeInTx(int maxNumOfDataIdsToPurgeInTx) {
        this.maxNumOfDataIdsToPurgeInTx = maxNumOfDataIdsToPurgeInTx;
    }

    public void setDeleteIncomingBatchesByNodeIdSql(String[] deleteIncomingBatchesByNodeIdSql) {
        this.deleteIncomingBatchesByNodeIdSql = deleteIncomingBatchesByNodeIdSql;
    }

    public void setSelectIncomingBatchOrderByCreateTimeSql(String selectIncomingBatchOrderByCreateTimeSql) {
        this.selectIncomingBatchOrderByCreateTimeSql = selectIncomingBatchOrderByCreateTimeSql;
    }

    public void setSelectOutgoingBatchHistoryRangeSql(String selectOutgoingBatchHistoryRangeSql) {
        this.selectOutgoingBatchHistoryRangeSql = selectOutgoingBatchHistoryRangeSql;
    }

    public void setSelectMinDataIdSql(String selectMinDataIdSql) {
        this.selectMinDataIdSql = selectMinDataIdSql;
    }

}
