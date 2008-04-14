/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
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
import java.sql.Types;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatchHistory;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.util.MaxRowsStatementCreator;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;

public class IncomingBatchService extends AbstractService implements IIncomingBatchService {

    private static final Log logger = LogFactory.getLog(IncomingBatchService.class);

    private String findIncomingBatchSql;

    private String findIncomingBatchErrorsSql;

    private String findIncomingBatchHistorySql;

    private String insertIncomingBatchSql;

    private String updateIncomingBatchSql;

    private String insertIncomingBatchHistorySql;

    private boolean skipDuplicateBatches = true;

    private IDbDialect dbDialect;

    public IncomingBatch findIncomingBatch(String batchId, String nodeId) {
        try {
            return (IncomingBatch) jdbcTemplate.queryForObject(findIncomingBatchSql, new Object[] { batchId,
                    nodeId }, new IncomingBatchMapper());
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public List<IncomingBatch> findIncomingBatchErrors(int maxRows) {
        return (List<IncomingBatch>) jdbcTemplate.query(new MaxRowsStatementCreator(
                findIncomingBatchErrorsSql, maxRows), new IncomingBatchMapper());
    }

    @SuppressWarnings("unchecked")
    public List<IncomingBatchHistory> findIncomingBatchHistory(String batchId, String nodeId) {
        return (List<IncomingBatchHistory>) jdbcTemplate.query(findIncomingBatchHistorySql, new Object[] {
                batchId, nodeId }, new IncomingBatchHistoryMapper());
    }

    public boolean acquireIncomingBatch(final IncomingBatch status) {
        Object savepoint = dbDialect.createSavepointForFallback();
        boolean okayToProcess = true;
        try {
            insertIncomingBatch(status);
            dbDialect.releaseSavepoint(savepoint);
        } catch (DataIntegrityViolationException e) {
            dbDialect.rollbackToSavepoint(savepoint);
            status.setRetry(true);
            okayToProcess = updateIncomingBatch(status) > 0 || (!skipDuplicateBatches);
            if (okayToProcess) {
                logger.warn("Retrying batch " + status.getNodeBatchId());
            } else {
                logger.warn("Skipping batch " + status.getNodeBatchId());
            }
        }
        return okayToProcess;
    }

    public void insertIncomingBatch(IncomingBatch status) {
        jdbcTemplate.update(insertIncomingBatchSql, new Object[] { Long.valueOf(status.getBatchId()),
                status.getNodeId(), status.getStatus().toString() });
    }

    public int updateIncomingBatch(IncomingBatch status) {
        return jdbcTemplate.update(updateIncomingBatchSql, new Object[] { status.getStatus().toString(),
                Long.valueOf(status.getBatchId()), status.getNodeId(), IncomingBatch.Status.ER.toString() });
    }

    public void insertIncomingBatchHistory(IncomingBatchHistory history) {
        try {
            jdbcTemplate.update(insertIncomingBatchHistorySql, new Object[] {
                    Long.valueOf(history.getBatchId()), history.getNodeId(), history.getStatus().toString(),
                    history.getNetworkMillis(), history.getFilterMillis(), history.getDatabaseMillis(),
                    history.getHostName(), history.getByteCount(), history.getStatementCount(),
                    history.getFallbackInsertCount(), history.getFallbackUpdateCount(),
                    history.getMissingDeleteCount(), history.getFailedRowNumber(), history.getStartTime(),
                    history.getEndTime(), history.getSqlState(), history.getSqlCode(),
                    StringUtils.abbreviate(history.getSqlMessage(), 50) }, new int[] { Types.INTEGER, Types.VARCHAR, Types.CHAR,
                    Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER,
                    Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.TIMESTAMP,
                    Types.TIMESTAMP, Types.VARCHAR, Types.INTEGER, Types.VARCHAR });
        } catch (RuntimeException ex) {
            logger.error(ex, ex);
            throw ex;
        }
    }

    class IncomingBatchMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int num) throws SQLException {
            IncomingBatch batch = new IncomingBatch();
            batch.setBatchId(rs.getString(1));
            batch.setNodeId(rs.getString(2));
            batch.setStatus(IncomingBatch.Status.valueOf(rs.getString(3)));
            batch.setCreateTime(rs.getTimestamp(4));
            return batch;
        }
    }

    class IncomingBatchHistoryMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int num) throws SQLException {
            IncomingBatchHistory history = new IncomingBatchHistory();
            history.setBatchId(rs.getString(1));
            history.setNodeId(rs.getString(2));
            history.setStatus(IncomingBatchHistory.Status.valueOf(rs.getString(3)));
            history.setStartTime(rs.getTime(4));
            history.setEndTime(rs.getTime(5));
            history.setFailedRowNumber(rs.getLong(6));
            history.setByteCount(rs.getLong(7));
            history.setNetworkMillis(rs.getLong(8));
            history.setFilterMillis(rs.getLong(9));
            history.setDatabaseMillis(rs.getLong(10));
            history.setStatementCount(rs.getLong(11));
            history.setFallbackInsertCount(rs.getLong(12));
            history.setFallbackUpdateCount(rs.getLong(13));
            history.setMissingDeleteCount(rs.getLong(14));
            history.setSqlState(rs.getString(15));
            history.setSqlCode(rs.getInt(16));
            history.setSqlMessage(rs.getString(17));
            return history;
        }
    }

    public void setFindIncomingBatchHistorySql(String findIncomingBatchHistorySql) {
        this.findIncomingBatchHistorySql = findIncomingBatchHistorySql;
    }

    public void setFindIncomingBatchSql(String findIncomingBatchSql) {
        this.findIncomingBatchSql = findIncomingBatchSql;
    }

    public void setInsertIncomingBatchHistorySql(String insertIncomingBatchHistorySql) {
        this.insertIncomingBatchHistorySql = insertIncomingBatchHistorySql;
    }

    public void setInsertIncomingBatchSql(String insertIncomingBatchSql) {
        this.insertIncomingBatchSql = insertIncomingBatchSql;
    }

    public void setUpdateIncomingBatchSql(String updateIncomingBatchSql) {
        this.updateIncomingBatchSql = updateIncomingBatchSql;
    }

    public void setSkipDuplicateBatches(boolean skipDuplicateBatchesEnabled) {
        this.skipDuplicateBatches = skipDuplicateBatchesEnabled;
    }

    public void setFindIncomingBatchErrorsSql(String findIncomingBatchErrorsSql) {
        this.findIncomingBatchErrorsSql = findIncomingBatchErrorsSql;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

}
