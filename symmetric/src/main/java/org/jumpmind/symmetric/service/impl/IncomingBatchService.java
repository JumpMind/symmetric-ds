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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatchHistory;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;

public class IncomingBatchService extends AbstractService implements IIncomingBatchService {

    private static final Log logger = LogFactory.getLog(IncomingBatchService.class);

    private String findIncomingBatchSql;

    private String findIncomingBatchHistorySql;

    private String insertIncomingBatchSql;

    private String updateIncomingBatchSql;

    private String insertIncomingBatchHistorySql;

    private boolean skipDuplicateBatches = true;

    public IncomingBatch findIncomingBatch(String batchId, String clientId) {
        try {
            return (IncomingBatch) jdbcTemplate.queryForObject(findIncomingBatchSql,
                    new Object[] { batchId, clientId }, new IncomingBatchMapper());
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public List<IncomingBatchHistory> findIncomingBatchHistory(String batchId, String clientId) {
        return (List<IncomingBatchHistory>) jdbcTemplate.query(findIncomingBatchHistorySql, new Object[] { batchId,
                clientId }, new IncomingBatchHistoryMapper());
    }

    public boolean acquireIncomingBatch(IncomingBatch status) {
        boolean okayToProcess = true;
        try {
            insertIncomingBatch(status);
        } catch (DataIntegrityViolationException e) {
            status.setRetry(true);
            okayToProcess = updateIncomingBatch(status) > 0 || (! skipDuplicateBatches);
            if (okayToProcess) {
                logger.warn("Retrying batch " + status.getClientBatchId());
            } else {
                logger.warn("Skipping batch " + status.getClientBatchId());
            }
        }
        return okayToProcess;
    }

    public void insertIncomingBatch(IncomingBatch status) {
        jdbcTemplate.update(insertIncomingBatchSql, new Object[] { status.getBatchId(), status.getClientId(),
                status.getStatus().toString() });
    }

    public int updateIncomingBatch(IncomingBatch status) {
        return jdbcTemplate.update(updateIncomingBatchSql, new Object[] { status.getStatus().toString(),
                status.getBatchId(), status.getClientId(), IncomingBatch.Status.ER.toString() });
    }

    public void insertIncomingBatchHistory(IncomingBatchHistory history) {
        jdbcTemplate.update(insertIncomingBatchHistorySql, new Object[] { history.getBatchId(), history.getClientId(),
                history.getStatus().toString(), history.getHostName(), history.getStatementCount(),
                history.getFallbackInsertCount(), history.getFallbackUpdateCount(), history.getMissingDeleteCount(),
                history.getFailedRowNumber(), history.getStartTime(), history.getEndTime() });
    }

    class IncomingBatchMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int num) throws SQLException {
            IncomingBatch batch = new IncomingBatch();
            batch.setBatchId(rs.getString(1));
            batch.setClientId(rs.getString(2));
            batch.setStatus(IncomingBatch.Status.valueOf(rs.getString(3)));
            return batch;
        }
    }

    class IncomingBatchHistoryMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int num) throws SQLException {
            IncomingBatchHistory history = new IncomingBatchHistory();
            history.setBatchId(rs.getString(1));
            history.setClientId(rs.getString(2));
            history.setStatus(IncomingBatchHistory.Status.valueOf(rs.getString(3)));
            history.setStartTime(rs.getTime(4));
            history.setEndTime(rs.getTime(5));
            history.setFailedRowNumber(rs.getLong(6));
            history.setStatementCount(rs.getLong(7));
            history.setFallbackInsertCount(rs.getLong(8));
            history.setFallbackUpdateCount(rs.getLong(9));
            history.setMissingDeleteCount(rs.getLong(10));
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

}
