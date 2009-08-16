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
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatch.Status;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.symmetric.util.MaxRowsStatementCreator;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;

public class IncomingBatchService extends AbstractService implements IIncomingBatchService {

    public IncomingBatch findIncomingBatch(long batchId, String nodeId) {
        try {
            return (IncomingBatch) jdbcTemplate.queryForObject(getSql("findIncomingBatchSql"), new Object[] { batchId,
                    nodeId }, new IncomingBatchMapper());
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public List<IncomingBatch> findIncomingBatchErrors(int maxRows) {
        return (List<IncomingBatch>) jdbcTemplate.query(new MaxRowsStatementCreator(
                getSql("findIncomingBatchErrorsSql"), maxRows), new IncomingBatchMapper());
    }

    public boolean acquireIncomingBatch(IncomingBatch batch) {
        boolean okayToProcess = true;
        if (batch.isPersistable()) {
            Object savepoint = dbDialect.createSavepointForFallback();

            try {
                insertIncomingBatch(batch);
                dbDialect.releaseSavepoint(savepoint);
            } catch (DataIntegrityViolationException e) {
                dbDialect.rollbackToSavepoint(savepoint);
                batch.setRetry(true);
                IncomingBatch existingBatch = findIncomingBatch(batch.getBatchId(), batch.getNodeId());
                if (existingBatch.getStatus() == Status.ER
                        || !parameterService.is(ParameterConstants.INCOMING_BATCH_SKIP_DUPLICATE_BATCHES_ENABLED)) {
                    okayToProcess = true;
                    log.warn("BatchRetrying", batch.getNodeBatchId());
                } else {
                    okayToProcess = false;
                    batch.setSkipCount(existingBatch.getSkipCount() + 1);
                    log.warn("BatchSkipping", batch.getNodeBatchId());
                }
            }
        }
        return okayToProcess;
    }

    public void insertIncomingBatch(IncomingBatch batch) {
        batch.setLastUpdatedHostName(AppUtils.getServerId());
        batch.setLastUpdatedTime(new Date());
        jdbcTemplate.update(getSql("insertIncomingBatchSql"), new Object[] { Long.valueOf(batch.getBatchId()),
                batch.getNodeId(), batch.getChannelId(), batch.getStatus().toString(), batch.getNetworkMillis(),
                batch.getFilterMillis(), batch.getDatabaseMillis(), batch.getFailedRowNumber(), batch.getByteCount(),
                batch.getStatementCount(), batch.getFallbackInsertCount(), batch.getFallbackUpdateCount(),
                batch.getMissingDeleteCount(), batch.getSkipCount(), batch.getSqlState(), batch.getSqlCode(),
                StringUtils.abbreviate(batch.getSqlMessage(), 1000), batch.getLastUpdatedHostName(),
                batch.getLastUpdatedTime() });
    }

    public int updateIncomingBatch(IncomingBatch batch) {
        batch.setLastUpdatedHostName(AppUtils.getServerId());
        batch.setLastUpdatedTime(new Date());
        return jdbcTemplate.update(getSql("updateIncomingBatchSql"), new Object[] { batch.getStatus().toString(),
                batch.getNetworkMillis(), batch.getFilterMillis(), batch.getDatabaseMillis(),
                batch.getFailedRowNumber(), batch.getByteCount(), batch.getStatementCount(),
                batch.getFallbackInsertCount(), batch.getFallbackUpdateCount(), batch.getMissingDeleteCount(),
                batch.getSkipCount(), batch.getSqlState(), batch.getSqlCode(),
                StringUtils.abbreviate(batch.getSqlMessage(), 1000), batch.getLastUpdatedHostName(),
                batch.getLastUpdatedTime(), Long.valueOf(batch.getBatchId()), batch.getNodeId() });
    }

    class IncomingBatchMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int num) throws SQLException {
            IncomingBatch batch = new IncomingBatch();
            batch.setBatchId(rs.getLong(1));
            batch.setNodeId(rs.getString(2));
            batch.setChannelId(rs.getString(3));
            batch.setStatus(IncomingBatch.Status.valueOf(rs.getString(4)));
            batch.setNetworkMillis(rs.getLong(5));
            batch.setFilterMillis(rs.getLong(6));
            batch.setDatabaseMillis(rs.getLong(7));
            batch.setFailedRowNumber(rs.getLong(8));
            batch.setByteCount(rs.getLong(9));
            batch.setStatementCount(rs.getLong(10));
            batch.setFallbackInsertCount(rs.getLong(11));
            batch.setFallbackUpdateCount(rs.getLong(12));
            batch.setMissingDeleteCount(rs.getLong(13));
            batch.setSkipCount(rs.getLong(14));
            batch.setSqlState(rs.getString(15));
            batch.setSqlCode(rs.getInt(16));
            batch.setSqlMessage(rs.getString(17));
            batch.setLastUpdatedHostName(rs.getString(18));
            batch.setLastUpdatedTime(rs.getTimestamp(19));
            batch.setCreateTime(rs.getTimestamp(20));
            return batch;
        }
    }

}
