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
import java.util.List;

import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IOutgoingBatchHistoryService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.transaction.annotation.Transactional;

public class AcknowledgeService extends AbstractService implements IAcknowledgeService {
    private String updateOutgoingBatchSql;

    private String selectDataIdSql;

    private IOutgoingBatchHistoryService outgoingBatchHistoryService;

    @Transactional
    public void ack(List<BatchInfo> batches) {
        for (final BatchInfo batch : batches) {
            final Integer id = new Integer(batch.getBatchId());
            // update the outgoing_batch record
            jdbcTemplate.execute(new ConnectionCallback() {
                public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                    PreparedStatement batchUpdate = null;
                    try {
                        batchUpdate = conn.prepareStatement(updateOutgoingBatchSql);
                        batchUpdate.setString(1, batch.isOk() ? Status.OK.name() : Status.ER.name());
                        batchUpdate.setInt(2, id);
                        batchUpdate.executeUpdate();
                        return null;
                    } finally {
                        JdbcUtils.closeStatement(batchUpdate);
                    }
                }
            });

            // add a record to outgoing_batch_hist indicating success
            if (batch.isOk()) {
                outgoingBatchHistoryService.ok(id);
            }
            // add a record to outgoing_batch_hist indicating an error
            else {
                if (batch.getErrorLine() != BatchInfo.UNDEFINED_ERROR_LINE_NUMBER) {
                    CallBackHandler handler = new CallBackHandler(batch.getErrorLine());

                    jdbcTemplate.query(selectDataIdSql, new Object[] { id }, handler);
                    final long dataId = handler.getDataId();

                    outgoingBatchHistoryService.error(id, dataId);
                } else {
                    outgoingBatchHistoryService.error(id, 0l);
                }
            }
        }
    }

    public void setUpdateOutgoingBatchSql(String updateOutgoingBatchSql) {
        this.updateOutgoingBatchSql = updateOutgoingBatchSql;
    }

    public void setSelectDataIdSql(String selectDataIdSql) {
        this.selectDataIdSql = selectDataIdSql;
    }

    public void setOutgoingBatchHistoryService(IOutgoingBatchHistoryService outgoingBatchHistoryService) {
        this.outgoingBatchHistoryService = outgoingBatchHistoryService;
    }

    class CallBackHandler implements RowCallbackHandler {
        int index = 0;

        long dataId = -1;

        long rowNumber;

        CallBackHandler(long rowNumber) {
            this.rowNumber = rowNumber;
        }

        public void processRow(ResultSet rs) throws SQLException {
            index++;

            if (index == rowNumber) {
                dataId = rs.getLong(1);
            }
        }

        public long getDataId() {
            return dataId;
        }
    }

}
