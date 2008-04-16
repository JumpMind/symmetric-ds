/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>
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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.NumberUtils;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.web.WebConstants;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.transaction.annotation.Transactional;

public class AcknowledgeService extends AbstractService implements IAcknowledgeService {

    private IOutgoingBatchService outgoingBatchService;

    private String selectDataIdSql;

    @Deprecated
    public void ack(final List<BatchInfo> batches) {
        for (BatchInfo batch : batches) {
            ack(batch);
        }
    }

    @Transactional
    public void ack(final BatchInfo batch) {
        OutgoingBatchHistory history = new OutgoingBatchHistory(batch);
        outgoingBatchService.setBatchStatus(batch.getBatchId(), batch.isOk() ? Status.OK : Status.ER);

        if (!batch.isOk() && batch.getErrorLine() != 0) {
            CallBackHandler handler = new CallBackHandler(batch.getErrorLine());
            jdbcTemplate.query(selectDataIdSql, new Object[] { history.getBatchId() }, handler);
            history.setFailedDataId(handler.getDataId());
        }

        history.setEndTime(new Date());
        outgoingBatchService.insertOutgoingBatchHistory(history);
    }
    
    public List<BatchInfo> getBatchInfo(Map<String, Object> parameters) {
        List<BatchInfo> batches = new ArrayList<BatchInfo>();
        for (String parameterName : parameters.keySet()) {
            if (parameterName.startsWith(WebConstants.ACK_BATCH_NAME)) {
                String batchId = parameterName.substring(WebConstants.ACK_BATCH_NAME.length());
                BatchInfo batchInfo = new BatchInfo(batchId);
                batchInfo.setNodeId(getParameter(parameters, WebConstants.NODE_ID));
                batchInfo.setNetworkMillis(getParameterAsNumber(parameters, WebConstants.ACK_NETWORK_MILLIS));
                batchInfo.setFilterMillis(getParameterAsNumber(parameters, WebConstants.ACK_FILTER_MILLIS));
                batchInfo.setDatabaseMillis(getParameterAsNumber(parameters, WebConstants.ACK_DATABASE_MILLIS));
                batchInfo.setByteCount(getParameterAsNumber(parameters, WebConstants.ACK_BYTE_COUNT));
                String status = getParameter(parameters, parameterName, "");
                batchInfo.setOk(status.equalsIgnoreCase(WebConstants.ACK_BATCH_OK));

                if (!batchInfo.isOk()) {
                    batchInfo.setErrorLine(NumberUtils.toLong(status));
                    batchInfo.setSqlState(getParameter(parameters, WebConstants.ACK_SQL_STATE));
                    batchInfo.setSqlCode((int) getParameterAsNumber(parameters, WebConstants.ACK_SQL_CODE));
                    batchInfo.setSqlMessage(getParameter(parameters, WebConstants.ACK_SQL_MESSAGE));
                }
                batches.add(batchInfo);
            }
        }
        return batches;
    }
    
    private long getParameterAsNumber(Map<String, Object> parameters, String parameterName) {
        return NumberUtils.toLong(getParameter(parameters, parameterName));
    }

    private String getParameter(Map<String, Object> parameters, String parameterName, String defaultValue) {
        String value = getParameter(parameters, parameterName);
        return value == null ? defaultValue : value;
    }
    
    private String getParameter(Map<String, Object> parameters, String parameterName) {
        Object value = parameters.get(parameterName);
        if (value instanceof String[]) {
            String[] arrayValue = (String[]) value;
            if (arrayValue.length > 0) {
                value = arrayValue[0];
            }
        }
        return (String) value;
    }

    class CallBackHandler implements RowCallbackHandler {
        int index = 0;

        long dataId = -1;

        long rowNumber;

        CallBackHandler(long rowNumber) {
            this.rowNumber = rowNumber;
        }

        public void processRow(ResultSet rs) throws SQLException {
            if (++index == rowNumber) {
                dataId = rs.getLong(1);
            }
        }

        public long getDataId() {
            return dataId;
        }
    }

    public void setOutgoingBatchService(IOutgoingBatchService outgoingBatchService) {
        this.outgoingBatchService = outgoingBatchService;
    }

    public void setSelectDataIdSql(String selectDataIdSql) {
        this.selectDataIdSql = selectDataIdSql;
    }
}
