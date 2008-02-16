/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.extract.IDataExtractor;
import org.jumpmind.symmetric.model.BatchType;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IExtractListener;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;

public class DataExtractorService implements IDataExtractorService {

    private IOutgoingBatchService outgoingBatchService;

    private IConfigurationService configurationService;

    private IDataExtractor dataExtractor;

    private IDbDialect dbDialect;

    private JdbcTemplate jdbcTemplate;

    private DataExtractorContext context;

    private String tablePrefix;

    private String selectEventDataToExtractSql;

    public void extractNodeIdentityFor(Node node, IOutgoingTransport transport) {
        String tableName = tablePrefix + "_node_identity";
        OutgoingBatch batch = new OutgoingBatch(node, Constants.CHANNEL_CONFIG, BatchType.INITIAL_LOAD);
        outgoingBatchService.insertOutgoingBatch(batch);

        try {
            BufferedWriter writer = transport.open();
            DataExtractorContext ctxCopy = context.copy();
            dataExtractor.init(writer, ctxCopy);
            dataExtractor.begin(batch, writer);
            TriggerHistory audit = new TriggerHistory(tableName, "node_id", "node_id");
            Data data = new Data(1, null, node.getNodeId(), DataEventType.INSERT, tableName, null, audit);
            dataExtractor.write(writer, data, ctxCopy);
            dataExtractor.commit(batch, writer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public OutgoingBatch extractInitialLoadFor(Node node, final Trigger trigger, final IOutgoingTransport transport) {

        OutgoingBatch batch = new OutgoingBatch(node, trigger.getChannelId(), BatchType.INITIAL_LOAD);
        outgoingBatchService.insertOutgoingBatch(batch);
        writeInitialLoad(node, trigger, transport, batch);
        outgoingBatchService.markOutgoingBatchSent(batch);
        return batch;
    }

    public void extractInitialLoadWithinBatchFor(Node node, final Trigger trigger, final IOutgoingTransport transport) {

        writeInitialLoad(node, trigger, transport, null);
    }

    protected void writeInitialLoad(Node node, final Trigger trigger, final IOutgoingTransport transport,
            final OutgoingBatch batch) {

        final String sql = dbDialect.createInitalLoadSqlFor(node, trigger);
        final TriggerHistory audit = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());

        jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                PreparedStatement st = null;
                ResultSet rs = null;
                try {
                    st = conn.prepareStatement(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY,
                            java.sql.ResultSet.CONCUR_READ_ONLY);
                    st.setFetchSize(dbDialect.getStreamingResultsFetchSize());
                    rs = st.executeQuery();
                    final BufferedWriter writer = transport.open();
                    final DataExtractorContext ctxCopy = context.copy();
                    if (batch != null) {
                        dataExtractor.init(writer, ctxCopy);
                        dataExtractor.begin(batch, writer);
                    }
                    while (rs.next()) {
                        dataExtractor.write(writer, new Data(0, null, rs.getString(1), DataEventType.INSERT, trigger
                                .getSourceTableName(), null, audit), ctxCopy);
                    }
                    if (batch != null) {
                        dataExtractor.commit(batch, writer);
                    }

                    return null;
                } catch (Exception e) {
                    throw new RuntimeException("Error during SQL: " + sql, e);
                } finally {
                    JdbcUtils.closeResultSet(rs);
                    JdbcUtils.closeStatement(st);
                }
            }
        });
    }

    public boolean extract(Node node, IOutgoingTransport transport) throws Exception {
        ExtractStreamHandler handler = new ExtractStreamHandler(transport);
        return extract(node, handler);
    }

    /**
     * Allow a handler callback to do the work so we can route the extracted data to 
     * other types of handlers for processing.
     */
    public boolean extract(Node node, final IExtractListener handler) throws Exception {

        outgoingBatchService.buildOutgoingBatches(node.getNodeId());

        List<OutgoingBatch> batches = outgoingBatchService.getOutgoingBatches(node.getNodeId());

        if (batches != null && batches.size() > 0) {
            try {
                handler.init();
                for (final OutgoingBatch batch : batches) {
                    try {
                        handler.startBatch(batch);
                        selectEventDataToExtract(handler, batch);
                        handler.endBatch(batch);

                        // At this point, we've already sent the data to the node, so if
                        // updating the batch to 'sent' fails, all this means is that the batch
                        // will be sent to the node again. This is expected to happen so
                        // infrequently, that the inefficiencies associated with re-sending a batch
                        // are negligible.
                        outgoingBatchService.markOutgoingBatchSent(batch);
                    } catch (Exception ex) {
                        outgoingBatchService.setBatchStatus(batch.getBatchId(), Status.ER);
                        throw ex;
                    }
                }
            } finally {
                handler.done();
            }
            return true;
        }
        return false;
    }

    public boolean extractBatchRange(IOutgoingTransport transport, String startBatchId, String endBatchId)
            throws Exception {

        ExtractStreamHandler handler = new ExtractStreamHandler(transport);
        return extractBatchRange(handler, startBatchId, endBatchId);
    }

    public boolean extractBatchRange(final IExtractListener handler, String startBatchId, String endBatchId)
            throws Exception {

        List<OutgoingBatch> batches = outgoingBatchService.getOutgoingBatchRange(startBatchId, endBatchId);

        if (batches != null && batches.size() > 0) {
            try {
                handler.init();
                for (final OutgoingBatch batch : batches) {
                    try {
                        handler.startBatch(batch);
                        selectEventDataToExtract(handler, batch);
                        handler.endBatch(batch);
                    } catch (Exception ex) {
                        outgoingBatchService.setBatchStatus(batch.getBatchId(), Status.ER);
                        throw ex;
                    }
                }
            } finally {
                handler.done();
            }
            return true;
        }
        return false;
    }

    private void selectEventDataToExtract(final IExtractListener handler, final OutgoingBatch batch) {
        jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    ps = conn.prepareStatement(selectEventDataToExtractSql, ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY);
                    ps.setFetchSize(dbDialect.getStreamingResultsFetchSize());
                    ps.setString(1, batch.getNodeId());
                    ps.setString(2, batch.getBatchId());
                    rs = ps.executeQuery();
                    while (rs.next()) {
                        try {
                            handler.dataExtracted(next(rs));
                        } catch (RuntimeException e) {
                            throw e;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                } finally {
                    JdbcUtils.closeResultSet(rs);
                    JdbcUtils.closeStatement(ps);
                }
                return null;
            }
        });
    }

    private Data next(ResultSet results) throws SQLException {
        long dataId = results.getLong(1);
        String tableName = results.getString(2);
        DataEventType eventType = DataEventType.getEventType(results.getString(3));
        String rowData = results.getString(4);
        String pk = results.getString(5);
        Date created = results.getDate(7);
        TriggerHistory audit = configurationService.getHistoryRecordFor(results.getInt(8));
        return new Data(dataId, pk, rowData, eventType, tableName, created, audit);
    }

    public void setOutgoingBatchService(IOutgoingBatchService batchBuilderService) {
        this.outgoingBatchService = batchBuilderService;
    }

    public void setDataExtractor(IDataExtractor dataExtractor) {
        this.dataExtractor = dataExtractor;
    }

    public void setContext(DataExtractorContext context) {
        this.context = context;
    }

    public void setDbDialect(IDbDialect dialect) {
        this.dbDialect = dialect;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setSelectEventDataToExtractSql(String selectEventDataToExtractSql) {
        this.selectEventDataToExtractSql = selectEventDataToExtractSql;
    }

    class ExtractStreamHandler implements IExtractListener {

        IOutgoingTransport transport;

        DataExtractorContext context;

        BufferedWriter writer;

        ExtractStreamHandler(IOutgoingTransport transport) throws Exception {
            this.transport = transport;
        }

        public void dataExtracted(Data data) throws Exception {
            dataExtractor.write(writer, data, context);
        }

        public void done() throws IOException {
        }

        public void endBatch(OutgoingBatch batch) throws Exception {
            dataExtractor.commit(batch, writer);
        }

        public void init() throws Exception {
            this.writer = transport.open();
            this.context = DataExtractorService.this.context.copy();
            dataExtractor.init(writer, context);
        }

        public void startBatch(OutgoingBatch batch) throws Exception {
            context.setBatch(batch);
            dataExtractor.begin(batch, writer);
        }

    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

}
