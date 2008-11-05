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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.extract.IDataExtractor;
import org.jumpmind.symmetric.extract.IExtractorFilter;
import org.jumpmind.symmetric.model.BatchType;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IExtractListener;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.support.JdbcUtils;

public class DataExtractorService extends AbstractService implements IDataExtractorService, BeanFactoryAware {

    protected static final Log logger = LogFactory.getLog(DataExtractorService.class);

    private IOutgoingBatchService outgoingBatchService;

    private IConfigurationService configurationService;

    private IDbDialect dbDialect;

    private BeanFactory beanFactory;

    private DataExtractorContext context;

    private List<IExtractorFilter> extractorFilters;

    private String tablePrefix;

    public OutgoingBatch extractNodeIdentityFor(Node node, IOutgoingTransport transport) {
        String tableName = tablePrefix + "_node_identity";
        OutgoingBatch batch = new OutgoingBatch(node, Constants.CHANNEL_CONFIG, BatchType.INITIAL_LOAD);
        outgoingBatchService.insertOutgoingBatch(batch);
        try {
            BufferedWriter writer = transport.open();
            IDataExtractor dataExtractor = getDataExtractor(node.getSymmetricVersion());
            DataExtractorContext ctxCopy = context.copy(dataExtractor);
            dataExtractor.init(writer, ctxCopy);
            dataExtractor.begin(batch, writer);
            TriggerHistory audit = new TriggerHistory(tableName, "node_id", "node_id");
            Data data = new Data(1, null, node.getNodeId(), DataEventType.INSERT, tableName, null, audit);
            dataExtractor.write(writer, data, ctxCopy);
            dataExtractor.commit(batch, writer);
            return batch;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private IDataExtractor getDataExtractor(String version) {
        String beanName = Constants.DATA_EXTRACTOR;
        if (version != null) {
            int[] versions = Version.parseVersion(version);
            // TODO: this should be versions[1] == 0 for 1.2 release
            if (versions[0] == 1 && versions[1] <= 1) {
                beanName += "10";
            } else if (versions[0] == 1 && versions[1] <= 3) {
                beanName += "13";
            }
        }
        return (IDataExtractor) beanFactory.getBean(beanName);
    }

    public OutgoingBatch extractInitialLoadFor(Node node, final Trigger trigger, final IOutgoingTransport transport) {

        OutgoingBatch batch = new OutgoingBatch(node, trigger.getChannelId(), BatchType.INITIAL_LOAD);
        outgoingBatchService.insertOutgoingBatch(batch);
        OutgoingBatchHistory history = new OutgoingBatchHistory(batch);
        writeInitialLoad(node, trigger, transport, batch, null);
        history.setStatus(OutgoingBatchHistory.Status.SE);
        history.setEndTime(new Date());
        outgoingBatchService.insertOutgoingBatchHistory(history);
        return batch;
    }

    public void extractInitialLoadWithinBatchFor(Node node, final Trigger trigger, final IOutgoingTransport transport,
            DataExtractorContext ctx) {
        writeInitialLoad(node, trigger, transport, null, ctx);
    }

    protected void writeInitialLoad(Node node, final Trigger trigger, final IOutgoingTransport transport,
            final OutgoingBatch batch, final DataExtractorContext ctx) {

        final String sql = dbDialect.createInitalLoadSqlFor(node, trigger);
        final TriggerHistory audit = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());
        final IDataExtractor dataExtractor = ctx != null ? ctx.getDataExtractor() : getDataExtractor(node
                .getSymmetricVersion());

        jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                try {
                    PreparedStatement st = null;
                    ResultSet rs = null;
                    try {
                        st = conn.prepareStatement(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY,
                                java.sql.ResultSet.CONCUR_READ_ONLY);
                        st.setFetchSize(dbDialect.getStreamingResultsFetchSize());
                        rs = st.executeQuery();
                        final BufferedWriter writer = transport.open();
                        final DataExtractorContext ctxCopy = ctx == null ? context.copy(dataExtractor) : ctx;
                        if (batch != null) {
                            dataExtractor.init(writer, ctxCopy);
                            dataExtractor.begin(batch, writer);
                        }
                        while (rs.next()) {
                            dataExtractor.write(writer, new Data(0, null, rs.getString(1), DataEventType.INSERT, audit
                                    .getSourceTableName(), null, audit), ctxCopy);
                        }
                        if (batch != null) {
                            dataExtractor.commit(batch, writer);
                        }
                    } finally {
                        JdbcUtils.closeResultSet(rs);
                        JdbcUtils.closeStatement(st);
                    }
                    return null;
                } catch (Exception e) {
                    throw new RuntimeException("Error during SQL: " + sql, e);
                }
            }
        });
    }

    public boolean extract(Node node, IOutgoingTransport transport) throws Exception {
        IDataExtractor dataExtractor = getDataExtractor(node.getSymmetricVersion());
        ExtractStreamHandler handler = new ExtractStreamHandler(dataExtractor, transport);
        return extract(node, handler);
    }

    /**
     * Allow a handler callback to do the work so we can route the extracted
     * data to other types of handlers for processing.
     */
    public boolean extract(Node node, final IExtractListener handler) throws Exception {

        List<NodeChannel> channels = configurationService.getChannels();

        for (NodeChannel nodeChannel : channels) {
            outgoingBatchService.buildOutgoingBatches(node.getNodeId(), nodeChannel);
        }

        List<OutgoingBatch> batches = outgoingBatchService.getOutgoingBatches(node.getNodeId());

        if (batches != null && batches.size() > 0) {
            OutgoingBatchHistory history = null;
            try {
                handler.init();
                for (final OutgoingBatch batch : batches) {
                    history = new OutgoingBatchHistory(batch);
                    handler.startBatch(batch);
                    selectEventDataToExtract(handler, batch);
                    handler.endBatch(batch);
                    history.setStatus(OutgoingBatchHistory.Status.SE);
                    history.setEndTime(new Date());
                    outgoingBatchService.insertOutgoingBatchHistory(history);
                }
            } catch (RuntimeException e) {
                SQLException se = unwrapSqlException(e);
                if (history != null) {
                    if (se != null) {
                        history.setSqlState(se.getSQLState());
                        history.setSqlCode(se.getErrorCode());
                        history.setSqlMessage(se.getMessage());
                    } else {
                        history.setSqlMessage(e.getMessage());
                    }
                    history.setStatus(OutgoingBatchHistory.Status.SE);
                    history.setEndTime(new Date());
                    outgoingBatchService.setBatchStatus(history.getBatchId(), Status.ER);
                    outgoingBatchService.insertOutgoingBatchHistory(history);
                } else {
                    logger.error(
                            "Could not log the outgoing batch status because the batch history has not been created.",
                            e);
                }
                throw e;
            } finally {
                handler.done();
            }
            return true;
        }
        return false;
    }
    
    public boolean extractBatchRange(IOutgoingTransport transport, String startBatchId, String endBatchId)
            throws Exception {
        IDataExtractor dataExtractor = getDataExtractor(null);
        ExtractStreamHandler handler = new ExtractStreamHandler(dataExtractor, transport);
        return extractBatchRange(handler, startBatchId, endBatchId);
    }

    public boolean extractBatchRange(final IExtractListener handler, String startBatchId, String endBatchId)
            throws Exception {

        List<OutgoingBatch> batches = outgoingBatchService.getOutgoingBatchRange(startBatchId, endBatchId);

        if (batches != null && batches.size() > 0) {
            try {
                handler.init();
                for (final OutgoingBatch batch : batches) {
                    handler.startBatch(batch);
                    selectEventDataToExtract(handler, batch);
                    handler.endBatch(batch);
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
                PreparedStatement ps = conn.prepareStatement(getSql("selectEventDataToExtractSql"),
                        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                ps.setFetchSize(dbDialect.getStreamingResultsFetchSize());
                ps.setString(1, batch.getNodeId());
                ps.setLong(2, batch.getBatchId());
                ResultSet rs = ps.executeQuery();
                try {
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
        String oldData = results.getString(6);
        Date created = results.getDate(7);
        TriggerHistory audit = configurationService.getHistoryRecordFor(results.getInt(8));
        Data data = new Data(dataId, pk, rowData, eventType, tableName, created, audit);
        data.setOldData(oldData);
        return data;
    }

    public void setOutgoingBatchService(IOutgoingBatchService batchBuilderService) {
        this.outgoingBatchService = batchBuilderService;
    }

    public void setContext(DataExtractorContext context) {
        this.context = context;
    }

    public void setDbDialect(IDbDialect dialect) {
        this.dbDialect = dialect;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    class ExtractStreamHandler implements IExtractListener {

        IOutgoingTransport transport;

        IDataExtractor dataExtractor;

        DataExtractorContext context;

        BufferedWriter writer;

        ExtractStreamHandler(IDataExtractor dataExtractor, IOutgoingTransport transport) throws Exception {
            this.transport = transport;
            this.dataExtractor = dataExtractor;
        }

        public void dataExtracted(Data data) throws Exception {
            if (extractorFilters != null) {
                for (IExtractorFilter filter : extractorFilters) {
                    if (!filter.filterData(data, context)) {
                        // short circuit the extract if instructed
                        return;
                    }
                }
            }
            dataExtractor.write(writer, data, context);
        }

        public void done() throws IOException {
        }

        public void endBatch(OutgoingBatch batch) throws Exception {
            dataExtractor.commit(batch, writer);
        }

        public void init() throws Exception {
            this.writer = transport.open();
            this.context = DataExtractorService.this.context.copy(dataExtractor);
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

    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }

    public void addExtractorFilter(IExtractorFilter extractorFilter) {
        if (this.extractorFilters == null) {
            this.extractorFilters = new ArrayList<IExtractorFilter>();
        }
        this.extractorFilters.add(extractorFilter);
    }

    public void setExtractorFilters(List<IExtractorFilter> extractorFilters) {
        this.extractorFilters = extractorFilters;
    }

}
