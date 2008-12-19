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
import java.io.OutputStream;
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
import org.jumpmind.symmetric.extract.csv.Util;
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
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IExtractListener;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.TransportUtils;
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

    private IAcknowledgeService acknowledgeService;

    private IDbDialect dbDialect;

    private BeanFactory beanFactory;

    private DataExtractorContext context;

    private List<IExtractorFilter> extractorFilters;

    /**
     * @see DataExtractorService#extractConfigurationStandalone(Node, BufferedWriter)
     */
    public void extractConfigurationStandalone(Node node, OutputStream out) throws IOException {
        this.extractConfigurationStandalone(node, TransportUtils.toWriter(out));
    }

    /**
     * Extract the SymmetricDS configuration for the passed in {@link Node}.  Note that this method will
     * insert an already acknowledged batch to indicate that the configuration was sent.  If the configuration 
     * fails to load for some reason on the client the batch status will NOT reflect the failure.
     */
    public void extractConfigurationStandalone(Node node, BufferedWriter writer) throws IOException {

        try {
            OutgoingBatch batch = new OutgoingBatch(node, Constants.CHANNEL_CONFIG, BatchType.INITIAL_LOAD);
            outgoingBatchService.insertOutgoingBatch(batch);
            OutgoingBatchHistory history = new OutgoingBatchHistory(batch);

            final IDataExtractor dataExtractor = getDataExtractor(node.getSymmetricVersion());
            final DataExtractorContext ctxCopy = context.copy(dataExtractor);

            dataExtractor.init(writer, ctxCopy);
            dataExtractor.begin(batch, writer);

            extractConfiguration(node, writer, ctxCopy);

            dataExtractor.commit(batch, writer);

            history.setStatus(OutgoingBatchHistory.Status.SE);
            history.setEndTime(new Date());
            outgoingBatchService.insertOutgoingBatchHistory(history);

            // acknowledge right away, because the acknowledgment is not
            // built into the registration protocol.
            acknowledgeService.ack(batch.getBatchInfo());

        } finally {
            writer.flush();
        }
    }

    public void extractConfiguration(Node node, BufferedWriter writer, DataExtractorContext ctx) throws IOException {
        List<Trigger> triggers = configurationService.getConfigurationTriggers(parameterService.getNodeGroupId(), node
                .getNodeGroupId());
        for (int i = triggers.size() - 1; i >= 0; i--) {
            Trigger trigger = triggers.get(i);
            String sql = dbDialect.createPurgeSqlFor(node, trigger, null);
            Util.writeSql(sql, writer);
        }

        for (int i = 0; i < triggers.size(); i++) {
            Trigger trigger = triggers.get(i);
            TriggerHistory hist = new TriggerHistory(dbDialect.getMetaDataFor(trigger, false), trigger);
            hist.setTriggerHistoryId(i);
            if (!trigger.getSourceTableName().endsWith("_node_identity")) {
                writeInitialLoad(node, trigger, hist, writer, null, ctx);
            } else {
                Data data = new Data(1, null, node.getNodeId(), DataEventType.INSERT, trigger.getSourceTableName(),
                        null, hist);
                ctx.getDataExtractor().write(writer, data, ctx);
            }
        }
    }

    private IDataExtractor getDataExtractor(String version) {
        String beanName = Constants.DATA_EXTRACTOR;
        // Version 1.4.1-appaji accepts "old" token, so it's like a 1.5 version
        if (version != null) {
            int[] versions = Version.parseVersion(version);
            if (versions[0] == 1) {
                if (versions[1] <= 2) {
                    beanName += "10";
                } else if (versions[1] <= 3) {
                    beanName += "13";
                } else if (versions[1] <= 4 && !version.equals("1.4.1-appaji")) {
                    beanName += "14";
                }
            }
        }
        return (IDataExtractor) beanFactory.getBean(beanName);
    }

    public OutgoingBatch extractInitialLoadFor(Node node, Trigger trigger, BufferedWriter writer) {

        OutgoingBatch batch = new OutgoingBatch(node, trigger.getChannelId(), BatchType.INITIAL_LOAD);
        outgoingBatchService.insertOutgoingBatch(batch);
        OutgoingBatchHistory history = new OutgoingBatchHistory(batch);
        writeInitialLoad(node, trigger, writer, batch, null);
        history.setStatus(OutgoingBatchHistory.Status.SE);
        history.setEndTime(new Date());
        outgoingBatchService.insertOutgoingBatchHistory(history);
        return batch;
    }

    public void extractInitialLoadWithinBatchFor(Node node, final Trigger trigger, BufferedWriter writer,
            DataExtractorContext ctx) {
        writeInitialLoad(node, trigger, writer, null, ctx);
    }

    protected void writeInitialLoad(Node node, Trigger trigger, BufferedWriter writer, final OutgoingBatch batch,
            final DataExtractorContext ctx) {
        writeInitialLoad(node, trigger, configurationService.getLatestHistoryRecordFor(trigger.getTriggerId()), writer,
                batch, ctx);
    }

    /**
     * 
     * @param node
     * @param trigger
     * @param audit
     * @param transport
     * @param batch
     *                If null, then assume this 'initial load' is part of
     *                another batch.
     * @param ctx
     */
    protected void writeInitialLoad(Node node, final Trigger trigger, final TriggerHistory audit,
            final BufferedWriter writer, final OutgoingBatch batch, final DataExtractorContext ctx) {

        final String sql = dbDialect.createInitalLoadSqlFor(node, trigger);

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

    public void setAcknowledgeService(IAcknowledgeService acknowledgeService) {
        this.acknowledgeService = acknowledgeService;
    }

}
