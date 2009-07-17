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
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.extract.IDataExtractor;
import org.jumpmind.symmetric.extract.IExtractorFilter;
import org.jumpmind.symmetric.extract.csv.Util;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.BatchType;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.DataMetaData;
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
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IExtractListener;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IRoutingService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.TransportUtils;
import org.jumpmind.symmetric.transport.file.FileOutgoingTransport;
import org.jumpmind.symmetric.upgrade.UpgradeConstants;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.support.JdbcUtils;

public class DataExtractorService extends AbstractService implements IDataExtractorService, BeanFactoryAware {

    protected static final Log logger = LogFactory.getLog(DataExtractorService.class);

    private IOutgoingBatchService outgoingBatchService;
    
    private IRoutingService routingService;
    
    private IDataService dataService;

    private IConfigurationService configurationService;

    private IAcknowledgeService acknowledgeService;

    private INodeService nodeService;

    private IDbDialect dbDialect;

    private BeanFactory beanFactory;

    private DataExtractorContext clonableContext;

    private List<IExtractorFilter> extractorFilters;

    /**
     * @see DataExtractorService#extractConfigurationStandalone(Node,
     *      BufferedWriter)
     */
    public void extractConfigurationStandalone(Node node, OutputStream out) throws IOException {
        this.extractConfigurationStandalone(node, TransportUtils.toWriter(out));
    }

    /**
     * Extract the SymmetricDS configuration for the passed in {@link Node}.
     * Note that this method will insert an already acknowledged batch to
     * indicate that the configuration was sent. If the configuration fails to
     * load for some reason on the client the batch status will NOT reflect the
     * failure.
     */
    public void extractConfigurationStandalone(Node node, BufferedWriter writer) throws IOException {
        try {
            OutgoingBatch batch = new OutgoingBatch(node.getNodeId(), Constants.CHANNEL_CONFIG, BatchType.INITIAL_LOAD);
            if (Version.isOlderThanVersion(node.getSymmetricVersion(),
                    UpgradeConstants.VERSION_FOR_NEW_REGISTRATION_PROTOCOL)) {
                outgoingBatchService.insertOutgoingBatch(batch);
                OutgoingBatchHistory history = new OutgoingBatchHistory(batch);
                history.setStatus(OutgoingBatchHistory.Status.SE);
                history.setEndTime(new Date());
                outgoingBatchService.insertOutgoingBatchHistory(history);

                // acknowledge right away, because the acknowledgment is not
                // built into the registration protocol.
                acknowledgeService.ack(batch.getBatchInfo());
            } else {
                batch.setBatchId(BatchInfo.VIRTUAL_BATCH_FOR_REGISTRATION);
            }

            final IDataExtractor dataExtractor = getDataExtractor(node.getSymmetricVersion());
            final DataExtractorContext ctxCopy = clonableContext.copy(dataExtractor);

            dataExtractor.init(writer, ctxCopy);
            dataExtractor.begin(batch, writer);

            extractConfiguration(node, writer, ctxCopy);

            dataExtractor.commit(batch, writer);

        } finally {
            writer.flush();
        }
    }

    public void extractConfiguration(Node node, BufferedWriter writer, DataExtractorContext ctx) throws IOException {
        List<Trigger> triggers = configurationService.getRegistrationTriggers(parameterService.getNodeGroupId(), node
                .getNodeGroupId());
        if (node.isVersionGreaterThanOrEqualTo(1, 5, 0)) {
            for (int i = triggers.size() - 1; i >= 0; i--) {
                Trigger trigger = triggers.get(i);
                StringBuilder sql = new StringBuilder(dbDialect.createPurgeSqlFor(node, trigger, null));
                addPurgeCriteriaToConfigurationTables(trigger.getSourceTableName(), sql);
                Util.writeSql(sql.toString(), writer);
            }
        }

        for (int i = 0; i < triggers.size(); i++) {
            Trigger trigger = triggers.get(i);
            TriggerHistory hist = new TriggerHistory(dbDialect.getMetaDataFor(trigger, false), trigger);
            hist.setTriggerHistoryId(Integer.MAX_VALUE - i);
            if (!trigger.getSourceTableName().endsWith(TableConstants.SYM_NODE_IDENTITY)) {
                writeInitialLoad(node, trigger, hist, writer, null, ctx);
            } else {
                Data data = new Data(1, null, node.getNodeId(), DataEventType.INSERT, trigger.getSourceTableName(),
                        null, hist, null, null);
                ctx.getDataExtractor().write(writer, data, ctx);
            }
        }

        if (triggers.size() == 0) {
            logger.error(node + " attempted registration, but was sent an empty configuration.");
        }
    }

    private void addPurgeCriteriaToConfigurationTables(String sourceTableName, StringBuilder sql) {
        if ((TableConstants.getTableName(dbDialect.getTablePrefix(), TableConstants.SYM_NODE)
                .equalsIgnoreCase(sourceTableName))
                || TableConstants.getTableName(dbDialect.getTablePrefix(), TableConstants.SYM_NODE_SECURITY)
                        .equalsIgnoreCase(sourceTableName)) {
            Node me = nodeService.findIdentity();
            if (me != null) {
                sql.append(String.format(" where created_at_node_id='%s'", me.getNodeId()));
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
        OutgoingBatch batch = new OutgoingBatch(node.getNodeId(), trigger.getChannelId(), BatchType.INITIAL_LOAD);
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
     * @param hist
     * @param transport
     * @param batch
     *            If null, then assume this 'initial load' is part of another
     *            batch.
     * @param ctx
     */
    protected void writeInitialLoad(final Node node, final Trigger trigger, final TriggerHistory hist,
            final BufferedWriter writer, final OutgoingBatch batch, final DataExtractorContext ctx) {
        final String sql = dbDialect.createInitalLoadSqlFor(node, trigger);

        final IDataExtractor dataExtractor = ctx != null ? ctx.getDataExtractor() : getDataExtractor(node
                .getSymmetricVersion());

        jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                try {
                    Table table = dbDialect.getMetaDataFor(trigger, true);
                    NodeChannel channel = batch != null ? configurationService.getChannel(batch.getChannelId()) : new NodeChannel(Constants.CHANNEL_RELOAD);
                    Set<Node> oneNodeSet = new HashSet<Node>();                    
                    oneNodeSet.add(node);
                    PreparedStatement st = null;
                    ResultSet rs = null;
                    try {
                        st = conn.prepareStatement(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY,
                                java.sql.ResultSet.CONCUR_READ_ONLY);
                        st.setFetchSize(dbDialect.getStreamingResultsFetchSize());
                        rs = st.executeQuery();
                        final DataExtractorContext ctxCopy = ctx == null ? clonableContext.copy(dataExtractor) : ctx;
                        if (batch != null) {
                            dataExtractor.init(writer, ctxCopy);
                            dataExtractor.begin(batch, writer);
                        }
                        while (rs.next()) {
                            Data data = new Data(0, null, rs.getString(1), DataEventType.INSERT, hist
                                    .getSourceTableName(), null, hist, null, null);  
                            DataMetaData dataMetaData = new DataMetaData(data, table, trigger, channel);
                            if (routingService.shouldDataBeRouted(dataMetaData, oneNodeSet, true)) {
                                dataExtractor.write(writer, data, ctxCopy);
                            }
                        }
                        if (batch != null) {
                            dataExtractor.commit(batch, writer);
                        }
                    } finally {
                        JdbcUtils.closeResultSet(rs);
                        JdbcUtils.closeStatement(st);
                    }
                    return null;
                } catch (SQLException e) {
                    throw new RuntimeException(e.getSQLState() + "Error during SQL: " + sql, e);
                } catch (Exception e) {
                    throw new RuntimeException("Error during SQL: " + sql, e);
                }
            }
        });
    }

    public boolean extract(Node node, IOutgoingTransport targetTransport) throws IOException {
        IDataExtractor dataExtractor = getDataExtractor(node.getSymmetricVersion());

        if (!parameterService.is(ParameterConstants.START_ROUTE_JOB)) {
            routingService.routeData();
        }

        List<OutgoingBatch> batches = outgoingBatchService.getOutgoingBatches(node.getNodeId());
        if (batches != null && batches.size() > 0) {
            FileOutgoingTransport fileTransport = null;

            try {
                if (parameterService.is(ParameterConstants.STREAM_TO_FILE_ENABLED)) {
                    fileTransport = new FileOutgoingTransport(parameterService
                            .getLong(ParameterConstants.STREAM_TO_FILE_THRESHOLD), "extract");
                }

                ExtractStreamHandler handler = new ExtractStreamHandler(dataExtractor,
                        fileTransport != null ? fileTransport : targetTransport);

                databaseExtract(node, batches, handler);

                networkTransfer(fileTransport, targetTransport);
            } finally {
                if (fileTransport != null) {
                    fileTransport.close();
                }
            }

            return true;
        } else {
            return false;
        }
    }

    protected void networkTransfer(FileOutgoingTransport fileTransport, IOutgoingTransport targetTransport)
            throws IOException {
        if (fileTransport != null) {
            fileTransport.close();
            Reader reader = null;
            try {
                reader = fileTransport.getReader();
                IOUtils.copy(reader, targetTransport.open());
            } finally {
                IOUtils.closeQuietly(reader);
                fileTransport.delete();
            }
        }
    }

    /**
     * Allow a handler callback to do the work so we can route the extracted
     * data to other types of handlers for processing.
     */
    protected void databaseExtract(Node node, List<OutgoingBatch> batches, final IExtractListener handler) throws IOException {
        OutgoingBatchHistory history = null;
        try {
            boolean initialized = false;
            for (final OutgoingBatch batch : batches) {
                history = new OutgoingBatchHistory(batch);
                long ts = System.currentTimeMillis();
                if (!initialized) {
                    handler.init();
                    initialized = true;
                }
                handler.startBatch(batch);
                selectEventDataToExtract(handler, batch);
                handler.endBatch(batch);
                history.setDatabaseMillis(System.currentTimeMillis() - ts);
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
                logger.error("Could not log the outgoing batch status because the batch history has not been created.",
                        e);
            }
            throw e;
        } finally {
            handler.done();
        }

    }

    public boolean extractBatchRange(IOutgoingTransport transport, String startBatchId, String endBatchId)
            throws IOException {
        IDataExtractor dataExtractor = getDataExtractor(null);
        ExtractStreamHandler handler = new ExtractStreamHandler(dataExtractor, transport);
        return extractBatchRange(handler, startBatchId, endBatchId);
    }
    
    private boolean areNumeric(String... data) {
        if (data != null) {
            for (String string : data) {
                try {
                    Long.parseLong(string);
                } catch (NumberFormatException e) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean extractBatchRange(final IExtractListener handler,
            String startBatchId, String endBatchId) throws IOException {
        if (areNumeric(startBatchId, endBatchId)) {
            List<OutgoingBatch> batches = outgoingBatchService
                    .getOutgoingBatchRange(startBatchId, endBatchId);

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
                            handler.dataExtracted(dataService.readData(rs));
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

    public void setOutgoingBatchService(IOutgoingBatchService batchBuilderService) {
        this.outgoingBatchService = batchBuilderService;
    }

    public void setContext(DataExtractorContext context) {
        this.clonableContext = context;
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

        ExtractStreamHandler(IDataExtractor dataExtractor, IOutgoingTransport transport) throws IOException {
            this.transport = transport;
            this.dataExtractor = dataExtractor;
        }

        public void dataExtracted(Data data) throws IOException {
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

        public void endBatch(OutgoingBatch batch) throws IOException {
            dataExtractor.commit(batch, writer);
        }

        public void init() throws IOException {
            this.writer = transport.open();
            this.context = DataExtractorService.this.clonableContext.copy(dataExtractor);
            dataExtractor.init(writer, context);
        }

        public void startBatch(OutgoingBatch batch) throws IOException {
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

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }
    
    public void setRoutingService(IRoutingService routingService) {
        this.routingService = routingService;
    }

    public void setDataService(IDataService dataService) {
        this.dataService = dataService;
    }
}
