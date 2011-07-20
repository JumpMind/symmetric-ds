/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */
package org.jumpmind.symmetric.service.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.csv.CsvReader;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.extract.IDataExtractor;
import org.jumpmind.symmetric.extract.IExtractorFilter;
import org.jumpmind.symmetric.io.ThresholdFileWriter;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.route.SimpleRouterContext;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IExtractListener;
import org.jumpmind.symmetric.service.IModelRetrievalHandler;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.statistic.DataExtractorStatisticsWriter;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticConstants;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.TransportUtils;
import org.jumpmind.symmetric.upgrade.UpgradeConstants;
import org.jumpmind.symmetric.util.CsvUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.support.JdbcUtils;

/**
 * @see IDataExtractorService
 */
public class DataExtractorService extends AbstractService implements IDataExtractorService, BeanFactoryAware {

    final static long MS_PASSED_BEFORE_BATCH_REQUERIED = 5000;
    
    private IOutgoingBatchService outgoingBatchService;

    private IRouterService routingService;

    private IDataService dataService;

    private IConfigurationService configurationService;

    private IAcknowledgeService acknowledgeService;

    private ITriggerRouterService triggerRouterService;

    private INodeService nodeService;

    private BeanFactory beanFactory;

    private DataExtractorContext clonableContext;

    private List<IExtractorFilter> extractorFilters;
    
    private IStatisticManager statisticManager;
    
    private Map<Long, File> extractedBatchesHandle = new HashMap<Long, File>();

    /**
     * @see DataExtractorService#extractConfigurationStandalone(Node,
     *      Writer)
     */
    public void extractConfigurationStandalone(Node node, OutputStream out, String... tablesToExclude) throws IOException {
        this.extractConfigurationStandalone(node, TransportUtils.toWriter(out), tablesToExclude);
    }

    /**
     * Extract the SymmetricDS configuration for the passed in {@link Node}.
     * Note that this method will insert an already acknowledged batch to
     * indicate that the configuration was sent. If the configuration fails to
     * load for some reason on the client the batch status will NOT reflect the
     * failure.
     */
    public void extractConfigurationStandalone(Node node, Writer writer, String... tablesToExclude) throws IOException {
        try {
            OutgoingBatch batch = new OutgoingBatch(node.getNodeId(), Constants.CHANNEL_CONFIG, Status.NE);
            if (Version.isOlderThanVersion(node.getSymmetricVersion(),
                    UpgradeConstants.VERSION_FOR_NEW_REGISTRATION_PROTOCOL)) {
                outgoingBatchService.insertOutgoingBatch(batch);
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

            extractConfiguration(node, writer, ctxCopy, tablesToExclude);

            dataExtractor.commit(batch, writer);

        } finally {
            writer.flush();
        }
    }

    public void extractConfiguration(Node node, Writer writer, DataExtractorContext ctx, String... tablesToExclude) throws IOException {
        NodeGroupLink nodeGroupLink = new NodeGroupLink(parameterService
                .getNodeGroupId(), node.getNodeGroupId());
        List<TriggerRouter> triggerRouters = triggerRouterService.getTriggerRoutersForRegistration(StringUtils.isBlank(node
                .getSymmetricVersion()) ? Version.version() : node.getSymmetricVersion(), nodeGroupLink, tablesToExclude);
        final IDataExtractor dataExtractor = ctx != null ? ctx.getDataExtractor() : getDataExtractor(node
                .getSymmetricVersion());
        
        if (node.isVersionGreaterThanOrEqualTo(1, 5, 0)) {
            for (int i = triggerRouters.size() - 1; i >= 0; i--) {
                TriggerRouter triggerRouter = triggerRouters.get(i);
                TriggerHistory triggerHistory = triggerRouterService.getNewestTriggerHistoryForTrigger(triggerRouter.getTrigger().getTriggerId());
                if (triggerHistory == null) {
                    triggerHistory = new TriggerHistory(dbDialect.getTable(triggerRouter.getTrigger(),
                            false), triggerRouter.getTrigger());
                    triggerHistory.setTriggerHistoryId(Integer.MAX_VALUE - i);
                }
                StringBuilder sql = new StringBuilder(dbDialect.createPurgeSqlFor(node, triggerRouter));
                addPurgeCriteriaToConfigurationTables(triggerRouter.getTrigger().getSourceTableName(), sql);
                String sourceTable = triggerHistory.getSourceTableName();
                Data data = new Data(1, null, sql.toString(), DataEventType.SQL, sourceTable,
                        null, triggerHistory, triggerRouter.getTrigger().getChannelId(), null,
                        null);
                dataExtractor.write(writer, data, triggerRouter.getRouter().getRouterId(), ctx);
            }
        }

        for (int i = 0; i < triggerRouters.size(); i++) {
            TriggerRouter triggerRouter = triggerRouters.get(i);
            TriggerHistory triggerHistory = triggerRouterService.getNewestTriggerHistoryForTrigger(triggerRouter.getTrigger().getTriggerId());
            if (triggerHistory == null) {
                triggerHistory = new TriggerHistory(dbDialect.getTable(triggerRouter.getTrigger(),
                        false), triggerRouter.getTrigger());
                triggerHistory.setTriggerHistoryId(Integer.MAX_VALUE - i);
            }
            
            if (!triggerRouter.getTrigger().getSourceTableName().endsWith(TableConstants.SYM_NODE_IDENTITY)) {
                writeInitialLoad(node, triggerRouter, triggerHistory, writer, ctx);
            } else {
                Data data = new Data(1, null, node.getNodeId(), DataEventType.INSERT, triggerHistory.getSourceTableName(), null, triggerHistory, triggerRouter.getTrigger().getChannelId(), null,
                        null);
                dataExtractor.write(writer, data, triggerRouter.getRouter().getRouterId(), ctx);
            }
        }

        if (triggerRouters.size() == 0) {
            log.error("RegistrationEmpty", node);
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
                } else if (versions[1] <= 7) {
                    beanName += "16";
                }
            }
        }
        return (IDataExtractor) beanFactory.getBean(beanName);
    }

    public void extractInitialLoadWithinBatchFor(Node node, final TriggerRouter trigger, Writer writer,
            DataExtractorContext ctx, TriggerHistory triggerHistory) {
        writeInitialLoad(node, trigger, triggerHistory, writer, ctx);
    }

    /**
     * @param batch
     *            If null, then assume this 'initial load' is part of another
     *            batch.
     */
    protected void writeInitialLoad(final Node node, final TriggerRouter triggerRouter, TriggerHistory triggerHistory,
            final Writer writer, final DataExtractorContext ctx) {

        triggerHistory = triggerHistory != null ? triggerHistory : triggerRouterService.getNewestTriggerHistoryForTrigger(triggerRouter.getTrigger()
                .getTriggerId());
        
        final boolean newExtractorCreated = ctx == null || ctx.getDataExtractor() == null;
        final IDataExtractor dataExtractor = !newExtractorCreated ? ctx.getDataExtractor() : getDataExtractor(node
                .getSymmetricVersion());

        // The table to use for the SQL may be different than the configured table if there is a 
        // legacy table that is swapped out by the dataExtractor.
        Table tableForSql = dbDialect.getTable(triggerRouter.getTrigger().getSourceCatalogName(), 
                triggerRouter.getTrigger().getSourceSchemaName(),
        		dataExtractor.getLegacyTableName(triggerRouter.getTrigger().getSourceTableName()), true);
        
        final String sql = dbDialect.createInitialLoadSqlFor(node, triggerRouter, tableForSql, triggerHistory,configurationService.getChannel(triggerRouter.getTrigger().getChannelId()));
        
        log.debug("Sql",sql);
        
        if (!tableForSql.getName().equals(triggerHistory.getSourceTableName())) {
        	// This is to make legacy tables backwards compatible
        	String tableName = triggerHistory.getSourceTableName();
        	triggerHistory = new TriggerHistory(tableForSql, triggerRouter.getTrigger());
        	triggerHistory.setSourceTableName(tableName);
        }
        
        final TriggerHistory triggerHistory2Use = triggerHistory;
        
        jdbcTemplate.execute(new ConnectionCallback<Object>() {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                try {
                    OutgoingBatch batch = ctx.getBatch();
                    Table table = dbDialect.getTable(triggerRouter.getTrigger(), true);
                    NodeChannel channel = batch != null ? configurationService.getNodeChannel(batch.getChannelId(), false)
                            : new NodeChannel(triggerRouter.getTrigger().getChannelId());
                    Set<Node> oneNodeSet = new HashSet<Node>();
                    oneNodeSet.add(node);
                    
                    boolean autoCommitFlag = conn.getAutoCommit(); 
                    PreparedStatement st = null;
                    ResultSet rs = null;
                    try {
                        
                        if (dbDialect.requiresAutoCommitFalseToSetFetchSize()) {
                            conn.setAutoCommit(false);
                        }
                        
                        st = conn.prepareStatement(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY,
                                java.sql.ResultSet.CONCUR_READ_ONLY);
                        st.setQueryTimeout(jdbcTemplate.getQueryTimeout());
                        st.setFetchSize(dbDialect.getStreamingResultsFetchSize());
                        long ts = System.currentTimeMillis();
                        rs = st.executeQuery();
                        long executeTimeInMs = System.currentTimeMillis()-ts;
                        if (executeTimeInMs > Constants.LONG_OPERATION_THRESHOLD) {
                            log.warn("LongRunningOperation", "selecting initial load to extract for batch " + batch.getBatchId(), executeTimeInMs);
                        }
                        final DataExtractorContext ctxCopy = ctx == null ? clonableContext.copy(dataExtractor) : ctx;
                        if (newExtractorCreated) {
                            dataExtractor.init(writer, ctxCopy);
                            dataExtractor.begin(batch, writer);
                        }
                        SimpleRouterContext routingContext = new SimpleRouterContext(node.getNodeId(), jdbcTemplate,
                                channel);
                        int dataNotRouted = 0;
                        int dataRouted = 0;
                        ts = System.currentTimeMillis();
                        while (rs.next()) {                        	
                            Data data = new Data(0, null, rs.getString(1), DataEventType.INSERT, triggerHistory2Use
                                    .getSourceTableName(), null, triggerHistory2Use, triggerRouter.getTrigger().getChannelId(), null, null);
                            DataMetaData dataMetaData = new DataMetaData(data, table, triggerRouter, channel);
                            if (!StringUtils.isBlank(triggerRouter.getInitialLoadSelect()) || 
                                    routingService.shouldDataBeRouted(routingContext, dataMetaData, oneNodeSet, true)) {
                                dataExtractor.write(writer, data, triggerRouter.getRouter().getRouterId(), ctxCopy);
                                dataRouted++;
                            } else {
                                dataNotRouted++;
                            }
                            
                            executeTimeInMs = System.currentTimeMillis()-ts;
                            if (executeTimeInMs >  DateUtils.MILLIS_PER_MINUTE * 10) {
                                log.warn("LongRunningOperation", "initial load extracted " + (dataRouted+dataNotRouted) + " data so far for batch " + batch.getBatchId(), executeTimeInMs);
                                ts = System.currentTimeMillis();
                            }

                        }
                        if (dataNotRouted > 0) {
                            log.info("RouterInitialLoadNotRouted",dataNotRouted, triggerRouter.getTrigger().getSourceTableName());
                        }
                        if (newExtractorCreated) {
                            dataExtractor.commit(batch, writer);
                        }
                    } finally {
                        if (dbDialect.requiresAutoCommitFalseToSetFetchSize()) {
                            conn.commit();
                            conn.setAutoCommit(autoCommitFlag); 
                        }
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
    
    private ExtractStreamHandler createExtractStreamHandler(IDataExtractor dataExtractor, Writer extractWriter) throws IOException {
        return new ExtractStreamHandler(dataExtractor,
                new DataExtractorStatisticsWriter(statisticManager, extractWriter,
                        StatisticConstants.FLUSH_SIZE_BYTES,
                        StatisticConstants.FLUSH_SIZE_LINES));
    }
    
    private List<OutgoingBatch> filterBatchesForExtraction(OutgoingBatches batches,
            ChannelMap suspendIgnoreChannelsList) {

        // We now have either our local suspend/ignore list, or the combined
        // remote send/ignore list and our local list (along with a
        // reservation, if we go this far...)

        // Now, we need to skip the suspended channels and ignore the
        // ignored ones by ultimately setting the status to ignored and
        // updating them.

        List<OutgoingBatch> ignoredBatches = batches
                .filterBatchesForChannels(suspendIgnoreChannelsList.getIgnoreChannels());

        // Finally, update the ignored outgoing batches such that they
        // will be skipped in the future.
        for (OutgoingBatch batch : ignoredBatches) {
            batch.setStatus(OutgoingBatch.Status.IG);
            if (log.isDebugEnabled()) {
                log.debug("BatchIgnored", batch.getBatchId());
            }
        }

        outgoingBatchService.updateOutgoingBatches(ignoredBatches);

        batches.filterBatchesForChannels(suspendIgnoreChannelsList.getSuspendChannels());

        // Remove non-load batches so that an initial load finishes before
        // any other batches are loaded.
        if (!batches.containsBatchesInError() && batches.containsLoadBatches()) {
            batches.removeNonLoadBatches();
        }

        return batches.getBatches();
    }

    public List<OutgoingBatch> extract(Node node, IOutgoingTransport targetTransport)
            throws IOException {

        List<OutgoingBatch> activeBatches = null;

        IDataExtractor dataExtractor = getDataExtractor(node.getSymmetricVersion());

        if (!parameterService.is(ParameterConstants.START_ROUTE_JOB)) {
            routingService.routeData();
        }

        OutgoingBatches batches = outgoingBatchService.getOutgoingBatches(node);
        long batchesSelectedAtMs = System.currentTimeMillis();

        if (batches.containsBatches()) {

            activeBatches = filterBatchesForExtraction(batches,
                    targetTransport.getSuspendIgnoreChannelLists(configurationService));

            if (activeBatches.size() > 0) {

                Writer extractWriter = null;
                BufferedWriter networkWriter = null;

                boolean streamToFileEnabled = parameterService
                        .is(ParameterConstants.STREAM_TO_FILE_ENABLED);

                ThresholdFileWriter fileWriter = new ThresholdFileWriter(
                        parameterService.getLong(ParameterConstants.STREAM_TO_FILE_THRESHOLD),
                        "extract");

                if (streamToFileEnabled) {
                    extractWriter = new BufferedWriter(fileWriter);
                    networkWriter = targetTransport.open();
                } else {
                    extractWriter = targetTransport.open();
                }

                ExtractStreamHandler handler = createExtractStreamHandler(dataExtractor,
                        extractWriter);

                handler.init();

                OutgoingBatch currentBatch = null;
                try {
                    final long maxBytesToSync = parameterService
                            .getLong(ParameterConstants.TRANSPORT_MAX_BYTES_TO_SYNC);
                    long bytesSentCount = 0;
                    int batchesSentCount = 0;
                    for (OutgoingBatch outgoingBatch : activeBatches) {
                        try {
                            if (System.currentTimeMillis() - batchesSelectedAtMs > MS_PASSED_BEFORE_BATCH_REQUERIED) {                                
                                outgoingBatch = outgoingBatchService.findOutgoingBatch(currentBatch
                                        .getBatchId());
                            }

                            currentBatch = outgoingBatch;

                            if (outgoingBatch.getStatus() != Status.OK) {
                                fileWriter.reset();
                                File previouslyExtracted = extractedBatchesHandle.get(currentBatch
                                        .getBatchId());
                                if (previouslyExtracted != null && previouslyExtracted.exists()) {
                                    log.info("DataExtractorUsingAlreadyExtractedBatch",
                                            currentBatch.getBatchId());
                                    fileWriter.setFile(previouslyExtracted);
                                } else {
                                    outgoingBatch.setStatus(OutgoingBatch.Status.QY);
                                    outgoingBatch
                                            .setExtractCount(outgoingBatch.getExtractCount() + 1);
                                    outgoingBatchService.updateOutgoingBatch(outgoingBatch);
                                    databaseExtract(node, outgoingBatch, handler, networkWriter);
                                }

                                if (System.currentTimeMillis()
                                        - outgoingBatch.getLastUpdatedTime().getTime() > MS_PASSED_BEFORE_BATCH_REQUERIED) {
                                    outgoingBatch = outgoingBatchService
                                            .findOutgoingBatch(currentBatch.getBatchId());
                                    currentBatch = outgoingBatch;
                                }
                                if (outgoingBatch.getStatus() != Status.OK) {
                                    outgoingBatch.setStatus(OutgoingBatch.Status.SE);
                                    outgoingBatch.setSentCount(outgoingBatch.getSentCount() + 1);
                                    outgoingBatchService.updateOutgoingBatch(outgoingBatch);

                                    File file = fileWriter.getFile();
                                    if (file != null) {
                                        extractedBatchesHandle.put(currentBatch.getBatchId(), file);
                                    }

                                    fileWriter.close();

                                    networkTransfer(fileWriter.getReader(), networkWriter);

                                    if (System.currentTimeMillis()
                                            - outgoingBatch.getLastUpdatedTime().getTime() > MS_PASSED_BEFORE_BATCH_REQUERIED) {
                                        outgoingBatch = outgoingBatchService
                                                .findOutgoingBatch(currentBatch.getBatchId());
                                        currentBatch = outgoingBatch;
                                    }
                                    if (outgoingBatch.getStatus() != Status.OK) {
                                        outgoingBatch.setStatus(OutgoingBatch.Status.LD);
                                        outgoingBatch
                                                .setLoadCount(outgoingBatch.getLoadCount() + 1);
                                        outgoingBatchService.updateOutgoingBatch(outgoingBatch);

                                        bytesSentCount += outgoingBatch.getByteCount();
                                        batchesSentCount++;

                                        if (bytesSentCount >= maxBytesToSync) {
                                            log.info("DataExtractorReachedMaxNumberOfBytesToSync",
                                                    batchesSentCount, bytesSentCount);
                                            break;
                                        }

                                    }
                                }

                            }

                            extractedBatchesHandle.remove(currentBatch.getBatchId());
                            fileWriter.delete();

                        } finally {
                            // It doesn't hurt anything to call close and delete
                            // a second time
                            fileWriter.close();

                        }
                    }
                } catch (Exception e) {
                    SQLException se = unwrapSqlException(e);
                    if (currentBatch != null) {
                        statisticManager.incrementDataExtractedErrors(currentBatch.getChannelId(),
                                1);
                        if (se != null) {
                            currentBatch.setSqlState(se.getSQLState());
                            currentBatch.setSqlCode(se.getErrorCode());
                            currentBatch.setSqlMessage(se.getMessage());
                        } else {
                            currentBatch.setSqlMessage(e.getMessage());
                        }
                        currentBatch.setStatus(OutgoingBatch.Status.ER);
                        currentBatch.setErrorFlag(true);
                        outgoingBatchService.updateOutgoingBatch(currentBatch);
                    } else {
                        log.error("BatchStatusLoggingFailed", e);
                    }

                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    } else if (e instanceof IOException) {
                        throw (IOException) e;
                    } else {
                        throw new RuntimeException(e);
                    }

                } finally {
                    handler.done();
                }

                // Next, we update the node channel controls to the current
                // timestamp
                Calendar now = Calendar.getInstance();

                for (NodeChannel nodeChannel : batches.getActiveChannels()) {
                    nodeChannel.setLastExtractedTime(now.getTime());
                    configurationService.saveNodeChannelControl(nodeChannel, false);
                }

            }
        }

        return activeBatches != null ? activeBatches : new ArrayList<OutgoingBatch>(0);
    }

    protected void networkTransfer(BufferedReader reader, BufferedWriter writer) throws IOException {        
        if (reader != null && writer != null) {
            CsvReader csvReader = CsvUtils.getCsvReader(reader);
            String channelId = null;
            long lineCount = 0;
            long byteCount = 0;
            try {
                String nextLine = null;
                while (csvReader.readRecord()) {
                    nextLine = csvReader.getRawRecord();
                    if (nextLine != null) {
                        lineCount++;
                        byteCount += nextLine.length();
                        if (nextLine.startsWith(CsvConstants.CHANNEL)) {
                            String[] csv = StringUtils.split(nextLine, ",");
                            if (csv.length > 1) {
                                if (channelId != null) {
                                    statisticManager.incrementDataBytesSent(channelId, byteCount);
                                    statisticManager.incrementDataSent(channelId, lineCount);
                                }
                                channelId = csv[1].trim();
                                byteCount = 0;
                                lineCount = 0;
                            }
                        }
                        writer.write(nextLine);
                        CsvUtils.writeLineFeed(writer);

                        if (!StringUtils.isBlank(channelId)) {
                            if (byteCount > StatisticConstants.FLUSH_SIZE_BYTES) {
                                statisticManager.incrementDataBytesSent(channelId, byteCount);
                                byteCount = 0;
                            }

                            if (lineCount > StatisticConstants.FLUSH_SIZE_LINES) {
                                statisticManager.incrementDataSent(channelId, lineCount);
                                lineCount = 0;
                            }
                        }
                    }
                }
                
                writer.flush();
                
            } finally {
                csvReader.close();
                IOUtils.closeQuietly(reader);
                if (!StringUtils.isBlank(channelId)) {
                    if (byteCount > 0) {
                        statisticManager.incrementDataBytesSent(channelId, byteCount);
                    }

                    if (lineCount > 0) {
                        statisticManager.incrementDataSent(channelId, lineCount);
                    }
                }
            }
        }
    }    

    /**
     * Allow a handler callback to do the work so we can route the extracted
     * data to other types of handlers for processing.
     */
    protected void databaseExtract(Node node, OutgoingBatch batch, final IExtractListener handler, BufferedWriter keepaliveWriter)
            throws IOException {
        batch.resetStats();
        long ts = System.currentTimeMillis();
        handler.startBatch(batch);
        selectEventDataToExtract(handler, batch, keepaliveWriter);
        handler.endBatch(batch);
        batch.setExtractMillis(System.currentTimeMillis() - ts);
    }

    public boolean extractBatchRange(IOutgoingTransport transport, String startBatchId, String endBatchId)
            throws IOException {
        IDataExtractor dataExtractor = getDataExtractor(null);
        ExtractStreamHandler handler = createExtractStreamHandler(dataExtractor, transport.open());;
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

    public boolean extractBatchRange(final IExtractListener handler, String startBatchId, String endBatchId)
            throws IOException {
        if (areNumeric(startBatchId, endBatchId)) {
            OutgoingBatches batches = outgoingBatchService.getOutgoingBatchRange(startBatchId, endBatchId);

            if (batches != null && batches.getBatches() != null && batches.getBatches().size() > 0) {
                try {
                    handler.init();
                    for (final OutgoingBatch batch : batches.getBatches()) {
                        handler.startBatch(batch);
                        selectEventDataToExtract(handler, batch, null);
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

    private void selectEventDataToExtract(final IExtractListener handler, final OutgoingBatch batch, final BufferedWriter keepAliveWriter) {
        final long flushForKeepAliveInMs = parameterService.getLong(ParameterConstants.DATA_EXTRACTOR_FLUSH_FOR_KEEP_ALIVE, -1);
        dataService.handleDataSelect(batch.getBatchId(), -1, batch.getChannelId(), false, new IModelRetrievalHandler<Data, String>() {        
            long lastKeepAliveFlush = System.currentTimeMillis();
            String nodeId = nodeService.findIdentityNodeId();
            public boolean retrieved(Data data, String routerId, int count) throws IOException {
                handler.dataExtracted(data, routerId);
                if (flushForKeepAliveInMs > 0 && System.currentTimeMillis()-lastKeepAliveFlush > flushForKeepAliveInMs && keepAliveWriter != null) {
                    CsvUtils.write(keepAliveWriter, CsvConstants.NODEID, CsvUtils.DELIMITER, nodeId);
                    CsvUtils.writeLineFeed(keepAliveWriter);  
                    keepAliveWriter.flush();
                    lastKeepAliveFlush = System.currentTimeMillis();
                }                
                return true;
            }
        });
    }

    public void setOutgoingBatchService(IOutgoingBatchService batchBuilderService) {
        this.outgoingBatchService = batchBuilderService;
    }

    public void setContext(DataExtractorContext context) {
        this.clonableContext = context;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    class ExtractStreamHandler implements IExtractListener {

        IDataExtractor dataExtractor;

        DataExtractorContext context;

        DataExtractorStatisticsWriter writer;

        ExtractStreamHandler(IDataExtractor dataExtractor, DataExtractorStatisticsWriter writer) throws IOException {
            this.dataExtractor = dataExtractor;
            this.writer = writer;
        }

        public void dataExtracted(Data data, String routerId) throws IOException {
            if (extractorFilters != null) {
                for (IExtractorFilter filter : extractorFilters) {
                    if (!filter.filterData(data, routerId, context)) {
                        // short circuit the extract if instructed
                        return;
                    }
                }
            }
            dataExtractor.write(writer, data, routerId, context);
        }

        public void done() throws IOException {
        }

        public void endBatch(OutgoingBatch batch) throws IOException {
            dataExtractor.commit(batch, writer);
            writer.flush();
        }

        public void init() throws IOException {
            this.context = DataExtractorService.this.clonableContext.copy(dataExtractor);
            dataExtractor.init(writer, context);
        }

        public void startBatch(OutgoingBatch batch) throws IOException {
            context.setBatch(batch);
            writer.setChannelId(batch.getChannelId());
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

    public void setRoutingService(IRouterService routingService) {
        this.routingService = routingService;
    }

    public void setDataService(IDataService dataService) {
        this.dataService = dataService;
    }

    public void setTriggerRouterService(ITriggerRouterService triggerService) {
        this.triggerRouterService = triggerService;
    }
    
    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }
}