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
 * under the License. 
 */
package org.jumpmind.symmetric.service.impl;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.AbstractSqlMap;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.log.Log;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.IoResource;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.DataProcessor;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.reader.IExtractBatchSource;
import org.jumpmind.symmetric.io.data.reader.ExtractDataReader;
import org.jumpmind.symmetric.io.data.reader.ProtocolDataReader;
import org.jumpmind.symmetric.io.data.writer.ProtocolDataWriter;
import org.jumpmind.symmetric.io.data.writer.StagingDataWriter;
import org.jumpmind.symmetric.io.data.writer.IProtocolDataWriterListener;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.TransportUtils;

/**
 * @see IDataExtractorService
 */
public class DataExtractorService extends AbstractService implements IDataExtractorService {

    final static long MS_PASSED_BEFORE_BATCH_REQUERIED = 5000;

    private IOutgoingBatchService outgoingBatchService;

    private IRouterService routerService;

    private IConfigurationService configurationService;

    private ITriggerRouterService triggerRouterService;

    private INodeService nodeService;

    private IStatisticManager statisticManager;

    private Map<Long, IoResource> extractedBatchesHandle = new HashMap<Long, IoResource>();

    private Set<String> extractingNodes = new HashSet<String>();

    public DataExtractorService(Log log, IParameterService parameterService,
            ISymmetricDialect symmetricDialect, IOutgoingBatchService outgoingBatchService,
            IRouterService routingService, IConfigurationService configurationService,
            ITriggerRouterService triggerRouterService, INodeService nodeService,
            IStatisticManager statisticManager) {
        super(log, parameterService, symmetricDialect);
        this.outgoingBatchService = outgoingBatchService;
        this.routerService = routingService;
        this.configurationService = configurationService;
        this.triggerRouterService = triggerRouterService;
        this.nodeService = nodeService;
        this.statisticManager = statisticManager;
    }

    @Override
    protected AbstractSqlMap createSqlMap() {
        return null;
    }

    /**
     * @see DataExtractorService#extractConfigurationStandalone(Node, Writer)
     */
    public void extractConfigurationStandalone(Node node, OutputStream out,
            String... tablesToExclude) throws IOException {
        this.extractConfigurationStandalone(node, TransportUtils.toWriter(out), tablesToExclude);
    }

    /**
     * Extract the SymmetricDS configuration for the passed in {@link Node}.
     * Note that this method will insert an already acknowledged batch to
     * indicate that the configuration was sent. If the configuration fails to
     * load for some reason on the client the batch status will NOT reflect the
     * failure.
     */
    public void extractConfigurationStandalone(Node node, Writer writer, String... tablesToExclude)
            throws IOException {
        Batch batch = new Batch(BatchInfo.VIRTUAL_BATCH_FOR_REGISTRATION, Constants.CHANNEL_CONFIG,
                symmetricDialect.getBinaryEncoding(), node.getNodeId());

        NodeGroupLink nodeGroupLink = new NodeGroupLink(parameterService.getNodeGroupId(),
                node.getNodeGroupId());

        List<TriggerRouter> triggerRouters = triggerRouterService.getTriggerRoutersForRegistration(
                StringUtils.isBlank(node.getSymmetricVersion()) ? Version.version() : node
                        .getSymmetricVersion(), nodeGroupLink, tablesToExclude);

        List<InitialLoadEvent> initialLoadEvents = new ArrayList<InitialLoadEvent>(
                triggerRouters.size());

        for (int i = triggerRouters.size() - 1; i >= 0; i--) {
            TriggerRouter triggerRouter = triggerRouters.get(i);
            TriggerHistory triggerHistory = triggerRouterService
                    .getNewestTriggerHistoryForTrigger(triggerRouter.getTrigger().getTriggerId());
            if (triggerHistory == null) {
                triggerHistory = new TriggerHistory(symmetricDialect.getTable(
                        triggerRouter.getTrigger(), false), triggerRouter.getTrigger());
                triggerHistory.setTriggerHistoryId(Integer.MAX_VALUE - i);
            }
            StringBuilder sql = new StringBuilder(symmetricDialect.createPurgeSqlFor(node,
                    triggerRouter));
            addPurgeCriteriaToConfigurationTables(triggerRouter.getTrigger().getSourceTableName(),
                    sql);
            String sourceTable = triggerHistory.getSourceTableName();
            Data data = new Data(1, null, sql.toString(), DataEventType.SQL, sourceTable, null,
                    triggerHistory, triggerRouter.getTrigger().getChannelId(), null, null);
            initialLoadEvents.add(new InitialLoadEvent(data));
        }

        for (int i = 0; i < triggerRouters.size(); i++) {
            TriggerRouter triggerRouter = triggerRouters.get(i);
            TriggerHistory triggerHistory = triggerRouterService
                    .getNewestTriggerHistoryForTrigger(triggerRouter.getTrigger().getTriggerId());
            if (triggerHistory == null) {
                triggerHistory = new TriggerHistory(symmetricDialect.getTable(
                        triggerRouter.getTrigger(), false), triggerRouter.getTrigger());
                triggerHistory.setTriggerHistoryId(Integer.MAX_VALUE - i);
            }

            if (!triggerRouter.getTrigger().getSourceTableName()
                    .endsWith(TableConstants.SYM_NODE_IDENTITY)) {
                initialLoadEvents.add(new InitialLoadEvent(node, triggerRouter, triggerHistory));
            } else {
                Data data = new Data(1, null, node.getNodeId(), DataEventType.INSERT,
                        triggerHistory.getSourceTableName(), null, triggerHistory, triggerRouter
                                .getTrigger().getChannelId(), null, null);
                initialLoadEvents.add(new InitialLoadEvent(data));
            }
        }

        InitialLoadSource source = new InitialLoadSource(batch, initialLoadEvents);
        ExtractDataReader dataReader = new ExtractDataReader(
                this.symmetricDialect.getPlatform(), source);
        ProtocolDataWriter dataWriter = new ProtocolDataWriter(writer);
        DataProcessor<ExtractDataReader, ProtocolDataWriter> processor = new DataProcessor<ExtractDataReader, ProtocolDataWriter>(
                dataReader, dataWriter);
        processor.process();

        if (triggerRouters.size() == 0) {
            log.error("%s attempted registration, but was sent an empty configuration", node);
        }

    }

    private void addPurgeCriteriaToConfigurationTables(String sourceTableName, StringBuilder sql) {
        if ((TableConstants
                .getTableName(parameterService.getTablePrefix(), TableConstants.SYM_NODE)
                .equalsIgnoreCase(sourceTableName))
                || TableConstants.getTableName(parameterService.getTablePrefix(),
                        TableConstants.SYM_NODE_SECURITY).equalsIgnoreCase(sourceTableName)) {
            Node me = nodeService.findIdentity();
            if (me != null) {
                sql.append(String.format(" where created_at_node_id='%s'", me.getNodeId()));
            }
        }
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
                log.debug("Batch %s is being ignored", batch.getBatchId());
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

        if (!extractingNodes.contains(node.getNodeId())) {
            try {
                extractingNodes.add(node.getNodeId());

                // make sure that data is routed before extracting if the route
                // job
                // is not configured to start automatically
                if (!parameterService.is(ParameterConstants.START_ROUTE_JOB)) {
                    routerService.routeData();
                }

                OutgoingBatches batches = outgoingBatchService.getOutgoingBatches(node);
                long batchesSelectedAtMs = System.currentTimeMillis();

                if (batches.containsBatches()) {

                    activeBatches = filterBatchesForExtraction(batches,
                            targetTransport.getSuspendIgnoreChannelLists(configurationService));

                    if (activeBatches.size() > 0) {

                        boolean streamToFileEnabled = parameterService
                                .is(ParameterConstants.STREAM_TO_FILE_ENABLED);

                        IDataWriter extractWriter = null;

                        if (streamToFileEnabled) {
                            long memoryThresholdInBytes = parameterService
                                    .getLong(ParameterConstants.STREAM_TO_FILE_THRESHOLD);
                            extractWriter = new StagingDataWriter(new File(
                                    System.getProperty("java.io.tmpdir")), memoryThresholdInBytes,
                                    new IProtocolDataWriterListener() {
                                        public void start(Batch batch) {
                                        }

                                        public void end(Batch batch, IoResource resource) {
                                            extractedBatchesHandle.put(batch.getBatchId(), resource);
                                        }
                                    });
                        } else {
                            extractWriter = new ProtocolDataWriter(targetTransport.open());
                        }

                        OutgoingBatch currentBatch = null;
                        try {
                            final long maxBytesToSync = parameterService
                                    .getLong(ParameterConstants.TRANSPORT_MAX_BYTES_TO_SYNC);
                            long bytesSentCount = 0;
                            int batchesSentCount = 0;
                            for (OutgoingBatch outgoingBatch : activeBatches) {
                                if (System.currentTimeMillis() - batchesSelectedAtMs > MS_PASSED_BEFORE_BATCH_REQUERIED) {
                                    outgoingBatch = outgoingBatchService
                                            .findOutgoingBatch(outgoingBatch.getBatchId());
                                }

                                currentBatch = outgoingBatch;

                                if (outgoingBatch.getStatus() != Status.OK) {
                                    IoResource previouslyExtracted = extractedBatchesHandle
                                            .get(currentBatch.getBatchId());
                                    if (previouslyExtracted != null && previouslyExtracted.exists()) {
                                        log.info("We have already extracted batch %d.  Using the existing extraction.  To force re-extraction, please restart this instance of SymmetricDS.",
                                                currentBatch.getBatchId());
                                    } else {
                                        outgoingBatch.setStatus(OutgoingBatch.Status.QY);
                                        outgoingBatch.setExtractCount(outgoingBatch
                                                .getExtractCount() + 1);
                                        outgoingBatchService.updateOutgoingBatch(outgoingBatch);

                                        IDataReader dataReader = new ExtractDataReader(
                                                symmetricDialect.getPlatform(),
                                                new SelectBatchSource(outgoingBatch));

                                        new DataProcessor<IDataReader, IDataWriter>(dataReader,
                                                extractWriter).process();
                                    }

                                    if (System.currentTimeMillis()
                                            - outgoingBatch.getLastUpdatedTime().getTime() > MS_PASSED_BEFORE_BATCH_REQUERIED) {
                                        outgoingBatch = outgoingBatchService
                                                .findOutgoingBatch(currentBatch.getBatchId());
                                        outgoingBatch.setExtractMillis(currentBatch
                                                .getExtractMillis());
                                        currentBatch = outgoingBatch;
                                    }

                                    if (outgoingBatch.getStatus() != Status.OK) {
                                        outgoingBatch.setStatus(OutgoingBatch.Status.SE);
                                        outgoingBatch
                                                .setSentCount(outgoingBatch.getSentCount() + 1);
                                        outgoingBatchService.updateOutgoingBatch(outgoingBatch);

                                        IoResource extractedBatch = extractedBatchesHandle
                                                .get(outgoingBatch.getBatchId());
                                        if (extractedBatch != null) {
                                            IDataReader dataReader = new ProtocolDataReader(
                                                    extractedBatch.open());
                                            IDataWriter dataWriter = new ProtocolDataWriter(
                                                    targetTransport.open());
                                            new DataProcessor<IDataReader, IDataWriter>(dataReader,
                                                    dataWriter).process();
                                        }

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
                                                log.info(
                                                        "DataExtractorReachedMaxNumberOfBytesToSync",
                                                        batchesSentCount, bytesSentCount);
                                                break;
                                            }

                                        }
                                    }

                                }

                            }

                            // TODO should be deleting handles here or when the
                            // ack comes back
                            // for (OutgoingBatch outgoingBatch : activeBatches)
                            // {
                            // File file =
                            // extractedBatchesHandle.remove(outgoingBatch
                            // .getBatchId());
                            // if (file != null && file.exists()) {
                            // fileWriter.delete();
                            // }
                            // }

                        } catch (Exception e) {
                            SQLException se = unwrapSqlException(e);
                            if (currentBatch != null) {
                                statisticManager.incrementDataExtractedErrors(
                                        currentBatch.getChannelId(), 1);
                                if (se != null) {
                                    currentBatch.setSqlState(se.getSQLState());
                                    currentBatch.setSqlCode(se.getErrorCode());
                                    currentBatch.setSqlMessage(se.getMessage());
                                } else {
                                    currentBatch.setSqlMessage(e.getMessage());
                                }
                                currentBatch.revertStatsOnError();
                                currentBatch.setStatus(OutgoingBatch.Status.ER);
                                currentBatch.setErrorFlag(true);
                                outgoingBatchService.updateOutgoingBatch(currentBatch);
                            } else {
                                log.error("Could not log the outgoing batch status because the batch was null.", e);
                            }

                            if (e instanceof RuntimeException) {
                                throw (RuntimeException) e;
                            } else if (e instanceof IOException) {
                                throw (IOException) e;
                            } else {
                                throw new RuntimeException(e);
                            }

                        }

                        // Next, we update the node channel controls to the
                        // current timestamp
                        Calendar now = Calendar.getInstance();

                        for (NodeChannel nodeChannel : batches.getActiveChannels()) {
                            nodeChannel.setLastExtractTime(now.getTime());
                            configurationService.saveNodeChannelControl(nodeChannel, false);
                        }

                    }
                }

            } finally {
                extractingNodes.remove(node.getNodeId());
            }

        }

        return activeBatches != null ? activeBatches : new ArrayList<OutgoingBatch>(0);

    }

    public boolean extractBatchRange(IOutgoingTransport transport, String startBatchId,
            String endBatchId) throws IOException {
        // TODO
        return false;
    }

    class InitialLoadEvent {

        private TriggerRouter triggerRouter;
        private TriggerHistory triggerHistory;
        private Node node;
        private Data data;

        public InitialLoadEvent(Node node, TriggerRouter triggerRouter,
                TriggerHistory triggerHistory) {
            this.node = node;
            this.triggerRouter = triggerRouter;
            this.triggerHistory = triggerHistory != null ? triggerHistory : triggerRouterService
                    .getNewestTriggerHistoryForTrigger(triggerRouter.getTrigger().getTriggerId());
        }

        public InitialLoadEvent(Data data) {
            this.data = data;
            this.triggerHistory = data.getTriggerHistory();
        }

        public TriggerHistory getTriggerHistory() {
            return triggerHistory;
        }

        public TriggerRouter getTriggerRouter() {
            return triggerRouter;
        }

        public Data getData() {
            return data;
        }

        public Node getNode() {
            return node;
        }

        public boolean containsData() {
            return data != null;
        }

    }

    class SelectBatchSource implements IExtractBatchSource {

        public SelectBatchSource(OutgoingBatch outgoingBatch) {
            // TODO Auto-generated constructor stub
        }

        public Batch getBatch() {
            // TODO Auto-generated method stub
            return null;
        }

        public Table getTable() {
            // TODO Auto-generated method stub
            return null;
        }

        public CsvData next() {
            // TODO symmetricDialect.massageDataExtractionSql
            // TODO Auto-generated method stub
            return null;
        }

        public boolean requiresLobsSelectedFromSource() {
            // TODO Auto-generated method stub
            return false;
        }

        public void close() {

        }
        
        class CsvDataRowMapper implements ISqlRowMapper<CsvData> {
            public CsvData mapRow(Row row) {
                CsvData data = new CsvData();
                data.putCsvData(CsvData.ROW_DATA, row.getString("ROW_DATA"));
                data.putCsvData(CsvData.PK_DATA, row.getString("PK_DATA"));
                //if (extractOldData) 
                {
                    data.putCsvData(CsvData.OLD_DATA, row.getString("OLD_DATA"));
                }

                data.putAttribute(CsvData.ATTRIBUTE_CHANNEL_ID, row.getString("CHANNEL_ID"));
                data.putAttribute(CsvData.ATTRIBUTE_TX_ID, row.getString("TRANSACTION_ID"));
                data.setDataEventType(DataEventType.getEventType(row.getString("EVENT_TYPE")));
                data.putAttribute(CsvData.ATTRIBUTE_TABLE_ID, row.getInt("TRIGGER_HIST_ID"));
                data.putAttribute(CsvData.ATTRIBUTE_SOURCE_NODE_ID, row.getString("SOURCE_NODE_ID"));
                data.putAttribute(CsvData.ATTRIBUTE_ROUTER_ID, row.getString("ROUTER_ID"));
                data.putAttribute(CsvData.ATTRIBUTE_EXTERNAL_DATA, row.getString("EXTERNAL_DATA"));
                data.putAttribute(CsvData.ATTRIBUTE_DATA_ID, row.getLong("DATA_ID"));
                return data;
            }
        }

    }

    class InitialLoadSource implements IExtractBatchSource {

        private Batch batch;

        private Table currentTable;

        private List<InitialLoadEvent> initialLoadEventsToSend;

        private InitialLoadEvent currentInitialLoadEvent;

        private ISqlReadCursor<Data> cursor;

        public InitialLoadSource(Batch batch, List<InitialLoadEvent> initialLoadEvents) {
            this.initialLoadEventsToSend = new ArrayList<InitialLoadEvent>(initialLoadEvents);
            this.batch = batch;
        }

        public Batch getBatch() {
            return batch;
        }

        public Table getTable() {
            return currentTable;
        }

        public CsvData next() {
            CsvData data = null;
            if (this.currentInitialLoadEvent == null && initialLoadEventsToSend.size() > 0) {
                this.currentInitialLoadEvent = initialLoadEventsToSend.remove(0);
                TriggerHistory history = this.currentInitialLoadEvent.getTriggerHistory();
                String catalogName = history.getSourceCatalogName();
                String schemaName = history.getSourceSchemaName();
                String tableName = history.getSourceTableName();
                this.currentTable = symmetricDialect.getPlatform().getTableFromCache(catalogName,
                        schemaName, tableName, false);
                if (this.currentTable == null) {
                    throw new RuntimeException(String.format(
                            "Could not find the table, %s, to extract",
                            Table.getFullyQualifiedTableName(catalogName, schemaName, tableName)));
                }
                this.currentTable = currentTable.copy();
                this.currentTable.orderColumns(history.getParsedColumnNames());
                if (this.currentInitialLoadEvent.containsData()) {
                    data = this.currentInitialLoadEvent.getData();
                    this.currentInitialLoadEvent = null;
                } else {
                    TriggerRouter triggerRouter = this.currentInitialLoadEvent.getTriggerRouter();
                    this.startNewCursor(history, triggerRouter);
                }
            }

            if (this.cursor != null) {
                data = this.cursor.next();
                if (data == null) {
                    this.cursor.close();
                    data = next();
                }
            }

            // TODO use router to filter out events that may not need routed

            return data;
        }

        protected void closeCursor() {
            if (this.cursor != null) {
                this.cursor.close();
                this.cursor = null;
                this.currentInitialLoadEvent = null;
            }
        }

        protected void startNewCursor(final TriggerHistory triggerHistory,
                TriggerRouter triggerRouter) {
            String initialLoadSql = symmetricDialect.createInitialLoadSqlFor(
                    this.currentInitialLoadEvent.getNode(), triggerRouter, this.currentTable,
                    triggerHistory,
                    configurationService.getChannel(triggerRouter.getTrigger().getChannelId()));
            this.cursor = sqlTemplate.queryForCursor(initialLoadSql, new ISqlRowMapper<Data>() {
                public Data mapRow(Row rs) {
                    return new Data(0, null, rs.stringValue(), DataEventType.INSERT, triggerHistory
                            .getSourceTableName(), null, triggerHistory, batch.getChannelId(),
                            null, null);
                }
            });
        }

        public boolean requiresLobsSelectedFromSource() {
            if (this.currentInitialLoadEvent != null
                    && this.currentInitialLoadEvent.getTriggerRouter() != null) {
                return this.currentInitialLoadEvent.getTriggerRouter().getTrigger()
                        .isUseStreamLobs();
            } else {
                return false;
            }
        }

        public void close() {
            closeCursor();
        }

    }

}