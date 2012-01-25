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
import java.io.OutputStream;
import java.io.Writer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.SymmetricException;
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
import org.jumpmind.symmetric.io.data.reader.ExtractDataReader;
import org.jumpmind.symmetric.io.data.reader.IExtractBatchSource;
import org.jumpmind.symmetric.io.data.reader.ProtocolDataReader;
import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.io.data.transform.TransformTable;
import org.jumpmind.symmetric.io.data.writer.IProtocolDataWriterListener;
import org.jumpmind.symmetric.io.data.writer.ProtocolDataWriter;
import org.jumpmind.symmetric.io.data.writer.StagingDataWriter;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.route.SimpleRouterContext;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.ITransformService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.service.impl.TransformService.TransformTableNodeGroupLink;
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

    private ITransformService transformService;

    private IDataService dataService;

    private INodeService nodeService;

    private IStatisticManager statisticManager;

    private Map<Long, IoResource> extractedBatchesHandle = new HashMap<Long, IoResource>();

    public DataExtractorService(IParameterService parameterService,
            ISymmetricDialect symmetricDialect, IOutgoingBatchService outgoingBatchService,
            IRouterService routingService, IConfigurationService configurationService,
            ITriggerRouterService triggerRouterService, INodeService nodeService,
            IDataService dataService, ITransformService transformService,
            IStatisticManager statisticManager) {
        super(parameterService, symmetricDialect);
        this.outgoingBatchService = outgoingBatchService;
        this.routerService = routingService;
        this.dataService = dataService;
        this.configurationService = configurationService;
        this.triggerRouterService = triggerRouterService;
        this.nodeService = nodeService;
        this.transformService = transformService;
        this.statisticManager = statisticManager;
    }

    /**
     * @see DataExtractorService#extractConfigurationStandalone(Node, Writer)
     */
    public void extractConfigurationStandalone(Node node, OutputStream out,
            String... tablesToExclude) {
        this.extractConfigurationStandalone(node, TransportUtils.toWriter(out), tablesToExclude);
    }

    /**
     * Extract the SymmetricDS configuration for the passed in {@link Node}.
     * Note that this method will insert an already acknowledged batch to
     * indicate that the configuration was sent. If the configuration fails to
     * load for some reason on the client the batch status will NOT reflect the
     * failure.
     */
    public void extractConfigurationStandalone(Node node, Writer writer, String... tablesToExclude) {
        Batch batch = new Batch(BatchInfo.VIRTUAL_BATCH_FOR_REGISTRATION, Constants.CHANNEL_CONFIG,
                symmetricDialect.getBinaryEncoding(), node.getNodeId());

        NodeGroupLink nodeGroupLink = new NodeGroupLink(parameterService.getNodeGroupId(),
                node.getNodeGroupId());

        List<TriggerRouter> triggerRouters = triggerRouterService.getTriggerRoutersForRegistration(
                StringUtils.isBlank(node.getSymmetricVersion()) ? Version.version() : node
                        .getSymmetricVersion(), nodeGroupLink, tablesToExclude);

        List<SelectFromTableEvent> initialLoadEvents = new ArrayList<SelectFromTableEvent>(
                triggerRouters.size() * 2);

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
            data.putAttribute(Data.ATTRIBUTE_ROUTER_ID, triggerRouter.getRouter().getRouterId());
            initialLoadEvents.add(new SelectFromTableEvent(data));
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
                initialLoadEvents
                        .add(new SelectFromTableEvent(node, triggerRouter, triggerHistory));
            } else {
                Data data = new Data(1, null, node.getNodeId(), DataEventType.INSERT,
                        triggerHistory.getSourceTableName(), null, triggerHistory, triggerRouter
                                .getTrigger().getChannelId(), null, null);
                initialLoadEvents.add(new SelectFromTableEvent(data));
            }
        }

        SelectFromTableSource source = new SelectFromTableSource(batch, initialLoadEvents);
        ExtractDataReader dataReader = new ExtractDataReader(this.symmetricDialect.getPlatform(),
                source);

        ProtocolDataWriter dataWriter = new ProtocolDataWriter(writer);
        DataProcessor processor = new DataProcessor(dataReader, dataWriter);
        processor.process();

        if (triggerRouters.size() == 0) {
            log.error("{} attempted registration, but was sent an empty configuration", node);
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
                log.debug("Batch {} is being ignored", batch.getBatchId());
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

    public List<OutgoingBatch> extract(Node targetNode, IOutgoingTransport targetTransport) {

        // make sure that data is routed before extracting if the route
        // job is not configured to start automatically
        if (!parameterService.is(ParameterConstants.START_ROUTE_JOB)) {
            routerService.routeData();
        }

        OutgoingBatches batches = outgoingBatchService.getOutgoingBatches(targetNode);

        if (batches.containsBatches()) {

            List<OutgoingBatch> activeBatches = filterBatchesForExtraction(batches,
                    targetTransport.getSuspendIgnoreChannelLists(configurationService));

            if (activeBatches.size() > 0) {
                extract(targetNode, targetTransport, activeBatches);
            }

            // Next, we update the node channel controls to the
            // current timestamp
            Calendar now = Calendar.getInstance();

            for (NodeChannel nodeChannel : batches.getActiveChannels()) {
                nodeChannel.setLastExtractTime(now.getTime());
                configurationService.saveNodeChannelControl(nodeChannel, false);
            }

            return activeBatches;
        }

        return new ArrayList<OutgoingBatch>(0);
    }

    public void extract(Node targetNode, IOutgoingTransport targetTransport,
            List<OutgoingBatch> activeBatches) {
        Node identity = nodeService.findIdentity();
        long batchesSelectedAtMs = System.currentTimeMillis();
        boolean streamToFileEnabled = parameterService
                .is(ParameterConstants.STREAM_TO_FILE_ENABLED);

        IDataWriter extractWriter = null;

        if (streamToFileEnabled) {
            long memoryThresholdInBytes = parameterService
                    .getLong(ParameterConstants.STREAM_TO_FILE_THRESHOLD);
            extractWriter = new StagingDataWriter(new File(System.getProperty("java.io.tmpdir")),
                    memoryThresholdInBytes, new IProtocolDataWriterListener() {
                        public void start(Batch batch) {
                        }

                        public void end(Batch batch, IoResource resource) {
                            extractedBatchesHandle.put(batch.getBatchId(), resource);
                        }
                    });
        } else {
            extractWriter = new ProtocolDataWriter(targetTransport.open());
        }

        List<TransformTableNodeGroupLink> transformsList = transformService.findTransformsFor(
                new NodeGroupLink(identity.getNodeGroupId(), targetNode.getNodeGroupId()),
                TransformPoint.EXTRACT, true);
        TransformTable[] transforms = transformsList != null ? transformsList
                .toArray(new TransformTable[transformsList.size()]) : null;
        TransformWriter transformExtractWriter = new TransformWriter(
                symmetricDialect.getPlatform(), TransformPoint.EXTRACT, extractWriter, transforms);

        final long maxBytesToSync = parameterService
                .getLong(ParameterConstants.TRANSPORT_MAX_BYTES_TO_SYNC);
        long bytesSentCount = 0;
        int batchesSentCount = 0;

        OutgoingBatch currentBatch = null;
        try {
            for (int i = 0; i < activeBatches.size(); i++) {
                currentBatch = activeBatches.get(i);

                if (System.currentTimeMillis() - batchesSelectedAtMs > MS_PASSED_BEFORE_BATCH_REQUERIED) {
                    currentBatch = outgoingBatchService
                            .findOutgoingBatch(currentBatch.getBatchId());
                }

                if (currentBatch.getStatus() != Status.OK) {
                    IoResource previouslyExtracted = extractedBatchesHandle.get(currentBatch
                            .getBatchId());
                    if (previouslyExtracted != null && previouslyExtracted.exists()) {
                        log.info(
                                "We have already extracted batch {}.  Using the existing extraction.  To force re-extraction, please restart this instance of SymmetricDS.",
                                currentBatch.getBatchId());
                    } else {
                        currentBatch.setStatus(OutgoingBatch.Status.QY);
                        currentBatch.setExtractCount(currentBatch.getExtractCount() + 1);
                        outgoingBatchService.updateOutgoingBatch(currentBatch);

                        IDataReader dataReader = new ExtractDataReader(
                                symmetricDialect.getPlatform(), new SelectFromSymData(currentBatch,
                                        targetNode));

                        new DataProcessor(dataReader, transformExtractWriter).process();
                    }

                    if (System.currentTimeMillis() - currentBatch.getLastUpdatedTime().getTime() > MS_PASSED_BEFORE_BATCH_REQUERIED) {
                        currentBatch = outgoingBatchService.findOutgoingBatch(currentBatch
                                .getBatchId());
                        currentBatch.setExtractMillis(currentBatch.getExtractMillis());
                    }

                    if (currentBatch.getStatus() != Status.OK) {
                        currentBatch.setStatus(OutgoingBatch.Status.SE);
                        currentBatch.setSentCount(currentBatch.getSentCount() + 1);
                        outgoingBatchService.updateOutgoingBatch(currentBatch);

                        IoResource extractedBatch = extractedBatchesHandle.get(currentBatch
                                .getBatchId());
                        if (extractedBatch != null) {
                            IDataReader dataReader = new ProtocolDataReader(extractedBatch.open());
                            IDataWriter dataWriter = new ProtocolDataWriter(targetTransport.open());
                            new DataProcessor(dataReader, dataWriter).process();
                        }

                        if (System.currentTimeMillis()
                                - currentBatch.getLastUpdatedTime().getTime() > MS_PASSED_BEFORE_BATCH_REQUERIED) {
                            currentBatch = outgoingBatchService.findOutgoingBatch(currentBatch
                                    .getBatchId());
                        }

                        if (currentBatch.getStatus() != Status.OK) {
                            currentBatch.setStatus(OutgoingBatch.Status.LD);
                            currentBatch.setLoadCount(currentBatch.getLoadCount() + 1);
                            outgoingBatchService.updateOutgoingBatch(currentBatch);

                            bytesSentCount += currentBatch.getByteCount();
                            batchesSentCount++;

                            if (bytesSentCount >= maxBytesToSync) {
                                log.info("DataExtractorReachedMaxNumberOfBytesToSync",
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
                statisticManager.incrementDataExtractedErrors(currentBatch.getChannelId(), 1);
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
            } else {
                throw new RuntimeException(e);
            }

        }

    }

    public boolean extractBatchRange(IOutgoingTransport transport, String startBatchId,
            String endBatchId) {
        // TODO
        return false;
    }

    protected Table lookupAndOrderColumnsAccordingToTriggerHistory(String routerId,
            TriggerHistory triggerHistory, Table currentTable) {
        String catalogName = triggerHistory.getSourceCatalogName();
        String schemaName = triggerHistory.getSourceSchemaName();
        String tableName = triggerHistory.getSourceTableName();
        if (currentTable == null
                || !currentTable.getFullyQualifiedTableName().equals(
                        Table.getFullyQualifiedTableName(catalogName, schemaName, tableName))) {
            currentTable = symmetricDialect.getPlatform().getTableFromCache(catalogName,
                    schemaName, tableName, false);
            if (currentTable == null) {
                throw new RuntimeException(String.format(
                        "Could not find the table, %s, to extract",
                        Table.getFullyQualifiedTableName(catalogName, schemaName, tableName)));
            }
            currentTable = currentTable.copy();
            currentTable.orderColumns(triggerHistory.getParsedColumnNames());

            Router router = triggerRouterService.getRouterById(routerId, false);
            if (router != null) {
                if (StringUtils.isNotBlank(router.getTargetCatalogName())) {
                    currentTable.setCatalog(router.getTargetCatalogName());
                }

                if (StringUtils.isNotBlank(router.getTargetSchemaName())) {
                    currentTable.setSchema(router.getTargetSchemaName());
                }

                if (StringUtils.isNotBlank(router.getTargetTableName())) {
                    currentTable.setName(router.getTargetTableName());
                }
            }
        }
        return currentTable;
    }

    class SelectFromSymData implements IExtractBatchSource {

        private Batch batch;

        private Table currentTable;

        private boolean requiresLobSelectedFromSource;

        private ISqlReadCursor<Data> cursor;

        private SelectFromTableSource reloadSource;

        private Node targetNode;

        public SelectFromSymData(OutgoingBatch outgoingBatch, Node targetNode) {
            this.batch = new Batch(outgoingBatch.getBatchId(), outgoingBatch.getChannelId(),
                    symmetricDialect.getBinaryEncoding(), outgoingBatch.getNodeId());
            this.targetNode = targetNode;
        }

        public Batch getBatch() {
            return batch;
        }

        public Table getTable() {
            return currentTable;
        }

        public CsvData next() {
            if (this.cursor == null) {
                this.cursor = dataService.selectDataFor(batch);
            }

            Data data = null;
            if (reloadSource != null) {
                data = (Data) reloadSource.next();
                currentTable = reloadSource.getTable();
                if (data == null) {
                    reloadSource.close();
                    reloadSource = null;
                }
            }

            if (data == null) {
                data = this.cursor.next();
                if (data != null) {
                    // TODO add null checks
                    TriggerHistory triggerHistory = data.getTriggerHistory();
                    String routerId = data.getAttribute(CsvData.ATTRIBUTE_ROUTER_ID);

                    if (data.getDataEventType() == DataEventType.RELOAD) {

                        String triggerId = triggerHistory.getTriggerId();

                        // fyi, this queries the database
                        TriggerRouter triggerRouter = triggerRouterService.findTriggerRouterById(
                                triggerId, routerId);
                        SelectFromTableEvent event = new SelectFromTableEvent(targetNode,
                                triggerRouter, triggerHistory);
                        this.reloadSource = new SelectFromTableSource(batch, event);
                        data = (Data) this.reloadSource.next();
                        this.currentTable = this.reloadSource.getTable();
                        this.requiresLobSelectedFromSource = this.reloadSource
                                .requiresLobsSelectedFromSource();
                    } else {
                        Trigger trigger = triggerRouterService.getTriggerById(
                                triggerHistory.getTriggerId(), false);
                        this.currentTable = lookupAndOrderColumnsAccordingToTriggerHistory(
                                routerId, triggerHistory, currentTable);
                        this.requiresLobSelectedFromSource = trigger.isUseStreamLobs();
                    }
                } else {
                    closeCursor();
                }
            }
            return data;
        }

        public boolean requiresLobsSelectedFromSource() {
            return requiresLobSelectedFromSource;
        }

        protected void closeCursor() {
            if (this.cursor != null) {
                this.cursor.close();
                this.cursor = null;
            }
        }

        public void close() {
            closeCursor();
            if (reloadSource != null) {
                reloadSource.close();
            }
        }

    }

    class SelectFromTableSource implements IExtractBatchSource {

        private Batch batch;

        private Table currentTable;

        private List<SelectFromTableEvent> selectFromTableEventsToSend;

        private SelectFromTableEvent currentInitialLoadEvent;

        private ISqlReadCursor<Data> cursor;

        private SimpleRouterContext routingContext;

        private Node node;

        private TriggerRouter triggerRouter;

        public SelectFromTableSource(Batch batch, SelectFromTableEvent event) {
            List<SelectFromTableEvent> initialLoadEvents = new ArrayList<DataExtractorService.SelectFromTableEvent>(
                    1);
            initialLoadEvents.add(event);
            this.init(batch, initialLoadEvents);
        }

        public SelectFromTableSource(Batch batch, List<SelectFromTableEvent> initialLoadEvents) {
            this.init(batch, initialLoadEvents);
        }

        protected void init(Batch batch, List<SelectFromTableEvent> initialLoadEvents) {
            this.selectFromTableEventsToSend = new ArrayList<SelectFromTableEvent>(
                    initialLoadEvents);
            this.batch = batch;
            this.node = nodeService.findNode(batch.getNodeId());
            if (node == null) {
                throw new SymmetricException("Could not find a node represented by %s",
                        this.batch.getNodeId());
            }
        }

        public Batch getBatch() {
            return batch;
        }

        public Table getTable() {
            return currentTable;
        }

        public CsvData next() {
            CsvData data = null;
            if (this.currentInitialLoadEvent == null && selectFromTableEventsToSend.size() > 0) {
                this.currentInitialLoadEvent = selectFromTableEventsToSend.remove(0);
                TriggerHistory history = this.currentInitialLoadEvent.getTriggerHistory();
                if (this.currentInitialLoadEvent.containsData()) {
                    data = this.currentInitialLoadEvent.getData();
                    this.currentInitialLoadEvent = null;
                    this.currentTable = lookupAndOrderColumnsAccordingToTriggerHistory(
                            (String) data.getAttribute(CsvData.ATTRIBUTE_ROUTER_ID), history,
                            currentTable);
                } else {
                    this.triggerRouter = this.currentInitialLoadEvent.getTriggerRouter();
                    NodeChannel channel = batch != null ? configurationService.getNodeChannel(
                            batch.getChannelId(), false) : new NodeChannel(this.triggerRouter
                            .getTrigger().getChannelId());
                    this.routingContext = new SimpleRouterContext(batch.getNodeId(), channel);
                    this.currentTable = lookupAndOrderColumnsAccordingToTriggerHistory(
                            triggerRouter.getRouter().getRouterId(), history, currentTable);
                    this.startNewCursor(history, triggerRouter);
                }

            }

            if (this.cursor != null) {
                data = this.cursor.next();
                if (data != null) {
                    if (!routerService.shouldDataBeRouted(routingContext, new DataMetaData(
                            (Data) data, currentTable, triggerRouter, routingContext.getChannel()),
                            node, true)) {
                        data = next();
                    }
                } else {
                    closeCursor();
                    data = next();
                }
            }

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
                final TriggerRouter triggerRouter) {
            String initialLoadSql = symmetricDialect.createInitialLoadSqlFor(
                    this.currentInitialLoadEvent.getNode(), triggerRouter, this.currentTable,
                    triggerHistory,
                    configurationService.getChannel(triggerRouter.getTrigger().getChannelId()));
            this.cursor = sqlTemplate.queryForCursor(initialLoadSql, new ISqlRowMapper<Data>() {
                public Data mapRow(Row rs) {
                    Data data = new Data(0, null, rs.stringValue(), DataEventType.INSERT,
                            triggerHistory.getSourceTableName(), null, triggerHistory, batch
                                    .getChannelId(), null, null);
                    data.putAttribute(Data.ATTRIBUTE_ROUTER_ID, triggerRouter.getRouter()
                            .getRouterId());
                    return data;
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

    class SelectFromTableEvent {

        private TriggerRouter triggerRouter;
        private TriggerHistory triggerHistory;
        private Node node;
        private Data data;

        public SelectFromTableEvent(Node node, TriggerRouter triggerRouter,
                TriggerHistory triggerHistory) {
            this.node = node;
            this.triggerRouter = triggerRouter;
            this.triggerHistory = triggerHistory != null ? triggerHistory : triggerRouterService
                    .getNewestTriggerHistoryForTrigger(triggerRouter.getTrigger().getTriggerId());
        }

        public SelectFromTableEvent(Data data) {
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

}
