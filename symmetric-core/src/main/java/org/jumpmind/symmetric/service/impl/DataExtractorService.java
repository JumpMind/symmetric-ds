/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
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

import java.io.OutputStream;
import java.io.Writer;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.io.DatabaseXmlUtil;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.PlatformColumn;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.DdlBuilderFactory;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlConstants;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.Batch.BatchType;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.DataProcessor;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.ProtocolException;
import org.jumpmind.symmetric.io.data.reader.ExtractDataReader;
import org.jumpmind.symmetric.io.data.reader.IExtractDataReaderSource;
import org.jumpmind.symmetric.io.data.reader.ProtocolDataReader;
import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.io.data.transform.TransformTable;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.data.writer.IProtocolDataWriterListener;
import org.jumpmind.symmetric.io.data.writer.ProtocolDataWriter;
import org.jumpmind.symmetric.io.data.writer.StagingDataWriter;
import org.jumpmind.symmetric.io.data.writer.StructureDataWriter;
import org.jumpmind.symmetric.io.data.writer.StructureDataWriter.PayloadType;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.ExtractRequest;
import org.jumpmind.symmetric.model.ExtractRequest.ExtractStatus;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeCommunication;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.model.OutgoingBatchWithPayload;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfoDataWriter;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.route.AbstractFileParsingRouter;
import org.jumpmind.symmetric.route.SimpleRouterContext;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeCommunicationService;
import org.jumpmind.symmetric.service.INodeCommunicationService.INodeCommunicationExecutor;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.ISequenceService;
import org.jumpmind.symmetric.service.ITransformService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.service.impl.TransformService.TransformTableNodeGroupLink;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.TransportUtils;
import org.jumpmind.util.Statistics;

/**
 * @see IDataExtractorService
 */
public class DataExtractorService extends AbstractService implements IDataExtractorService,
        INodeCommunicationExecutor {

    final static long MS_PASSED_BEFORE_BATCH_REQUERIED = 5000;
        
    protected enum ExtractMode { FOR_SYM_CLIENT, FOR_PAYLOAD_CLIENT, EXTRACT_ONLY };

    private IOutgoingBatchService outgoingBatchService;

    private IRouterService routerService;

    private IConfigurationService configurationService;

    private ITriggerRouterService triggerRouterService;

    private ITransformService transformService;
    
    private ISequenceService sequenceService;

    private IDataService dataService;

    private INodeService nodeService;

    private IStatisticManager statisticManager;

    private IStagingManager stagingManager;

    private INodeCommunicationService nodeCommunicationService;

    private IClusterService clusterService;

    private Map<String, Semaphore> locks = new HashMap<String, Semaphore>();

    public DataExtractorService(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect());
        this.outgoingBatchService = engine.getOutgoingBatchService();
        this.routerService = engine.getRouterService();
        this.dataService = engine.getDataService();
        this.configurationService = engine.getConfigurationService();
        this.triggerRouterService = engine.getTriggerRouterService();
        this.nodeService = engine.getNodeService();
        this.transformService = engine.getTransformService();
        this.statisticManager = engine.getStatisticManager();
        this.stagingManager = engine.getStagingManager();
        this.nodeCommunicationService = engine.getNodeCommunicationService();
        this.clusterService = engine.getClusterService();
        this.sequenceService = engine.getSequenceService();
        setSqlMap(new DataExtractorServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    /**
     * @see DataExtractorService#extractConfigurationStandalone(Node, Writer)
     */
    public void extractConfigurationStandalone(Node node, OutputStream out) {
        this.extractConfigurationStandalone(node, TransportUtils.toWriter(out));
    }

    /**
     * Extract the SymmetricDS configuration for the passed in {@link Node}.
     */
    public void extractConfigurationStandalone(Node targetNode, Writer writer,
            String... tablesToExclude) {
        Node sourceNode = nodeService.findIdentity();

        if (targetNode != null && sourceNode != null) {

            Batch batch = new Batch(BatchType.EXTRACT, Constants.VIRTUAL_BATCH_FOR_REGISTRATION,
                    Constants.CHANNEL_CONFIG, symmetricDialect.getBinaryEncoding(),
                    sourceNode.getNodeId(), targetNode.getNodeId(), false);

            NodeGroupLink nodeGroupLink = new NodeGroupLink(parameterService.getNodeGroupId(),
                    targetNode.getNodeGroupId());

            List<TriggerRouter> triggerRouters = triggerRouterService
                    .buildTriggerRoutersForSymmetricTables(
                            StringUtils.isBlank(targetNode.getSymmetricVersion()) ? Version
                                    .version() : targetNode.getSymmetricVersion(), nodeGroupLink,
                            tablesToExclude);

            List<SelectFromTableEvent> initialLoadEvents = new ArrayList<SelectFromTableEvent>(
                    triggerRouters.size() * 2);

            boolean pre37 = Version.isOlderThanVersion(targetNode.getSymmetricVersion(), "3.7.0");

            for (int i = triggerRouters.size() - 1; i >= 0; i--) {
                TriggerRouter triggerRouter = triggerRouters.get(i);
                String channelId = triggerRouter.getTrigger().getChannelId();
                if (Constants.CHANNEL_CONFIG.equals(channelId)
                        || Constants.CHANNEL_HEARTBEAT.equals(channelId)) {
                    if (!(pre37 && triggerRouter.getTrigger().getSourceTableName().toLowerCase()
                            .contains("extension"))) {

                        TriggerHistory triggerHistory = triggerRouterService
                                .getNewestTriggerHistoryForTrigger(triggerRouter.getTrigger()
                                        .getTriggerId(), null, null, triggerRouter.getTrigger()
                                        .getSourceTableName());
                        if (triggerHistory == null) {
                            Trigger trigger = triggerRouter.getTrigger();
                            Table table = symmetricDialect.getPlatform().getTableFromCache(
                                    trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                                    trigger.getSourceTableName(), false);
                            if (table == null) {
                                throw new IllegalStateException("Could not find a required table: "
                                        + triggerRouter.getTrigger().getSourceTableName());
                            }
                            triggerHistory = new TriggerHistory(table, triggerRouter.getTrigger(),
                                    symmetricDialect.getTriggerTemplate());
                            triggerHistory.setTriggerHistoryId(Integer.MAX_VALUE - i);
                        }

                        StringBuilder sql = new StringBuilder(symmetricDialect.createPurgeSqlFor(
                                targetNode, triggerRouter, triggerHistory));
                        addPurgeCriteriaToConfigurationTables(triggerRouter.getTrigger()
                                .getSourceTableName(), sql);
                        String sourceTable = triggerHistory.getSourceTableName();
                        Data data = new Data(1, null, sql.toString(), DataEventType.SQL,
                                sourceTable, null, triggerHistory, triggerRouter.getTrigger()
                                        .getChannelId(), null, null);
                        data.putAttribute(Data.ATTRIBUTE_ROUTER_ID, triggerRouter.getRouter()
                                .getRouterId());
                        initialLoadEvents.add(new SelectFromTableEvent(data));
                    }
                }
            }

            for (int i = 0; i < triggerRouters.size(); i++) {
                TriggerRouter triggerRouter = triggerRouters.get(i);
                String channelId = triggerRouter.getTrigger().getChannelId();
                if (Constants.CHANNEL_CONFIG.equals(channelId)
                        || Constants.CHANNEL_HEARTBEAT.equals(channelId)) {
                    if (!(pre37 && triggerRouter.getTrigger().getSourceTableName().toLowerCase()
                            .contains("extension"))) {
                        TriggerHistory triggerHistory = triggerRouterService
                                .getNewestTriggerHistoryForTrigger(triggerRouter.getTrigger()
                                        .getTriggerId(), null, null, null);
                        if (triggerHistory == null) {
                            Trigger trigger = triggerRouter.getTrigger();
                            triggerHistory = new TriggerHistory(symmetricDialect.getPlatform()
                                    .getTableFromCache(trigger.getSourceCatalogName(),
                                            trigger.getSourceSchemaName(),
                                            trigger.getSourceTableName(), false), trigger,
                                    symmetricDialect.getTriggerTemplate());
                            triggerHistory.setTriggerHistoryId(Integer.MAX_VALUE - i);
                        }

                        Table table = symmetricDialect.getPlatform().getTableFromCache(
                        		triggerHistory.getSourceCatalogName(), triggerHistory.getSourceSchemaName(),
                        		triggerHistory.getSourceTableName(), false);
                        String initialLoadSql = "1=1 order by ";
                        String quote = symmetricDialect.getPlatform().getDdlBuilder().getDatabaseInfo().getDelimiterToken();
                        Column[] pkColumns = table.getPrimaryKeyColumns();
                        for (int j = 0; j < pkColumns.length; j++) {
                        	if (j > 0) {
                        		initialLoadSql += ", ";	
                        	}
                        	initialLoadSql += quote + pkColumns[j].getName() + quote;
                        }

                        if (!triggerRouter.getTrigger().getSourceTableName()
                                .endsWith(TableConstants.SYM_NODE_IDENTITY)) {
                            initialLoadEvents.add(new SelectFromTableEvent(targetNode,
                                    triggerRouter, triggerHistory, initialLoadSql));
                        } else {
                            Data data = new Data(1, null, targetNode.getNodeId(),
                                    DataEventType.INSERT, triggerHistory.getSourceTableName(),
                                    null, triggerHistory,
                                    triggerRouter.getTrigger().getChannelId(), null, null);
                            initialLoadEvents.add(new SelectFromTableEvent(data));
                        }
                    }
                }
            }

            SelectFromTableSource source = new SelectFromTableSource(batch, initialLoadEvents);
            ExtractDataReader dataReader = new ExtractDataReader(
                    this.symmetricDialect.getPlatform(), source);

            ProtocolDataWriter dataWriter = new ProtocolDataWriter(
                    nodeService.findIdentityNodeId(), writer, targetNode.requires13Compatiblity());
            DataProcessor processor = new DataProcessor(dataReader, dataWriter,
                    "configuration extract");
            DataContext ctx = new DataContext();
            ctx.put(Constants.DATA_CONTEXT_TARGET_NODE, targetNode);
            ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE, sourceNode);
            processor.process(ctx);

            if (triggerRouters.size() == 0) {
                log.error("{} attempted registration, but was sent an empty configuration",
                        targetNode);
            }
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

        if (parameterService.is(ParameterConstants.FILE_SYNC_ENABLE)) {
            List<Channel> fileSyncChannels = configurationService.getFileSyncChannels();
            for (Channel channel : fileSyncChannels) {
                batches.filterBatchesForChannel(channel);    
            }            
        }

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
            batch.setStatus(OutgoingBatch.Status.OK);
            batch.incrementIgnoreCount();
            if (log.isDebugEnabled()) {
                log.debug("Batch {} is being ignored", batch.getBatchId());
            }
        }

        outgoingBatchService.updateOutgoingBatches(ignoredBatches);

        batches.filterBatchesForChannels(suspendIgnoreChannelsList.getSuspendChannels());

        // Remove non-load batches so that an initial load finishes before
        // any other batches are loaded.
        if (parameterService.is(ParameterConstants.INITIAL_LOAD_BLOCK_CHANNELS, true)) {
            if (!batches.containsBatchesInError() && batches.containsLoadBatches()) {
                batches.removeNonLoadBatches();
            }
        }

        return batches.getBatches();
    }

    public List<OutgoingBatchWithPayload> extractToPayload(ProcessInfo processInfo,
            Node targetNode, PayloadType payloadType, boolean useJdbcTimestampFormat,
            boolean useUpsertStatements, boolean useDelimiterIdentifiers) {

        OutgoingBatches batches = outgoingBatchService.getOutgoingBatches(targetNode.getNodeId(),
                false);

        if (batches.containsBatches()) {

            ChannelMap channelMap = configurationService.getSuspendIgnoreChannelLists(targetNode
                    .getNodeId());

            List<OutgoingBatch> activeBatches = filterBatchesForExtraction(batches, channelMap);

            if (activeBatches.size() > 0) {
                IDdlBuilder builder = DdlBuilderFactory.createDdlBuilder(targetNode
                        .getDatabaseType());
                if (builder == null) {
                    throw new IllegalStateException(
                            "Could not find a ddl builder registered for the database type of "
                                    + targetNode.getDatabaseType()
                                    + ".  Please check the database type setting for node '"
                                    + targetNode.getNodeId() + "'");
                }
                StructureDataWriter writer = new StructureDataWriter(
                        symmetricDialect.getPlatform(), targetNode.getDatabaseType(), payloadType,
                        useDelimiterIdentifiers, symmetricDialect.getBinaryEncoding(),
                        useJdbcTimestampFormat, useUpsertStatements);
                List<OutgoingBatch> extractedBatches = extract(processInfo, targetNode,
                        activeBatches, writer, ExtractMode.FOR_PAYLOAD_CLIENT);

                List<OutgoingBatchWithPayload> batchesWithPayload = new ArrayList<OutgoingBatchWithPayload>();
                for (OutgoingBatch batch : extractedBatches) {
                    OutgoingBatchWithPayload batchWithPayload = new OutgoingBatchWithPayload(batch,
                            payloadType);
                    batchWithPayload.setPayload(writer.getPayloadMap().get(batch.getBatchId()));
                    batchWithPayload.setPayloadType(payloadType);
                    batchesWithPayload.add(batchWithPayload);
                }

                return batchesWithPayload;

            }
        }

        return Collections.emptyList();
    }

    public List<OutgoingBatch> extract(ProcessInfo processInfo, Node targetNode,
            IOutgoingTransport transport) {

        /*
         * make sure that data is routed before extracting if the route job is
         * not configured to start automatically
         */
        if (!parameterService.is(ParameterConstants.START_ROUTE_JOB)) {
            routerService.routeData(true);
        }

        OutgoingBatches batches = outgoingBatchService.getOutgoingBatches(targetNode.getNodeId(),
                false);

        if (batches.containsBatches()) {

            ChannelMap channelMap = transport.getSuspendIgnoreChannelLists(configurationService,
                    targetNode);

            List<OutgoingBatch> activeBatches = filterBatchesForExtraction(batches, channelMap);

            if (activeBatches.size() > 0) {
                IDataWriter dataWriter = new ProtocolDataWriter(nodeService.findIdentityNodeId(),
                        transport.openWriter(), targetNode.requires13Compatiblity());

                return extract(processInfo, targetNode, activeBatches, dataWriter,
                        ExtractMode.FOR_SYM_CLIENT);
            }

        }

        return Collections.emptyList();

    }
    
    /**
     * This method will extract an outgoing batch, but will not update the outgoing batch status
     */
    public boolean extractOnlyOutgoingBatch(String nodeId, long batchId, Writer writer) {
        boolean extracted = false;
        
        Node targetNode = null;
        if (Constants.UNROUTED_NODE_ID.equals(nodeId)) {
            targetNode = new Node(nodeId, parameterService.getNodeGroupId());
        } else {
            targetNode = nodeService.findNode(nodeId);
        }
        if (targetNode != null) {
            OutgoingBatch batch = outgoingBatchService.findOutgoingBatch(batchId, nodeId);
            if (batch != null) {
                IDataWriter dataWriter = new ProtocolDataWriter(nodeService.findIdentityNodeId(),
                        writer, targetNode.requires13Compatiblity());
                List<OutgoingBatch> batches = new ArrayList<OutgoingBatch>(1);
                batches.add(batch);
                batches = extract(new ProcessInfo(), targetNode, batches, dataWriter,
                        ExtractMode.EXTRACT_ONLY);
                extracted = batches.size() > 0;
            }
        }
        return extracted;
    }

    protected List<OutgoingBatch> extract(ProcessInfo processInfo, Node targetNode,
            List<OutgoingBatch> activeBatches, IDataWriter dataWriter, ExtractMode mode) {
        boolean streamToFileEnabled = parameterService
                .is(ParameterConstants.STREAM_TO_FILE_ENABLED);
        List<OutgoingBatch> processedBatches = new ArrayList<OutgoingBatch>(activeBatches.size());
        if (activeBatches.size() > 0) {
            Set<String> channelsProcessed = new HashSet<String>();
            long batchesSelectedAtMs = System.currentTimeMillis();
            OutgoingBatch currentBatch = null;
            try {

                long bytesSentCount = 0;
                int batchesSentCount = 0;
                long maxBytesToSync = parameterService
                        .getLong(ParameterConstants.TRANSPORT_MAX_BYTES_TO_SYNC);

                for (int i = 0; i < activeBatches.size(); i++) {
                    currentBatch = activeBatches.get(i);
                    
                    channelsProcessed.add(currentBatch.getChannelId());
                    processInfo.setDataCount(currentBatch.getDataEventCount());
                    processInfo.setCurrentBatchId(currentBatch.getBatchId());
                    processInfo.setCurrentLoadId(currentBatch.getLoadId());

                    currentBatch = requeryIfEnoughTimeHasPassed(batchesSelectedAtMs, currentBatch);

                    if (currentBatch.isExtractJobFlag() && currentBatch.getStatus() != Status.IG) {
                        if (parameterService.is(ParameterConstants.INITIAL_LOAD_USE_EXTRACT_JOB)) {
                            if (currentBatch.getStatus() != Status.RQ && currentBatch.getStatus() != Status.IG
                                    && !isPreviouslyExtracted(currentBatch)) {
                                /*
                                 * the batch must have been purged. it needs to
                                 * be re-extracted
                                 */
                                log.info(
                                        "Batch {} is marked as ready but it has been deleted.  Rescheduling it for extraction",
                                        currentBatch.getNodeBatchId());
                                if (changeBatchStatus(Status.RQ, currentBatch, mode)) {
                                    resetExtractRequest(currentBatch);
                                }
                                break;
                            } else if (currentBatch.getStatus() == Status.RQ) {
                                log.info(
                                        "Batch {} is not ready for delivery.  It is currently scheduled for extraction",
                                        currentBatch.getNodeBatchId());
                                break;
                            }
                        } else {
                            currentBatch.setStatus(Status.NE);
                            currentBatch.setExtractJobFlag(false);
                        }
                    } else {
                        processInfo.setStatus(ProcessInfo.Status.EXTRACTING);
                        currentBatch = extractOutgoingBatch(processInfo, targetNode, dataWriter,
                                    currentBatch, streamToFileEnabled, true, mode);                        
                    }
                                        
                    if (streamToFileEnabled || mode == ExtractMode.FOR_PAYLOAD_CLIENT) {
                        processInfo.setStatus(ProcessInfo.Status.TRANSFERRING);
                        currentBatch = sendOutgoingBatch(processInfo, targetNode, currentBatch,
                                dataWriter, mode);
                    }
                    
                    processedBatches.add(currentBatch);

                    if (currentBatch.getStatus() != Status.OK) {
                        currentBatch.setLoadCount(currentBatch.getLoadCount() + 1);
                        changeBatchStatus(Status.LD, currentBatch, mode);

                        bytesSentCount += currentBatch.getByteCount();
                        batchesSentCount++;
                        
                        if (bytesSentCount >= maxBytesToSync && processedBatches.size() < activeBatches.size()) {
                            log.info(
                                    "Reached the total byte threshold after {} of {} batches were extracted for node '{}'.  The remaining batches will be extracted on a subsequent sync",
                                    new Object[] { batchesSentCount, activeBatches.size(),
                                            targetNode.getNodeId() });
                            break;
                        }
                    }                                       
                }

            } catch (RuntimeException e) {
                SQLException se = unwrapSqlException(e);
                if (currentBatch != null) {
                    /* Reread batch in case the ignore flag has been set */
                    currentBatch = outgoingBatchService.
                            findOutgoingBatch(currentBatch.getBatchId(), currentBatch.getNodeId());
                    statisticManager.incrementDataExtractedErrors(currentBatch.getChannelId(), 1);
                    if (se != null) {
                        currentBatch.setSqlState(se.getSQLState());
                        currentBatch.setSqlCode(se.getErrorCode());
                        currentBatch.setSqlMessage(se.getMessage());
                    } else {
                        currentBatch.setSqlMessage(getRootMessage(e));
                    }
                    currentBatch.revertStatsOnError();
                    if (currentBatch.getStatus() != Status.IG &&
                            currentBatch.getStatus() != Status.OK) {
                        currentBatch.setStatus(Status.ER);
                        currentBatch.setErrorFlag(true);
                    }
                    outgoingBatchService.updateOutgoingBatch(currentBatch);

                    if (isStreamClosedByClient(e)) {
                        log.warn(
                                "Failed to transport batch {}.  The stream was closed by the client.  There is a good chance that a previously sent batch errored out and the stream was closed or there was a network error.  The error was: {}",
                                currentBatch, getRootMessage(e));
                    } else {
                        if (e instanceof ProtocolException) {
                            IStagedResource resource = getStagedResource(currentBatch);
                            if (resource != null) {
                                resource.delete();
                            }
                        }
                        log.error("Failed to extract batch {}", currentBatch, e);
                    }
                    processInfo.setStatus(ProcessInfo.Status.ERROR);
                } else {
                    log.error("Could not log the outgoing batch status because the batch was null",
                            e);
                }
            }

            // Next, we update the node channel controls to the
            // current timestamp
            Calendar now = Calendar.getInstance();

            for (String channelProcessed : channelsProcessed) {
                NodeChannel nodeChannel = configurationService.getNodeChannel(channelProcessed,
                        targetNode.getNodeId(), false);
                if (nodeChannel != null) {
                    nodeChannel.setLastExtractTime(now.getTime());                    
                    configurationService.updateLastExtractTime(nodeChannel);
                }
            }

            return processedBatches;
        } else {
            return Collections.emptyList();
        }
    }

    final protected boolean changeBatchStatus(Status status, OutgoingBatch currentBatch, ExtractMode mode) {
        if (currentBatch.getStatus() != Status.IG) {
            currentBatch.setStatus(status);
        }
        if (mode != ExtractMode.EXTRACT_ONLY) {
            outgoingBatchService.updateOutgoingBatch(currentBatch);
            return true;
        } else {
            return false;
        }
        
    }

    /**
     * If time has passed, then re-query the batch to double check that the
     * status has not changed
     */
    final protected OutgoingBatch requeryIfEnoughTimeHasPassed(long ts, OutgoingBatch currentBatch) {
        if (System.currentTimeMillis() - ts > MS_PASSED_BEFORE_BATCH_REQUERIED) {
            currentBatch = outgoingBatchService.findOutgoingBatch(currentBatch.getBatchId(),
                    currentBatch.getNodeId());
        }
        return currentBatch;
    }

    protected OutgoingBatch extractOutgoingBatch(ProcessInfo processInfo, Node targetNode,
            IDataWriter dataWriter, OutgoingBatch currentBatch, boolean useStagingDataWriter, 
            boolean updateBatchStatistics, ExtractMode mode) {
        if (currentBatch.getStatus() != Status.OK || ExtractMode.EXTRACT_ONLY == mode) {
            
            Node sourceNode = nodeService.findIdentity();

            TransformWriter transformExtractWriter = null;
            if (useStagingDataWriter) {
                long memoryThresholdInBytes = parameterService
                        .getLong(ParameterConstants.STREAM_TO_FILE_THRESHOLD);                
                transformExtractWriter = createTransformDataWriter(
                        sourceNode,
                        targetNode,
                        new ProcessInfoDataWriter(new StagingDataWriter(memoryThresholdInBytes, nodeService
                                .findIdentityNodeId(), Constants.STAGING_CATEGORY_OUTGOING,
                                stagingManager), processInfo));
            } else {
                transformExtractWriter = createTransformDataWriter(sourceNode, targetNode,
                        new ProcessInfoDataWriter(dataWriter, processInfo));
            }

            long ts = System.currentTimeMillis();
            long extractTimeInMs = 0l;
            long byteCount = 0l;
            long transformTimeInMs = 0l;

            if (currentBatch.getStatus() == Status.IG) {
                Batch batch = new Batch(BatchType.EXTRACT, currentBatch.getBatchId(),
                        currentBatch.getChannelId(), symmetricDialect.getBinaryEncoding(),
                        sourceNode.getNodeId(), currentBatch.getNodeId(),
                        currentBatch.isCommonFlag());
                batch.setIgnored(true);
                try {
                    IStagedResource resource = getStagedResource(currentBatch);
                    if (resource != null) {
                        resource.delete();
                    }
                    DataContext ctx = new DataContext(batch);
                    ctx.put("targetNode", targetNode);
                    ctx.put("sourceNode", sourceNode);
                    transformExtractWriter.open(ctx);
                    transformExtractWriter.start(batch);
                    transformExtractWriter.end(batch, false);
                } finally {
                    transformExtractWriter.close();
                }
            } else if (!isPreviouslyExtracted(currentBatch)) {                
                int maxPermits = parameterService.getInt(ParameterConstants.CONCURRENT_WORKERS);
                String semaphoreKey = useStagingDataWriter ? Long.toString(currentBatch
                        .getBatchId()) : currentBatch.getNodeBatchId();
                Semaphore lock = null;
                try {
                    synchronized (locks) {
                        lock = locks.get(semaphoreKey);
                        if (lock == null) {
                            lock = new Semaphore(maxPermits);
                            locks.put(semaphoreKey, lock);
                        }
                        try {
                            lock.acquire();
                        } catch (InterruptedException e) {
                            throw new org.jumpmind.exception.InterruptedException(e);
                        }
                    }

                    synchronized (lock) {
                        if (!isPreviouslyExtracted(currentBatch)) {
                            currentBatch.setExtractCount(currentBatch.getExtractCount() + 1);
                            if (updateBatchStatistics) {
                                changeBatchStatus(Status.QY, currentBatch, mode);
                            }
                            currentBatch.resetStats();
                            IDataReader dataReader = new ExtractDataReader(
                                    symmetricDialect.getPlatform(), new SelectFromSymDataSource(
                                            currentBatch, sourceNode, targetNode, processInfo));
                            DataContext ctx = new DataContext();
                            ctx.put(Constants.DATA_CONTEXT_TARGET_NODE, targetNode);
                            ctx.put(Constants.DATA_CONTEXT_TARGET_NODE_ID, targetNode.getNodeId());
                            ctx.put(Constants.DATA_CONTEXT_TARGET_NODE_EXTERNAL_ID, targetNode.getExternalId());
                            ctx.put(Constants.DATA_CONTEXT_TARGET_NODE_GROUP_ID, targetNode.getNodeGroupId());
                            ctx.put(Constants.DATA_CONTEXT_TARGET_NODE, targetNode);
                            ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE, sourceNode);
                            ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE_ID, sourceNode.getNodeId());
                            ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE_EXTERNAL_ID, sourceNode.getExternalId());
                            ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE_GROUP_ID, sourceNode.getNodeGroupId());
                            
                            new DataProcessor(dataReader, transformExtractWriter, "extract").process(ctx);
                            extractTimeInMs = System.currentTimeMillis() - ts;
                            Statistics stats = transformExtractWriter.getNestedWriter()
                                    .getStatistics().values().iterator().next();
                            transformTimeInMs = stats.get(DataWriterStatisticConstants.TRANSFORMMILLIS);
                            extractTimeInMs = extractTimeInMs - transformTimeInMs;
                            byteCount = stats.get(DataWriterStatisticConstants.BYTECOUNT);
                        }
                    }
                } catch (RuntimeException ex) {
                    IStagedResource resource = getStagedResource(currentBatch);
                    if (resource != null) {
                        resource.close();
                        resource.delete();
                    }
                    throw ex;
                } finally {
                    lock.release();
                    synchronized (locks) {
                        if (lock.availablePermits() == maxPermits) {
                            locks.remove(semaphoreKey);
                        }
                    }
                }
            }

            if (updateBatchStatistics) {
                long dataEventCount = currentBatch.getDataEventCount();
                long insertEventCount = currentBatch.getInsertEventCount();
                currentBatch = requeryIfEnoughTimeHasPassed(ts, currentBatch);

                // preserve in the case of a reload event
                if (dataEventCount > currentBatch.getDataEventCount()) {
                    currentBatch.setDataEventCount(dataEventCount);
                }

                // preserve in the case of a reload event
                if (insertEventCount > currentBatch.getInsertEventCount()) {
                    currentBatch.setInsertEventCount(insertEventCount);
                }

                // only update the current batch after we have possibly
                // "re-queried"
                if (extractTimeInMs > 0) {
                    currentBatch.setExtractMillis(extractTimeInMs);
                }

                if (byteCount > 0) {
                    currentBatch.setByteCount(byteCount);
                    statisticManager.incrementDataBytesExtracted(currentBatch.getChannelId(),
                            byteCount);
                    statisticManager.incrementDataExtracted(currentBatch.getChannelId(),
                            currentBatch.getExtractCount());
                }
            }

        }

        return currentBatch;
    }

    protected IStagedResource getStagedResource(OutgoingBatch currentBatch) {
        return stagingManager.find(Constants.STAGING_CATEGORY_OUTGOING,
                currentBatch.getStagedLocation(), currentBatch.getBatchId());
    }

    protected boolean isPreviouslyExtracted(OutgoingBatch currentBatch) {
        IStagedResource previouslyExtracted = getStagedResource(currentBatch);
        if (previouslyExtracted != null && previouslyExtracted.exists()
                && previouslyExtracted.getState() != State.CREATE) {
            if (log.isDebugEnabled()) {
                log.debug("We have already extracted batch {}.  Using the existing extraction: {}",
                        currentBatch.getBatchId(), previouslyExtracted);
            }
            return true;
        } else {
            return false;
        }
    }

    protected OutgoingBatch sendOutgoingBatch(ProcessInfo processInfo, Node targetNode,
            OutgoingBatch currentBatch, IDataWriter dataWriter, ExtractMode mode) {
        if (currentBatch.getStatus() != Status.OK || ExtractMode.EXTRACT_ONLY == mode) {
            currentBatch.setSentCount(currentBatch.getSentCount() + 1);
            changeBatchStatus(Status.SE, currentBatch, mode);

            long ts = System.currentTimeMillis();

            IStagedResource extractedBatch = getStagedResource(currentBatch);
            if (extractedBatch != null) {
                IDataReader dataReader = new ProtocolDataReader(BatchType.EXTRACT,
                        currentBatch.getNodeId(), extractedBatch);

                DataContext ctx = new DataContext();
                ctx.put(Constants.DATA_CONTEXT_TARGET_NODE, targetNode);
                ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE, nodeService.findIdentity());
                new DataProcessor(dataReader, new ProcessInfoDataWriter(dataWriter, processInfo), "send from stage")
                        .process(ctx);
                if (dataWriter.getStatistics().size() > 0) {
                    Statistics stats = dataWriter.getStatistics().values().iterator().next();
                    statisticManager.incrementDataSent(currentBatch.getChannelId(),
                            stats.get(DataWriterStatisticConstants.STATEMENTCOUNT));
                    long byteCount = stats.get(DataWriterStatisticConstants.BYTECOUNT);
                    statisticManager.incrementDataBytesSent(currentBatch.getChannelId(), byteCount);
                } else {
                    log.warn("Could not find recorded statistics for batch {}",
                            currentBatch.getNodeBatchId());
                }

            } else {
                throw new IllegalStateException(String.format(
                        "Could not find the staged resource for batch %s",
                        currentBatch.getNodeBatchId()));
            }

            currentBatch = requeryIfEnoughTimeHasPassed(ts, currentBatch);

        }

        return currentBatch;

    }

    public boolean extractBatchRange(Writer writer, String nodeId, long startBatchId,
            long endBatchId) {
        boolean foundBatch = false;
        Node sourceNode = nodeService.findIdentity();
        for (long batchId = startBatchId; batchId <= endBatchId; batchId++) {
            OutgoingBatch batch = outgoingBatchService.findOutgoingBatch(batchId, nodeId);
            if (batch != null) {
                Node targetNode = nodeService.findNode(nodeId);
                if (targetNode == null && Constants.UNROUTED_NODE_ID.equals(nodeId)) {
                    targetNode = new Node();
                    targetNode.setNodeId("-1");
                }
                if (targetNode != null) {
                    IDataReader dataReader = new ExtractDataReader(symmetricDialect.getPlatform(),
                            new SelectFromSymDataSource(batch, sourceNode, targetNode, new ProcessInfo()));
                    DataContext ctx = new DataContext();
                    ctx.put(Constants.DATA_CONTEXT_TARGET_NODE, targetNode);
                    ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE, nodeService.findIdentity());
                    new DataProcessor(dataReader, createTransformDataWriter(
                            nodeService.findIdentity(), targetNode,
                            new ProtocolDataWriter(nodeService.findIdentityNodeId(), writer,
                                    targetNode.requires13Compatiblity())), "extract range").process(ctx);
                    foundBatch = true;
                }
            }
        }
        return foundBatch;
    }

    public boolean extractBatchRange(Writer writer, String nodeId, Date startBatchTime,
            Date endBatchTime, String... channelIds) {
        boolean foundBatch = false;
        Node sourceNode = nodeService.findIdentity();
        OutgoingBatches batches = outgoingBatchService.getOutgoingBatchRange(nodeId,
                startBatchTime, endBatchTime, channelIds);
        List<OutgoingBatch> list = batches.getBatches();
        for (OutgoingBatch outgoingBatch : list) {
            Node targetNode = nodeService.findNode(nodeId);
            if (targetNode == null && Constants.UNROUTED_NODE_ID.equals(nodeId)) {
                targetNode = new Node();
                targetNode.setNodeId("-1");
            }
            if (targetNode != null) {
                IDataReader dataReader = new ExtractDataReader(symmetricDialect.getPlatform(),
                        new SelectFromSymDataSource(outgoingBatch, sourceNode, targetNode, new ProcessInfo()));
                DataContext ctx = new DataContext();
                ctx.put(Constants.DATA_CONTEXT_TARGET_NODE, targetNode);
                ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE, nodeService.findIdentity());
                new DataProcessor(dataReader, createTransformDataWriter(nodeService.findIdentity(),
                        targetNode, new ProtocolDataWriter(nodeService.findIdentityNodeId(),
                                writer, targetNode.requires13Compatiblity())), "extract range").process(ctx);
                foundBatch = true;
            }
        }
        return foundBatch;
    }

    protected TransformWriter createTransformDataWriter(Node identity, Node targetNode,
            IDataWriter extractWriter) {
        List<TransformTableNodeGroupLink> transformsList = null;
        if (targetNode != null) {
            transformsList = transformService.findTransformsFor(
                    new NodeGroupLink(identity.getNodeGroupId(), targetNode.getNodeGroupId()),
                    TransformPoint.EXTRACT);
        }
        TransformTable[] transforms = transformsList != null ? transformsList
                .toArray(new TransformTable[transformsList.size()]) : null;
        TransformWriter transformExtractWriter = new TransformWriter(
                symmetricDialect.getPlatform(), TransformPoint.EXTRACT, extractWriter, 
                transformService.getColumnTransforms(), transforms);
        return transformExtractWriter;
    }

    protected Table lookupAndOrderColumnsAccordingToTriggerHistory(String routerId,
            TriggerHistory triggerHistory, boolean setTargetTableName, boolean useDatabaseDefinition) {
        String catalogName = triggerHistory.getSourceCatalogName();
        String schemaName = triggerHistory.getSourceSchemaName();
        String tableName = triggerHistory.getSourceTableName();
        Table table = null;
        if (useDatabaseDefinition) {
            table = platform.getTableFromCache(catalogName, schemaName, tableName, false);
            
            if (table != null && table.getColumnCount() < triggerHistory.getParsedColumnNames().length) {
                /*
                 * If the column count is less than what trigger history reports, then
                 * chances are the table cache is out of date.
                 */
                table = platform.getTableFromCache(catalogName, schemaName, tableName, true);
            }

            if (table != null) {
                table = table.copyAndFilterColumns(triggerHistory.getParsedColumnNames(),
                    triggerHistory.getParsedPkColumnNames(), true);
            } else {
                throw new SymmetricException("Could not find the following table.  It might have been dropped: %s", Table.getFullyQualifiedTableName(catalogName, schemaName, tableName));
            }
        } else {
            table = new Table(tableName);
            table.addColumns(triggerHistory.getParsedColumnNames());
            table.setPrimaryKeys(triggerHistory.getParsedPkColumnNames());
        }

        Router router = triggerRouterService.getRouterById(routerId, false);        
        if (router != null && setTargetTableName) {            
            if (router.isUseSourceCatalogSchema()) {
                table.setCatalog(catalogName);
                table.setSchema(schemaName);
            } else {
                table.setCatalog(null);
                table.setSchema(null);
            }
            
            if (StringUtils.equals(Constants.NONE_TOKEN, router.getTargetCatalogName())) {
                table.setCatalog(null);
            } else if (StringUtils.isNotBlank(router.getTargetCatalogName())) {
                table.setCatalog(router.getTargetCatalogName());
            }

            if (StringUtils.equals(Constants.NONE_TOKEN, router.getTargetSchemaName())) {
                table.setSchema(null);
            } else if (StringUtils.isNotBlank(router.getTargetSchemaName())) {
                table.setSchema(router.getTargetSchemaName());
            }

            if (StringUtils.isNotBlank(router.getTargetTableName())) {
                table.setName(router.getTargetTableName());
            }
        }
        return table;
    }

    public RemoteNodeStatuses queueWork(boolean force) {
        final RemoteNodeStatuses statuses = new RemoteNodeStatuses(configurationService.getChannels(false));
        Node identity = nodeService.findIdentity();
        if (identity != null) {
            if (force || clusterService.lock(ClusterConstants.INITIAL_LOAD_EXTRACT)) {
                try {
                    List<String> nodeIds = getExtractRequestNodes();
                    for (String nodeId : nodeIds) {
                        queue(nodeId, statuses);
                    }
                } finally {
                    if (!force) {
                        clusterService.unlock(ClusterConstants.INITIAL_LOAD_EXTRACT);
                    }
                }
            }
        } else {
            log.debug("Not running initial load extract service because this node does not have an identity");
        }
        return statuses;
    }

    protected void queue(String nodeId, RemoteNodeStatuses statuses) {
        final NodeCommunication.CommunicationType TYPE = NodeCommunication.CommunicationType.EXTRACT;
        int availableThreads = nodeCommunicationService.getAvailableThreads(TYPE);
        NodeCommunication lock = nodeCommunicationService.find(nodeId, TYPE);
        if (availableThreads > 0) {
            nodeCommunicationService.execute(lock, statuses, this);
        }
    }

    public List<String> getExtractRequestNodes() {
        return sqlTemplate.query(getSql("selectNodeIdsForExtractSql"), SqlConstants.STRING_MAPPER,
                ExtractStatus.NE.name());
    }

    public List<ExtractRequest> getExtractRequestsForNode(String nodeId) {
        return sqlTemplate.query(getSql("selectExtractRequestForNodeSql"),
                new ExtractRequestMapper(), nodeId, ExtractRequest.ExtractStatus.NE.name());
    }

    protected void resetExtractRequest(OutgoingBatch batch) {
        sqlTemplate.update(getSql("resetExtractRequestStatus"), ExtractStatus.NE.name(),
                batch.getBatchId(), batch.getBatchId(), batch.getNodeId());
    }

    public void requestExtractRequest(ISqlTransaction transaction, String nodeId,
            TriggerRouter triggerRouter, long startBatchId, long endBatchId) {
        long requestId = sequenceService.nextVal(transaction, Constants.SEQUENCE_EXTRACT_REQ);
        transaction.prepareAndExecute(getSql("insertExtractRequestSql"),
                new Object[] { requestId, nodeId, ExtractStatus.NE.name(), startBatchId,
                        endBatchId, triggerRouter.getTrigger().getTriggerId(),
                        triggerRouter.getRouter().getRouterId() }, new int[] { Types.BIGINT, Types.VARCHAR,
                        Types.VARCHAR, Types.BIGINT, Types.BIGINT, Types.VARCHAR, Types.VARCHAR });
    }

    protected void updateExtractRequestStatus(ISqlTransaction transaction, long extractId,
            ExtractStatus status) {
        transaction.prepareAndExecute(getSql("updateExtractRequestStatus"), status.name(),
                extractId);
    }

    /**
     * This is a callback method used by the NodeCommunicationService that extracts an initial load
     * in the background.
     */
    public void execute(NodeCommunication nodeCommunication, RemoteNodeStatus status) {
        List<ExtractRequest> requests = getExtractRequestsForNode(nodeCommunication.getNodeId());
        long ts = System.currentTimeMillis();
        /*
         * Process extract requests until it has taken longer than 30 seconds, and then
         * allow the process to return so progress status can be seen.
         */
        for (int i = 0; i < requests.size()
                && (System.currentTimeMillis() - ts) <= Constants.LONG_OPERATION_THRESHOLD; i++) {
            ExtractRequest request = requests.get(i);
            Node identity = nodeService.findIdentity();
            Node targetNode = nodeService.findNode(nodeCommunication.getNodeId());
            log.info(
                    "Extracting batches for request {}. Starting at batch {}.  Ending at batch {}",
                    new Object[] { request.getRequestId(), request.getStartBatchId(),
                            request.getEndBatchId() });
            List<OutgoingBatch> batches = outgoingBatchService.getOutgoingBatchRange(
                    request.getStartBatchId(), request.getEndBatchId()).getBatches();

            ProcessInfo processInfo = statisticManager.newProcessInfo(new ProcessInfoKey(identity
                    .getNodeId(), nodeCommunication.getNodeId(),
                    ProcessInfoKey.ProcessType.INITIAL_LOAD_EXTRACT_JOB));
            try {
                boolean areBatchesOk = true;

                /*
                 * check to see if batches have been OK'd by another reload
                 * request 
                 */
                for (OutgoingBatch outgoingBatch : batches) {
                    if (outgoingBatch.getStatus() != Status.OK) {
                        areBatchesOk = false;
                    }
                }

                if (!areBatchesOk) {

                    Channel channel = configurationService
                            .getChannel(batches.get(0).getChannelId());
                    /*
                     * "Trick" the extractor to extract one reload batch, but we
                     * will split it across the N batches when writing it
                     */
                    extractOutgoingBatch(processInfo, targetNode,
                            new MultiBatchStagingWriter(identity.getNodeId(), stagingManager,
                                    batches, channel.getMaxBatchSize()), batches.get(0), false,
                            false, ExtractMode.FOR_SYM_CLIENT);

                } else {
                    log.info("Batches already had an OK status for request {}, batches {} to {}.  Not extracting", new Object[] { request.getRequestId(), request.getStartBatchId(),
                            request.getEndBatchId() });
                }

                /*
                 * re-query the batches to see if they have been OK'd while
                 * extracting
                 */
                List<OutgoingBatch> checkBatches = outgoingBatchService.getOutgoingBatchRange(
                        request.getStartBatchId(), request.getEndBatchId()).getBatches();

                areBatchesOk = true;

                /*
                 * check to see if batches have been OK'd by another reload
                 * request while extracting
                 */
                for (OutgoingBatch outgoingBatch : checkBatches) {
                    if (outgoingBatch.getStatus() != Status.OK) {
                        areBatchesOk = false;
                    }
                }

                ISqlTransaction transaction = null;
                try {
                    transaction = sqlTemplate.startSqlTransaction();
                    updateExtractRequestStatus(transaction, request.getRequestId(),
                            ExtractStatus.OK);

                    if (!areBatchesOk) {
                        for (OutgoingBatch outgoingBatch : batches) {
                            if (parameterService.is(ParameterConstants.INITIAL_LOAD_EXTRACT_AND_SEND_WHEN_STAGED, false)) {
                            	if (outgoingBatch.getStatus() == Status.RQ) {
                            		outgoingBatch.setStatus(Status.NE);
                                	outgoingBatchService.updateOutgoingBatch(transaction, outgoingBatch);
                            	}
                            } else {
                            	outgoingBatch.setStatus(Status.NE);
                            	outgoingBatchService.updateOutgoingBatch(transaction, outgoingBatch);
                            }
                        }
                    } else {
                        log.info("Batches already had an OK status for request {}, batches {} to {}.  Not updating the status to NE", new Object[] { request.getRequestId(), request.getStartBatchId(),
                                request.getEndBatchId() });
                    }
                    transaction.commit();

                } catch (Error ex) {
                    if (transaction != null) {
                        transaction.rollback();
                    }
                    throw ex;
                } catch (RuntimeException ex) {
                    if (transaction != null) {
                        transaction.rollback();
                    }
                    throw ex;
                } finally {
                    close(transaction);
                }
                processInfo.setStatus(org.jumpmind.symmetric.model.ProcessInfo.Status.OK);

            } catch (RuntimeException ex) {
                log.debug(
                        "Failed to extract batches for request {}. Starting at batch {}.  Ending at batch {}",
                        new Object[] { request.getRequestId(), request.getStartBatchId(),
                                request.getEndBatchId() });
                processInfo.setStatus(org.jumpmind.symmetric.model.ProcessInfo.Status.ERROR);
                throw ex;
            }
        }
    }

    class ExtractRequestMapper implements ISqlRowMapper<ExtractRequest> {
        public ExtractRequest mapRow(Row row) {
            ExtractRequest request = new ExtractRequest();
            request.setNodeId(row.getString("node_id"));
            request.setRequestId(row.getLong("request_id"));
            request.setStartBatchId(row.getLong("start_batch_id"));
            request.setEndBatchId(row.getLong("end_batch_id"));
            request.setStatus(ExtractStatus.valueOf(row.getString("status").toUpperCase()));
            request.setCreateTime(row.getDateTime("create_time"));
            request.setLastUpdateTime(row.getDateTime("last_update_time"));
            request.setTriggerRouter(triggerRouterService.findTriggerRouterById(
                    row.getString("trigger_id"), row.getString("router_id")));
            return request;
        }
    }

    public class MultiBatchStagingWriter implements IDataWriter {

        long maxBatchSize;

        StagingDataWriter currentDataWriter;

        List<OutgoingBatch> batches;
        
        List<OutgoingBatch> finishedBatches;

        IStagingManager stagingManager;

        String sourceNodeId;

        DataContext context;

        Table table;

        OutgoingBatch outgoingBatch;

        Batch batch;

        boolean inError = false;

        public MultiBatchStagingWriter(String sourceNodeId, IStagingManager stagingManager,
                List<OutgoingBatch> batches, long maxBatchSize) {
            this.sourceNodeId = sourceNodeId;
            this.maxBatchSize = maxBatchSize;
            this.stagingManager = stagingManager;
            this.batches = new ArrayList<OutgoingBatch>(batches);
            this.finishedBatches = new ArrayList<OutgoingBatch>(batches.size());
        }

        public void open(DataContext context) {
            this.context = context;
            this.nextBatch();
            long memoryThresholdInBytes = parameterService
                    .getLong(ParameterConstants.STREAM_TO_FILE_THRESHOLD);
            this.currentDataWriter = new StagingDataWriter(memoryThresholdInBytes, sourceNodeId,
                    Constants.STAGING_CATEGORY_OUTGOING, stagingManager,
                    (IProtocolDataWriterListener[]) null);
            this.currentDataWriter.open(context);
        }

        public void close() {
           if (this.currentDataWriter != null) {
               this.currentDataWriter.close();
           }
        }

        public Map<Batch, Statistics> getStatistics() {
            return currentDataWriter.getStatistics();
        }

        public void start(Batch batch) {
            this.batch = batch;
            this.currentDataWriter.start(batch);
        }

        public boolean start(Table table) {
            this.table = table;
            this.currentDataWriter.start(table);
            return true;
        }

        public void write(CsvData data) {
            this.outgoingBatch.incrementDataEventCount();
            this.outgoingBatch.incrementInsertEventCount();
            this.currentDataWriter.write(data);            
            if (this.outgoingBatch.getDataEventCount() >= maxBatchSize && this.batches.size() > 0) {
                this.currentDataWriter.end(table);
                this.currentDataWriter.end(batch, false);
                Statistics stats = this.currentDataWriter.getStatistics().get(batch);
                this.outgoingBatch.setByteCount(stats.get(DataWriterStatisticConstants.BYTECOUNT));
                this.outgoingBatch.setExtractMillis(System.currentTimeMillis() - batch.getStartTime().getTime());
                this.currentDataWriter.close();   
                checkSend();
                startNewBatch();
            }

        }

        public void checkSend() {
        	if (parameterService.is(ParameterConstants.INITIAL_LOAD_EXTRACT_AND_SEND_WHEN_STAGED, false)) {
        		this.outgoingBatch.setStatus(Status.NE);
            	ISqlTransaction transaction = null;
                try {
                    transaction = sqlTemplate.startSqlTransaction();
                    outgoingBatchService.updateOutgoingBatch(transaction, this.outgoingBatch);
                    transaction.commit();
                } catch (Error ex) {
                    if (transaction != null) {
                        transaction.rollback();
                    }
                    throw ex;
                } catch (RuntimeException ex) {
                    if (transaction != null) {
                        transaction.rollback();
                    }
                    throw ex;
                } finally {
                	if (transaction != null) {
                        transaction.close();
                    }
                }
            }
        }
        
        public void end(Table table) {
            if (this.currentDataWriter != null) {
                this.currentDataWriter.end(table);
                Statistics stats = this.currentDataWriter.getStatistics().get(batch);
                this.outgoingBatch.setByteCount(stats.get(DataWriterStatisticConstants.BYTECOUNT));
                this.outgoingBatch.setExtractMillis(System.currentTimeMillis() - batch.getStartTime().getTime());
            }
        }

        public void end(Batch batch, boolean inError) {
            this.inError = inError;
            if (this.currentDataWriter != null) {
                this.currentDataWriter.end(this.batch, inError);
            }
        }
        
        protected void nextBatch() {
            if (this.outgoingBatch != null) {
                this.finishedBatches.add(outgoingBatch);
            }
            this.outgoingBatch = this.batches.remove(0);
            this.outgoingBatch.setDataEventCount(0);
            this.outgoingBatch.setInsertEventCount(0);
            
            /*
             * Update the last update time so the batch 
             * isn't purged prematurely
             */
            for (OutgoingBatch batch : finishedBatches) {
                IStagedResource resource = getStagedResource(batch);
                if (resource != null) {
                    resource.refreshLastUpdateTime();
                }
            }
        }

        protected void startNewBatch() {
            this.nextBatch();
            long memoryThresholdInBytes = parameterService
                    .getLong(ParameterConstants.STREAM_TO_FILE_THRESHOLD);            
            this.currentDataWriter = new StagingDataWriter(memoryThresholdInBytes, sourceNodeId,
                    Constants.STAGING_CATEGORY_OUTGOING, stagingManager,
                    (IProtocolDataWriterListener[]) null);
            this.batch = new Batch(BatchType.EXTRACT, outgoingBatch.getBatchId(),
                    outgoingBatch.getChannelId(), symmetricDialect.getBinaryEncoding(),
                    sourceNodeId, outgoingBatch.getNodeId(), false);
            this.currentDataWriter.open(context);
            this.currentDataWriter.start(batch);
            this.currentDataWriter.start(table);
        }

    }

    class SelectFromSymDataSource implements IExtractDataReaderSource {

        private Batch batch;

        private OutgoingBatch outgoingBatch;

        private Table targetTable;

        private Table sourceTable;

        private TriggerHistory lastTriggerHistory;
        
        private String lastRouterId;

        private boolean requiresLobSelectedFromSource;

        private ISqlReadCursor<Data> cursor;

        private SelectFromTableSource reloadSource;

        private Node targetNode;
        
        private ProcessInfo processInfo;

        public SelectFromSymDataSource(OutgoingBatch outgoingBatch, 
                Node sourceNode, Node targetNode, ProcessInfo processInfo) {
            this.processInfo = processInfo;
            this.outgoingBatch = outgoingBatch;
            this.batch = new Batch(BatchType.EXTRACT, outgoingBatch.getBatchId(),
                    outgoingBatch.getChannelId(), symmetricDialect.getBinaryEncoding(),
                    sourceNode.getNodeId(), outgoingBatch.getNodeId(), outgoingBatch.isCommonFlag());
            this.targetNode = targetNode;
        }

        public Batch getBatch() {
            return batch;
        }

        public Table getSourceTable() {
            return sourceTable;
        }

        public Table getTargetTable() {
            return targetTable;
        }

        public CsvData next() {
            if (this.cursor == null) {
                this.cursor = dataService.selectDataFor(batch);
            }

            Data data = null;
            if (reloadSource != null) {
                data = (Data) reloadSource.next();
                targetTable = reloadSource.getTargetTable();
                sourceTable = reloadSource.getSourceTable();
                if (data == null) {
                    reloadSource.close();
                    reloadSource = null;
                }
            }

            if (data == null) {
                data = this.cursor.next();
                if (data != null) {
                    TriggerHistory triggerHistory = data.getTriggerHistory();
                    String routerId = data.getAttribute(CsvData.ATTRIBUTE_ROUTER_ID);

                    if (data.getDataEventType() == DataEventType.RELOAD) {

                        String triggerId = triggerHistory.getTriggerId();

                        TriggerRouter triggerRouter = triggerRouterService
                                .getTriggerRouterForCurrentNode(triggerId, routerId, false);
                        if (triggerRouter != null) {
                            processInfo.setCurrentTableName(triggerHistory.getSourceTableName());
                            SelectFromTableEvent event = new SelectFromTableEvent(targetNode,
                                    triggerRouter, triggerHistory, data.getRowData());
                            this.reloadSource = new SelectFromTableSource(outgoingBatch, batch,
                                    event);
                            data = (Data) this.reloadSource.next();
                            this.sourceTable = reloadSource.getSourceTable();
                            this.targetTable = this.reloadSource.getTargetTable();
                            this.requiresLobSelectedFromSource = this.reloadSource
                                    .requiresLobsSelectedFromSource();
                            
                            if (data == null) {
                                data = (Data)next();
                            }
                        } else {
                            log.warn(
                                    "Could not find trigger router definition for {}:{}.  Skipping reload event with the data id of {}",
                                    new Object[] { triggerId, routerId, data.getDataId() });
                            return next();
                        }
                    } else {
                        Trigger trigger = triggerRouterService.getTriggerById(
                                triggerHistory.getTriggerId(), false);
                        if (trigger != null || triggerHistory.getTriggerId().equals(AbstractFileParsingRouter.TRIGGER_ID_FILE_PARSER)) {
                            if (lastTriggerHistory == null || lastTriggerHistory
                                    .getTriggerHistoryId() != triggerHistory.getTriggerHistoryId() || 
                                    lastRouterId == null || !lastRouterId.equals(routerId)) {
                                this.sourceTable = lookupAndOrderColumnsAccordingToTriggerHistory(
                                        routerId, triggerHistory, false, true);
                                this.targetTable = lookupAndOrderColumnsAccordingToTriggerHistory(
                                        routerId, triggerHistory, true, false);
                                this.requiresLobSelectedFromSource = trigger == null ? false : trigger.isUseStreamLobs();
                            }

                            data.setNoBinaryOldData(requiresLobSelectedFromSource
                                    || symmetricDialect.getName().equals(
                                            DatabaseNamesConstants.MSSQL2000)
                                    || symmetricDialect.getName().equals(
                                            DatabaseNamesConstants.MSSQL2005)
                                    || symmetricDialect.getName().equals(
                                            DatabaseNamesConstants.MSSQL2008));
                            
                            outgoingBatch.incrementDataEventCount();
                        } else {
                            log.error(
                                    "Could not locate a trigger with the id of {} for {}.  It was recorded in the hist table with a hist id of {}",
                                    new Object[] { triggerHistory.getTriggerId(),
                                            triggerHistory.getSourceTableName(),
                                            triggerHistory.getTriggerHistoryId() });
                        }
                        if (data.getDataEventType() == DataEventType.CREATE && StringUtils.isBlank(data.getCsvData(CsvData.ROW_DATA))) {                          

                            boolean excludeDefaults = parameterService.is(ParameterConstants.CREATE_TABLE_WITHOUT_DEFAULTS, false);
                            boolean excludeForeignKeys = parameterService.is(ParameterConstants.CREATE_TABLE_WITHOUT_FOREIGN_KEYS, false);
                            
                            /*
                             * Force a reread of table so new columns are picked up.  A create
                             * event is usually sent after there is a change to the table so 
                             * we want to make sure that the cache is updated
                             */
                            this.sourceTable = platform.getTableFromCache(sourceTable.getCatalog(), 
                                    sourceTable.getSchema(), sourceTable.getName(), true);
                            
                            this.targetTable = lookupAndOrderColumnsAccordingToTriggerHistory(
                                    routerId, triggerHistory, true, true);
                            
                            Database db = new Database();
                            db.setName("dataextractor");                            
                            db.setCatalog(targetTable.getCatalog());
                            db.setSchema(targetTable.getSchema());
                            db.addTable(targetTable);
                            if (excludeDefaults) {
                                Column[] columns = targetTable.getColumns();
                                for (Column column : columns) {
                                    column.setDefaultValue(null);
                                    Map<String, PlatformColumn> platformColumns = column.getPlatformColumns();
                                    if (platformColumns != null) {
                                        Collection<PlatformColumn> cols = platformColumns.values();
                                        for (PlatformColumn platformColumn : cols) {
                                            platformColumn.setDefaultValue(null);
                                        }
                                    }
                                }
                            }
                            if (excludeForeignKeys) {
                                targetTable.removeAllForeignKeys();
                            }
                            data.setRowData(CsvUtils.escapeCsvData(DatabaseXmlUtil.toXml(db)));
                        }
                    }

                    lastTriggerHistory = triggerHistory;
                    lastRouterId = routerId;
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

    class SelectFromTableSource implements IExtractDataReaderSource {

        private OutgoingBatch outgoingBatch;

        private Batch batch;

        private Table targetTable;

        private Table sourceTable;

        private List<SelectFromTableEvent> selectFromTableEventsToSend;

        private SelectFromTableEvent currentInitialLoadEvent;

        private ISqlReadCursor<Data> cursor;

        private SimpleRouterContext routingContext;

        private Node node;

        private TriggerRouter triggerRouter;

        public SelectFromTableSource(OutgoingBatch outgoingBatch, Batch batch,
                SelectFromTableEvent event) {
            this.outgoingBatch = outgoingBatch;
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
            this.node = nodeService.findNode(batch.getTargetNodeId());
            if (node == null) {
                throw new SymmetricException("Could not find a node represented by %s",
                        this.batch.getTargetNodeId());
            }
        }

        public Table getSourceTable() {
            return sourceTable;
        }

        public Batch getBatch() {
            return batch;
        }

        public Table getTargetTable() {
            return targetTable;
        }

        public CsvData next() {
            CsvData data = null;
            do {
                data = selectNext();
            } while (data != null
                    && routingContext != null
                    && !routerService.shouldDataBeRouted(routingContext,
                            new DataMetaData((Data) data, sourceTable, triggerRouter.getRouter(),
                                    routingContext.getChannel()), node, true, StringUtils
                                    .isNotBlank(triggerRouter.getInitialLoadSelect()), triggerRouter));

            if (data != null && outgoingBatch != null && !outgoingBatch.isExtractJobFlag()) {
                outgoingBatch.incrementDataEventCount();
                outgoingBatch.incrementEventCount(data.getDataEventType());
            }

            return data;
        }

        protected CsvData selectNext() {
            CsvData data = null;
            if (this.currentInitialLoadEvent == null && selectFromTableEventsToSend.size() > 0) {
                this.currentInitialLoadEvent = selectFromTableEventsToSend.remove(0);
                TriggerHistory history = this.currentInitialLoadEvent.getTriggerHistory();
                if (this.currentInitialLoadEvent.containsData()) {
                    data = this.currentInitialLoadEvent.getData();
                    this.currentInitialLoadEvent = null;
                    this.sourceTable = lookupAndOrderColumnsAccordingToTriggerHistory(
                            (String) data.getAttribute(CsvData.ATTRIBUTE_ROUTER_ID), history, false, true);
                    this.targetTable = lookupAndOrderColumnsAccordingToTriggerHistory(
                            (String) data.getAttribute(CsvData.ATTRIBUTE_ROUTER_ID), history, true, false);
                } else {
                    this.triggerRouter = this.currentInitialLoadEvent.getTriggerRouter();
                    if (this.routingContext == null) {
                        NodeChannel channel = batch != null ? configurationService.getNodeChannel(
                                batch.getChannelId(), false) : new NodeChannel(this.triggerRouter
                                .getTrigger().getChannelId());
                        this.routingContext = new SimpleRouterContext(batch.getTargetNodeId(),
                                channel);
                    }
                    this.sourceTable = lookupAndOrderColumnsAccordingToTriggerHistory(triggerRouter
                            .getRouter().getRouterId(), history, false, true);
                    this.targetTable = lookupAndOrderColumnsAccordingToTriggerHistory(triggerRouter
                            .getRouter().getRouterId(), history, true, false);
                    this.startNewCursor(history, triggerRouter,
                            this.currentInitialLoadEvent.getInitialLoadSelect());

                }

            }

            if (this.cursor != null) {
                data = this.cursor.next();
                if (data == null) {
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
                final TriggerRouter triggerRouter, String overrideSelectSql) {
            final String initialLoadSql = symmetricDialect.createInitialLoadSqlFor(
                    this.currentInitialLoadEvent.getNode(), triggerRouter, sourceTable,
                    triggerHistory,
                    configurationService.getChannel(triggerRouter.getTrigger().getChannelId()),
                    overrideSelectSql);

            final int expectedCommaCount = triggerHistory.getParsedColumnNames().length - 1;
            final boolean selectedAsCsv = symmetricDialect.getParameterService().is(
                    ParameterConstants.INITIAL_LOAD_CONCAT_CSV_IN_SQL_ENABLED); 
            final boolean objectValuesWillNeedEscaped = !symmetricDialect.getTriggerTemplate()
                    .useTriggerTemplateForColumnTemplatesDuringInitialLoad();
            
            this.cursor = sqlTemplate.queryForCursor(initialLoadSql, new ISqlRowMapper<Data>() {
                public Data mapRow(Row row) {
                    String csvRow = null;                    
                    if (selectedAsCsv) {
                        csvRow = row.stringValue();
                    } else if (objectValuesWillNeedEscaped) {
                        String[] rowData = platform.getStringValues(
                                symmetricDialect.getBinaryEncoding(), sourceTable.getColumns(),
                                row, false, true);
                        csvRow = CsvUtils.escapeCsvData(rowData, '\0', '"');
                    } else {
                        csvRow = row.csvValue();

                    }
                    int commaCount = StringUtils.countMatches(csvRow, ",");
                    if (expectedCommaCount <= commaCount) {
                        Data data = new Data(0, null, csvRow, DataEventType.INSERT, triggerHistory
                                .getSourceTableName(), null, triggerHistory, batch.getChannelId(),
                                null, null);
                        data.putAttribute(Data.ATTRIBUTE_ROUTER_ID, triggerRouter.getRouter()
                                .getRouterId());
                        return data;
                    } else {
                        throw new SymmetricException(
                                "The extracted row data did not have the expected (%d) number of columns (actual=%s): %s.  The initial load sql was: %s",
                                expectedCommaCount, commaCount, csvRow, initialLoadSql);
                    }
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
        private String initialLoadSelect;

        public SelectFromTableEvent(Node node, TriggerRouter triggerRouter,
                TriggerHistory triggerHistory, String initialLoadSelect) {
            this.node = node;
            this.triggerRouter = triggerRouter;
            this.initialLoadSelect = initialLoadSelect;
            Trigger trigger = triggerRouter.getTrigger();
            this.triggerHistory = triggerHistory != null ? triggerHistory : triggerRouterService
                    .getNewestTriggerHistoryForTrigger(trigger.getTriggerId(),
                            trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                            trigger.getSourceTableName());
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

        public String getInitialLoadSelect() {
            return initialLoadSelect;
        }

    }

}
