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

import java.io.EOFException;
import java.io.OutputStream;
import java.io.Writer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.Batch.BatchType;
import org.jumpmind.symmetric.io.data.CsvData;
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
import org.jumpmind.symmetric.io.data.writer.ProtocolDataWriter;
import org.jumpmind.symmetric.io.data.writer.StagingDataWriter;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfoDataWriter;
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
import org.jumpmind.util.Statistics;

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

    private IStagingManager stagingManager;
    
    private Map<Long, Semaphore> locks = new HashMap<Long, Semaphore>();

    public DataExtractorService(IParameterService parameterService,
            ISymmetricDialect symmetricDialect, IOutgoingBatchService outgoingBatchService,
            IRouterService routingService, IConfigurationService configurationService,
            ITriggerRouterService triggerRouterService, INodeService nodeService,
            IDataService dataService, ITransformService transformService,
            IStatisticManager statisticManager, IStagingManager stagingManager) {
        super(parameterService, symmetricDialect);
        this.outgoingBatchService = outgoingBatchService;
        this.routerService = routingService;
        this.dataService = dataService;
        this.configurationService = configurationService;
        this.triggerRouterService = triggerRouterService;
        this.nodeService = nodeService;
        this.transformService = transformService;
        this.statisticManager = statisticManager;
        this.stagingManager = stagingManager;
    }

    /**
     * @see DataExtractorService#extractConfigurationStandalone(Node, Writer)
     */
    public void extractConfigurationStandalone(Node node, OutputStream out) {
        this.extractConfigurationStandalone(node, TransportUtils.toWriter(out));
    }

    /**
     * Extract the SymmetricDS configuration for the passed in {@link Node}.
     * Note that this method will insert an already acknowledged batch to
     * indicate that the configuration was sent. If the configuration fails to
     * load for some reason on the client the batch status will NOT reflect the
     * failure.
     */
    public void extractConfigurationStandalone(Node targetNode, Writer writer,
            String... tablesToExclude) {
        Node sourceNode = nodeService.findIdentity();

        Batch batch = new Batch(BatchType.EXTRACT, Constants.VIRTUAL_BATCH_FOR_REGISTRATION,
                Constants.CHANNEL_CONFIG, symmetricDialect.getBinaryEncoding(),
                sourceNode.getNodeId(), targetNode.getNodeId(), false);

        NodeGroupLink nodeGroupLink = new NodeGroupLink(parameterService.getNodeGroupId(),
                targetNode.getNodeGroupId());

        List<TriggerRouter> triggerRouters = triggerRouterService
                .buildTriggerRoutersForSymmetricTables(
                        StringUtils.isBlank(targetNode.getSymmetricVersion()) ? Version.version()
                                : targetNode.getSymmetricVersion(), nodeGroupLink, tablesToExclude);

        List<SelectFromTableEvent> initialLoadEvents = new ArrayList<SelectFromTableEvent>(
                triggerRouters.size() * 2);

        for (int i = triggerRouters.size() - 1; i >= 0; i--) {
            TriggerRouter triggerRouter = triggerRouters.get(i);
            if (!triggerRouter.getTrigger().getChannelId().equals(Constants.CHANNEL_FILESYNC)) {
                TriggerHistory triggerHistory = triggerRouterService
                        .getNewestTriggerHistoryForTrigger(triggerRouter.getTrigger()
                                .getTriggerId());
                if (triggerHistory == null) {
                    Trigger trigger = triggerRouter.getTrigger();
                    Table table = symmetricDialect.getPlatform().getTableFromCache(
                            trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                            trigger.getSourceTableName(), false);
                    if (table == null) {
                        throw new IllegalStateException("Could not find a required table: "
                                + triggerRouter.getTrigger().getSourceTableName());
                    }
                    triggerHistory = new TriggerHistory(table, triggerRouter.getTrigger());
                    triggerHistory.setTriggerHistoryId(Integer.MAX_VALUE - i);
                }

                StringBuilder sql = new StringBuilder(symmetricDialect.createPurgeSqlFor(
                        targetNode, triggerRouter, triggerHistory));
                addPurgeCriteriaToConfigurationTables(triggerRouter.getTrigger()
                        .getSourceTableName(), sql);
                String sourceTable = triggerHistory.getSourceTableName();
                Data data = new Data(1, null, sql.toString(), DataEventType.SQL, sourceTable, null,
                        triggerHistory, triggerRouter.getTrigger().getChannelId(), null, null);
                data.putAttribute(Data.ATTRIBUTE_ROUTER_ID, triggerRouter.getRouter().getRouterId());
                initialLoadEvents.add(new SelectFromTableEvent(data));
            }
        }

        for (int i = 0; i < triggerRouters.size(); i++) {
            TriggerRouter triggerRouter = triggerRouters.get(i);
            if (!triggerRouter.getTrigger().getChannelId().equals(Constants.CHANNEL_FILESYNC)) {
                TriggerHistory triggerHistory = triggerRouterService
                        .getNewestTriggerHistoryForTrigger(triggerRouter.getTrigger()
                                .getTriggerId());
                if (triggerHistory == null) {
                    Trigger trigger = triggerRouter.getTrigger();
                    triggerHistory = new TriggerHistory(symmetricDialect.getPlatform()
                            .getTableFromCache(trigger.getSourceCatalogName(),
                                    trigger.getSourceSchemaName(), trigger.getSourceTableName(),
                                    false), trigger);
                    triggerHistory.setTriggerHistoryId(Integer.MAX_VALUE - i);
                }

                if (!triggerRouter.getTrigger().getSourceTableName()
                        .endsWith(TableConstants.SYM_NODE_IDENTITY)) {
                    initialLoadEvents.add(new SelectFromTableEvent(targetNode, triggerRouter,
                            triggerHistory));
                } else {
                    Data data = new Data(1, null, targetNode.getNodeId(), DataEventType.INSERT,
                            triggerHistory.getSourceTableName(), null, triggerHistory,
                            triggerRouter.getTrigger().getChannelId(), null, null);
                    initialLoadEvents.add(new SelectFromTableEvent(data));
                }
            }
        }

        SelectFromTableSource source = new SelectFromTableSource(batch, initialLoadEvents);
        ExtractDataReader dataReader = new ExtractDataReader(this.symmetricDialect.getPlatform(),
                source);

        ProtocolDataWriter dataWriter = new ProtocolDataWriter(nodeService.findIdentityNodeId(),
                writer, targetNode.requires13Compatiblity());
        DataProcessor processor = new DataProcessor(dataReader, dataWriter);
        DataContext ctx = new DataContext();
        ctx.put(Constants.DATA_CONTEXT_TARGET_NODE, targetNode);
        ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE, sourceNode);
        processor.process(ctx);

        if (triggerRouters.size() == 0) {
            log.error("{} attempted registration, but was sent an empty configuration", targetNode);
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
            batches.filterBatchesForChannel(Constants.CHANNEL_FILESYNC);
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
        if (!batches.containsBatchesInError() && batches.containsLoadBatches()) {
            batches.removeNonLoadBatches();
        }

        return batches.getBatches();
    }

    public List<OutgoingBatch> extract(ProcessInfo processInfo, Node targetNode, IOutgoingTransport targetTransport) {

        // make sure that data is routed before extracting if the route
        // job is not configured to start automatically
        if (!parameterService.is(ParameterConstants.START_ROUTE_JOB)) {
            routerService.routeData(true);
        }

        OutgoingBatches batches = outgoingBatchService.getOutgoingBatches(targetNode.getNodeId(), false);

        if (batches.containsBatches()) {

            List<OutgoingBatch> activeBatches = filterBatchesForExtraction(batches,
                    targetTransport.getSuspendIgnoreChannelLists(configurationService));

            if (activeBatches.size() > 0) {
                extract(processInfo, targetNode, targetTransport, activeBatches);
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

    protected void extract(ProcessInfo processInfo, Node targetNode, IOutgoingTransport targetTransport,
            List<OutgoingBatch> activeBatches) {
        long batchesSelectedAtMs = System.currentTimeMillis();
        OutgoingBatch currentBatch = null;
        try {

            IDataWriter dataWriter = null;
            long bytesSentCount = 0;
            int batchesSentCount = 0;
            long maxBytesToSync = parameterService
                    .getLong(ParameterConstants.TRANSPORT_MAX_BYTES_TO_SYNC);

            boolean streamToFileEnabled = parameterService
                    .is(ParameterConstants.STREAM_TO_FILE_ENABLED);

            for (int i = 0; i < activeBatches.size(); i++) {
                currentBatch = activeBatches.get(i);
                
                processInfo.incrementBatchCount();
                processInfo.setCurrentBatchId(currentBatch.getBatchId());

                currentBatch = requeryIfEnoughTimeHasPassed(batchesSelectedAtMs, currentBatch);
                
                if (dataWriter == null) {
                    dataWriter = new ProtocolDataWriter(nodeService.findIdentityNodeId(),
                            targetTransport.open(), targetNode.requires13Compatiblity());
                }
                
                processInfo.setStatus(ProcessInfo.Status.EXTRACTING);
                currentBatch = extractOutgoingBatch(processInfo, targetNode, dataWriter, currentBatch,
                        streamToFileEnabled);

                if (streamToFileEnabled) {
                    processInfo.setStatus(ProcessInfo.Status.TRANSFERRING);
                    currentBatch = sendOutgoingBatch(processInfo, targetNode, currentBatch, dataWriter);
                }

                if (currentBatch.getStatus() != Status.OK) {
                    currentBatch.setLoadCount(currentBatch.getLoadCount() + 1);
                    changeBatchStatus(Status.LD, currentBatch);

                    bytesSentCount += currentBatch.getByteCount();
                    batchesSentCount++;

                    if (bytesSentCount >= maxBytesToSync && i < activeBatches.size()-1) {
                        log.info(
                                "Reached the total byte threshold after {} of {} batches were extracted.  The remaining batches will be extracted on a subsequent sync", new Object[] {
                                batchesSentCount, activeBatches.size()});
                        break;
                    }
                }
            }

        } catch (RuntimeException e) {
            SQLException se = unwrapSqlException(e);
            if (currentBatch != null) {
                statisticManager.incrementDataExtractedErrors(currentBatch.getChannelId(), 1);
                if (se != null) {
                    currentBatch.setSqlState(se.getSQLState());
                    currentBatch.setSqlCode(se.getErrorCode());
                    currentBatch.setSqlMessage(se.getMessage());
                } else {
                    currentBatch.setSqlMessage(getRootMessage(e));
                }
                currentBatch.revertStatsOnError();
                if (currentBatch.getStatus() != Status.IG) {
                    currentBatch.setStatus(Status.ER);
                }
                currentBatch.setErrorFlag(true);
                outgoingBatchService.updateOutgoingBatch(currentBatch);

                if (isStreamClosedByClient(e)) {
                    log.warn(
                            "Failed to extract batch {}.  The stream was closed by the client.  There is a good chance that a previously sent batch errored out and the stream was closed.  The error was: {}",
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
            } else {
                log.error("Could not log the outgoing batch status because the batch was null", e);
            }

        }

    }

    protected boolean isStreamClosedByClient(Exception ex) {
        if (ExceptionUtils.indexOfType(ex, EOFException.class) >= 0) {
            return true;
        } else {
            return false;
        }
    }

    final protected void changeBatchStatus(Status status, OutgoingBatch currentBatch) {
        if (currentBatch.getStatus() != Status.IG) {
            currentBatch.setStatus(status);
        }
        outgoingBatchService.updateOutgoingBatch(currentBatch);
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
            IDataWriter dataWriter, OutgoingBatch currentBatch,
            boolean streamToFileEnabled) {
        if (currentBatch.getStatus() != Status.OK) {
            Node sourceNode = nodeService.findIdentity();

            TransformWriter transformExtractWriter = null;
            if (streamToFileEnabled) {
                transformExtractWriter = createTransformDataWriter(sourceNode, targetNode,
                        new ProcessInfoDataWriter(new StagingDataWriter(nodeService.findIdentityNodeId(),
                                Constants.STAGING_CATEGORY_OUTGOING, stagingManager), processInfo));
            } else {
                transformExtractWriter = createTransformDataWriter(
                        sourceNode,
                        targetNode, new ProcessInfoDataWriter(dataWriter, processInfo));
            }

            long ts = System.currentTimeMillis();
            long extractTimeInMs = 0l;
            long byteCount = 0l;

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
            } else {
                if (currentBatch.getStatus() == Status.NE || 
                        !isPreviouslyExtracted(currentBatch)) {
                    int maxPermits = parameterService.getInt(ParameterConstants.CONCURRENT_WORKERS);
                    Semaphore lock = null;
                    try {
                        synchronized (locks) {
                            lock = locks.get(currentBatch.getBatchId());
                            if (lock == null) {
                                lock = new Semaphore(maxPermits);
                                locks.put(currentBatch.getBatchId(), lock);
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
                                changeBatchStatus(Status.QY, currentBatch);
                                IDataReader dataReader = new ExtractDataReader(
                                        symmetricDialect.getPlatform(),
                                        new SelectFromSymDataSource(currentBatch, sourceNode,
                                                targetNode));
                                DataContext ctx = new DataContext();
                                ctx.put(Constants.DATA_CONTEXT_TARGET_NODE, targetNode);
                                ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE, sourceNode);
                                new DataProcessor(dataReader, transformExtractWriter).process(ctx);
                                extractTimeInMs = System.currentTimeMillis() - ts;
                                Statistics stats = transformExtractWriter.getNestedWriter()
                                        .getStatistics().values().iterator().next();
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
                        synchronized (locks) {
                            lock.release();
                            if (lock.availablePermits() == maxPermits) {
                                locks.remove(currentBatch.getBatchId());
                            }
                        }
                    }
                }
            }

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

            // only update the current batch after we have possibly "re-queried"
            if (extractTimeInMs > 0) {
                currentBatch.setExtractMillis(extractTimeInMs);
            }

            if (byteCount > 0) {
                currentBatch.setByteCount(byteCount);
                statisticManager
                        .incrementDataBytesExtracted(currentBatch.getChannelId(), byteCount);
                statisticManager.incrementDataExtracted(currentBatch.getChannelId(),
                        currentBatch.getExtractCount());
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

    protected OutgoingBatch sendOutgoingBatch(ProcessInfo processInfo, Node targetNode, OutgoingBatch currentBatch,
            IDataWriter dataWriter) {
        if (currentBatch.getStatus() != Status.OK) {
            currentBatch.setSentCount(currentBatch.getSentCount() + 1);
            changeBatchStatus(Status.SE, currentBatch);

            long ts = System.currentTimeMillis();

            IStagedResource extractedBatch = getStagedResource(currentBatch);
            if (extractedBatch != null) {
                IDataReader dataReader = new ProtocolDataReader(BatchType.EXTRACT,
                        currentBatch.getNodeId(), extractedBatch);

                DataContext ctx = new DataContext();
                ctx.put(Constants.DATA_CONTEXT_TARGET_NODE, targetNode);
                ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE, nodeService.findIdentity());
                new DataProcessor(dataReader, new ProcessInfoDataWriter(dataWriter, processInfo)).process(ctx);
                if (dataWriter.getStatistics().size() > 0) {
                    Statistics stats = dataWriter.getStatistics().values().iterator().next();
                    statisticManager.incrementDataSent(currentBatch.getChannelId(),
                            stats.get(DataWriterStatisticConstants.STATEMENTCOUNT));
                    long byteCount = stats.get(DataWriterStatisticConstants.BYTECOUNT);
                    statisticManager.incrementDataBytesSent(currentBatch.getChannelId(),
                            byteCount);
                    if (byteCount == 0) {
                        log.warn("The data writer reported sending 0 bytes for batch {}", currentBatch.getNodeBatchId());
                    }
                } else {
                    log.warn("Could not find recorded statistics for batch {}", currentBatch.getNodeBatchId());
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
                            new SelectFromSymDataSource(batch, sourceNode, targetNode));
                    DataContext ctx = new DataContext();
                    ctx.put(Constants.DATA_CONTEXT_TARGET_NODE, targetNode);
                    ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE, nodeService.findIdentity());
                    new DataProcessor(dataReader, createTransformDataWriter(
                            nodeService.findIdentity(), targetNode,
                            new ProtocolDataWriter(nodeService.findIdentityNodeId(), writer,
                                    targetNode.requires13Compatiblity()))).process(ctx);
                    foundBatch = true;
                }
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
                    TransformPoint.EXTRACT, true);
        }
        TransformTable[] transforms = transformsList != null ? transformsList
                .toArray(new TransformTable[transformsList.size()]) : null;
        TransformWriter transformExtractWriter = new TransformWriter(
                symmetricDialect.getPlatform(), TransformPoint.EXTRACT, extractWriter, transforms);
        return transformExtractWriter;
    }

    protected Table lookupAndOrderColumnsAccordingToTriggerHistory(String routerId,
            TriggerHistory triggerHistory, boolean setTargetTableName) {
        String catalogName = triggerHistory.getSourceCatalogName();
        String schemaName = triggerHistory.getSourceSchemaName();
        String tableName = triggerHistory.getSourceTableName();
        Table table = symmetricDialect.getPlatform().getTableFromCache(catalogName, schemaName,
                tableName, false);
        if (table == null) {
            table = new Table(tableName);
            table.addColumns(triggerHistory.getParsedColumnNames());
        }
        table = table.copyAndFilterColumns(triggerHistory.getParsedColumnNames(),
                triggerHistory.getParsedPkColumnNames(), true);
        table.setCatalog(catalogName);
        table.setSchema(schemaName);

        Router router = triggerRouterService.getRouterById(routerId, false);
        if (router != null && setTargetTableName) {
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

    class SelectFromSymDataSource implements IExtractDataReaderSource {

        private Batch batch;

        private OutgoingBatch outgoingBatch;

        private Table targetTable;
        
        private Table sourceTable;

        private TriggerHistory lastTriggerHistory;

        private boolean requiresLobSelectedFromSource;

        private ISqlReadCursor<Data> cursor;

        private SelectFromTableSource reloadSource;

        private Node targetNode;

        public SelectFromSymDataSource(OutgoingBatch outgoingBatch, Node sourceNode, Node targetNode) {
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

                        TriggerRouter triggerRouter = triggerRouterService.getTriggerRouterForCurrentNode(triggerId, routerId, false);
                        if (triggerRouter != null) {
                            SelectFromTableEvent event = new SelectFromTableEvent(targetNode,
                                    triggerRouter, triggerHistory);
                            this.reloadSource = new SelectFromTableSource(outgoingBatch, batch,
                                    event);
                            data = (Data) this.reloadSource.next();
                            this.sourceTable = reloadSource.getSourceTable();
                            this.targetTable = this.reloadSource.getTargetTable();
                            this.requiresLobSelectedFromSource = this.reloadSource
                                    .requiresLobsSelectedFromSource();
                        } else {
                            log.warn("Could not find trigger router definition for {}:{}.  Skipping reload event with the data id of {}", new Object[] {triggerId, routerId, data.getDataId()});
                            return next();
                        }
                    } else {
                        Trigger trigger = triggerRouterService.getTriggerById(
                                triggerHistory.getTriggerId(), false);
                        if (trigger != null) {
                            if ((lastTriggerHistory == null || lastTriggerHistory
                                    .getTriggerHistoryId() != triggerHistory.getTriggerHistoryId())) {
                                this.sourceTable = lookupAndOrderColumnsAccordingToTriggerHistory(
                                        routerId, triggerHistory, false);                                
                                this.targetTable = lookupAndOrderColumnsAccordingToTriggerHistory(
                                        routerId, triggerHistory, true);
                                this.requiresLobSelectedFromSource = trigger.isUseStreamLobs();
                            }
                            
                            data.setNoBinaryOldData(requiresLobSelectedFromSource || 
                                    symmetricDialect.getName().equals(DatabaseNamesConstants.MSSQL));
                        } else {
                            log.error(
                                    "Could not locate a trigger with the id of {} for {}.  It was recorded in the hist table with a hist id of {}",
                                    new Object[] { triggerHistory.getTriggerId(),
                                            triggerHistory.getSourceTableName(),
                                            triggerHistory.getTriggerHistoryId() });
                        }
                    }

                    lastTriggerHistory = triggerHistory;
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
                    && !routerService.shouldDataBeRouted(routingContext, new DataMetaData(
                            (Data) data, targetTable, triggerRouter, routingContext.getChannel()),
                            node, true));

            if (data != null && outgoingBatch != null) {
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
                            (String) data.getAttribute(CsvData.ATTRIBUTE_ROUTER_ID), history, false);
                    this.targetTable = lookupAndOrderColumnsAccordingToTriggerHistory(
                            (String) data.getAttribute(CsvData.ATTRIBUTE_ROUTER_ID), history, true);
                } else {
                    this.triggerRouter = this.currentInitialLoadEvent.getTriggerRouter();                    
                    if (this.routingContext == null) {
                        NodeChannel channel = batch != null ? configurationService.getNodeChannel(
                                batch.getChannelId(), false) : new NodeChannel(this.triggerRouter
                                .getTrigger().getChannelId());
                        this.routingContext = new SimpleRouterContext(batch.getTargetNodeId(),
                                channel);
                    }
                    this.sourceTable = lookupAndOrderColumnsAccordingToTriggerHistory(
                            triggerRouter.getRouter().getRouterId(), history, false);
                    this.targetTable = lookupAndOrderColumnsAccordingToTriggerHistory(
                            triggerRouter.getRouter().getRouterId(), history, true);                    
                    this.startNewCursor(history, triggerRouter);

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
                final TriggerRouter triggerRouter) {
            final int expectedCommaCount = triggerHistory.getParsedColumnNames().length - 1;
            String initialLoadSql = symmetricDialect.createInitialLoadSqlFor(
                    this.currentInitialLoadEvent.getNode(), triggerRouter, this.sourceTable,
                    triggerHistory,
                    configurationService.getChannel(triggerRouter.getTrigger().getChannelId()));
            this.cursor = sqlTemplate.queryForCursor(initialLoadSql, new ISqlRowMapper<Data>() {
                public Data mapRow(Row rs) {
                    String csvRow = rs.stringValue();
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
                                "The extracted row data did not have the expected (%d) number of columns: %s",
                                expectedCommaCount, csvRow);
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

        public SelectFromTableEvent(Node node, TriggerRouter triggerRouter,
                TriggerHistory triggerHistory) {
            this.node = node;
            this.triggerRouter = triggerRouter;
            Trigger trigger = triggerRouter.getTrigger();
            this.triggerHistory = triggerHistory != null ? triggerHistory : triggerRouterService
                    .getNewestTriggerHistoryForTrigger(trigger.getTriggerId());
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
