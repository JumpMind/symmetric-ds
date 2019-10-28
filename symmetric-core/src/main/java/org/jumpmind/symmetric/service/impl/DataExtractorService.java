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

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.jumpmind.symmetric.common.Constants.LOG_PROCESS_SUMMARY_THRESHOLD;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.jumpmind.db.io.DatabaseXmlUtil;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.PlatformColumn;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.DdlBuilderFactory;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.AbstractSymmetricEngine;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.Batch.BatchType;
import org.jumpmind.symmetric.io.data.CsvConstants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.DataProcessor;
import org.jumpmind.symmetric.io.data.IDataProcessorListener;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.ProtocolException;
import org.jumpmind.symmetric.io.data.reader.DataReaderStatistics;
import org.jumpmind.symmetric.io.data.reader.ExtractDataReader;
import org.jumpmind.symmetric.io.data.reader.IExtractDataReaderSource;
import org.jumpmind.symmetric.io.data.reader.ProtocolDataReader;
import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.io.data.transform.TransformTable;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.data.writer.ProtocolDataWriter;
import org.jumpmind.symmetric.io.data.writer.StagingDataWriter;
import org.jumpmind.symmetric.io.data.writer.StructureDataWriter;
import org.jumpmind.symmetric.io.data.writer.StructureDataWriter.PayloadType;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.io.stage.StagingFileLock;
import org.jumpmind.symmetric.load.IReloadVariableFilter;
import org.jumpmind.symmetric.model.AbstractBatch.Status;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.ExtractRequest;
import org.jumpmind.symmetric.model.ExtractRequest.ExtractStatus;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeCommunication;
import org.jumpmind.symmetric.model.NodeCommunication.CommunicationType;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchWithPayload;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfo.ProcessStatus;
import org.jumpmind.symmetric.model.ProcessInfoDataWriter;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessType;
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
import org.jumpmind.symmetric.service.IExtensionService;
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
import org.jumpmind.symmetric.transport.BatchBufferedWriter;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.TransportUtils;
import org.jumpmind.symmetric.util.SymmetricUtils;
import org.jumpmind.util.AppUtils;
import org.jumpmind.util.CustomizableThreadFactory;
import org.jumpmind.util.ExceptionUtils;
import org.jumpmind.util.FormatUtils;
import org.jumpmind.util.FutureImpl;
import org.jumpmind.util.Statistics;

/**
 * @see IDataExtractorService
 */
public class DataExtractorService extends AbstractService implements IDataExtractorService,
        INodeCommunicationExecutor {

    final static long MS_PASSED_BEFORE_BATCH_REQUERIED = 5000;
        
    protected enum ExtractMode { FOR_SYM_CLIENT, FOR_PAYLOAD_CLIENT, EXTRACT_ONLY };

    IOutgoingBatchService outgoingBatchService;

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
    
    private IExtensionService extensionService;

    private Map<String, BatchLock> locks = new ConcurrentHashMap<String, BatchLock>();
    
    private CustomizableThreadFactory threadPoolFactory;

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
        this.extensionService = engine.getExtensionService();
        setSqlMap(new DataExtractorServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    /**
     * @see DataExtractorService#extractConfigurationStandalone(Node, Writer)
     */
    public void extractConfigurationStandalone(Node node, OutputStream out) {
        this.extractConfigurationStandalone(node, TransportUtils.toWriter(out),
                TableConstants.SYM_MONITOR_EVENT, TableConstants.SYM_CONSOLE_EVENT);
    }
    
    public void extractConfigurationOnly(Node node, OutputStream out) {
        this.extractConfigurationStandalone(node, TransportUtils.toWriter(out),                
                TableConstants.SYM_NODE, TableConstants.SYM_NODE_SECURITY,
                TableConstants.SYM_NODE_IDENTITY, TableConstants.SYM_NODE_HOST, TableConstants.SYM_FILE_SNAPSHOT,
                TableConstants.SYM_NODE_CHANNEL_CTL, TableConstants.SYM_CONSOLE_USER,
                TableConstants.SYM_TABLE_RELOAD_REQUEST, TableConstants.SYM_MONITOR_EVENT, TableConstants.SYM_CONSOLE_EVENT);
    }
    
    protected boolean filter(Node targetNode, String tableName) {        
        boolean pre37 = Version.isOlderThanVersion(targetNode.getSymmetricVersionParts(), Version.VERSION_3_7_0);
        boolean pre38 = Version.isOlderThanVersion(targetNode.getSymmetricVersionParts(), Version.VERSION_3_8_0);
        boolean pre3818 = Version.isOlderThanVersion(targetNode.getSymmetricVersionParts(), Version.VERSION_3_8_18);
        boolean pre39 =  Version.isOlderThanVersion(targetNode.getSymmetricVersionParts(), Version.VERSION_3_9_0);

        tableName = tableName.toLowerCase();
        boolean include = true;
        if (pre39 && tableName.contains(TableConstants.SYM_JOB)) {
            include = false;
        } else if (pre37 && tableName.contains(TableConstants.SYM_EXTENSION)) {
            include = false;
        } else if (pre38 && (tableName.contains(TableConstants.SYM_MONITOR) || 
                tableName.contains(TableConstants.SYM_NOTIFICATION))) {
            include = false;
        } else if (pre3818 && tableName.contains(TableConstants.SYM_CONSOLE_USER_HIST)) {
            include = false;
        } else if (tableName.contains(TableConstants.SYM_CONSOLE_USER) 
                || tableName.contains(TableConstants.SYM_CONSOLE_USER_HIST)) {
            boolean isTargetProfessional = StringUtils.equals(targetNode.getDeploymentType(), 
                    Constants.DEPLOYMENT_TYPE_PROFESSIONAL);
            if (!isTargetProfessional) {
                include = false;
            }
        }
        
        return include;
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

            for (int i = triggerRouters.size() - 1; i >= 0; i--) {
                TriggerRouter triggerRouter = triggerRouters.get(i);
                String channelId = triggerRouter.getTrigger().getChannelId();
                if (Constants.CHANNEL_CONFIG.equals(channelId)
                        || Constants.CHANNEL_HEARTBEAT.equals(channelId)) {
                    if (filter(targetNode, triggerRouter.getTrigger().getSourceTableName())) {

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
                    if (filter(targetNode, triggerRouter.getTrigger().getSourceTableName())) {
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
            if (batches.containsLoadBatches() && !(parameterService.is(ParameterConstants.INITIAL_LOAD_UNBLOCK_CHANNELS_ON_ERROR, true) 
            		 && batches.containsBatchesInError())) {
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
                        activeBatches, writer, null, ExtractMode.FOR_PAYLOAD_CLIENT);

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

    public List<OutgoingBatch> extract(ProcessInfo extractInfo, Node targetNode,
            IOutgoingTransport transport) {
        return extract(extractInfo, targetNode, null, transport);
    }
    
    public List<OutgoingBatch> extract(ProcessInfo extractInfo, Node targetNode, String queue, 
            IOutgoingTransport transport) {

        /*
         * make sure that data is routed before extracting if the route job is
         * not configured to start automatically
         */
        String startRouteJob = parameterService.getString(ParameterConstants.START_ROUTE_JOB_38);
        boolean startRoutingJob = false;
        if (StringUtils.isBlank(startRouteJob)) {
            startRoutingJob = parameterService.is(ParameterConstants.START_ROUTE_JOB);
        } else {
            startRoutingJob = parameterService.is(ParameterConstants.START_ROUTE_JOB_38);
        }
        
        if (!startRoutingJob && parameterService.is(ParameterConstants.ROUTE_ON_EXTRACT)) {
            routerService.routeData(true);
        }
        
        OutgoingBatches batches = loadPendingBatches(extractInfo, targetNode, queue, transport);

        if (batches != null && batches.containsBatches()) {

            ChannelMap channelMap = transport.getSuspendIgnoreChannelLists(configurationService, queue,
                    targetNode);

            List<OutgoingBatch> activeBatches = filterBatchesForExtraction(batches, channelMap);

            if (activeBatches.size() > 0) {
                BufferedWriter writer = transport.openWriter();
                IDataWriter dataWriter = new ProtocolDataWriter(nodeService.findIdentityNodeId(),
                        writer, targetNode.requires13Compatiblity());

                return extract(extractInfo, targetNode, activeBatches, dataWriter, writer, ExtractMode.FOR_SYM_CLIENT);
            }

        }

        return Collections.emptyList();

    }

    protected OutgoingBatches loadPendingBatches(ProcessInfo extractInfo, Node targetNode, String queue, IOutgoingTransport transport) {
        
        BufferedWriter writer = transport.getWriter();         
        
        Callable<OutgoingBatches> getOutgoingBatches = () -> {                            
            OutgoingBatches batches = null;
            if (queue != null) {
                NodeGroupLink link = configurationService.getNodeGroupLinkFor(nodeService.findIdentity().getNodeGroupId(),
                        targetNode.getNodeGroupId(), false);
                if (link != null) {
                    NodeGroupLinkAction defaultAction = configurationService.getNodeGroupLinkFor(nodeService.findIdentity().getNodeGroupId(),
                            targetNode.getNodeGroupId(), false).getDataEventAction();
                    ProcessType processType = extractInfo.getKey().getProcessType();
                    NodeGroupLinkAction action = null;
                    
                    if (processType.equals(ProcessType.PUSH_JOB_EXTRACT)) {
                        action = NodeGroupLinkAction.P;
                    } else if (processType.equals(ProcessType.PULL_HANDLER_EXTRACT)) {
                        action = NodeGroupLinkAction.W;
                    }
                    batches = outgoingBatchService.getOutgoingBatches(targetNode.getNodeId(), queue, action, defaultAction, false);
                } else {
                    log.error("Group link not found for " + nodeService.findIdentity().getNodeGroupId() +
                            " to " + targetNode.getNodeGroupId() + ".  Check that configuration matches on both nodes.");
                }
            } else {
                batches = outgoingBatchService.getOutgoingBatches(targetNode.getNodeId(), false);
            }
            return batches;
        };
        
        if (writer != null) {
            final boolean streamToFileEnabled = parameterService.is(ParameterConstants.STREAM_TO_FILE_ENABLED);
            long keepAliveMillis = parameterService.getLong(ParameterConstants.DATA_LOADER_SEND_ACK_KEEPALIVE);
            Node sourceNode = nodeService.findIdentity();
            FutureTask<OutgoingBatches> getOutgoingBatchesTask = new FutureTask<OutgoingBatches>(getOutgoingBatches);            
            ExecutorService executor = Executors.newFixedThreadPool(1);
            executor.execute(getOutgoingBatchesTask);
            
            try {                
                while (true) {                
                    try {
                        return getOutgoingBatchesTask.get(keepAliveMillis, TimeUnit.MILLISECONDS);
                    } catch (TimeoutException ex) {
                        writeKeepAliveAck(writer, sourceNode, streamToFileEnabled);
                    } catch (Exception ex) {
                        throw new SymmetricException("Failed to execute getOutgoingBatchesTask ", ex);
                    }
                }
            } finally {
                executor.shutdown();
            }
        } else {
            try {                
                return getOutgoingBatches.call();
            } catch (Exception ex) {
                throw new SymmetricException("Failed to execute getOutgoingBatchesTask ", ex);
            }
        }
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
                batches = extract(new ProcessInfo(), targetNode, batches, dataWriter, null,
                        ExtractMode.EXTRACT_ONLY);
                extracted = batches.size() > 0;
            }
        }
        return extracted;
    }

    protected List<OutgoingBatch> extract(final ProcessInfo extractInfo, final Node targetNode,
            final List<OutgoingBatch> activeBatches, final IDataWriter dataWriter, final BufferedWriter writer, final ExtractMode mode) {
        if (activeBatches.size() > 0) {
            final List<OutgoingBatch> processedBatches = new ArrayList<OutgoingBatch>(activeBatches.size());
            Set<String> channelsProcessed = new HashSet<String>();
            long batchesSelectedAtMs = System.currentTimeMillis();
            OutgoingBatch currentBatch = null;
            ExecutorService executor = null;
            try {
                final boolean streamToFileEnabled = parameterService.is(ParameterConstants.STREAM_TO_FILE_ENABLED);
                long keepAliveMillis = parameterService.getLong(ParameterConstants.DATA_LOADER_SEND_ACK_KEEPALIVE);
                Node sourceNode = nodeService.findIdentity();
                final FutureExtractStatus status = new FutureExtractStatus();

                if (this.threadPoolFactory == null) {
                    this.threadPoolFactory = new CustomizableThreadFactory(String.format("%s-dataextractor", parameterService.getEngineName().toLowerCase()));
                }
                
                executor = streamToFileEnabled ? Executors.newFixedThreadPool(1, this.threadPoolFactory) : null;

                List<Future<FutureOutgoingBatch>> futures = new ArrayList<Future<FutureOutgoingBatch>>();

                extractInfo.setTotalBatchCount(activeBatches.size());                
                for (int i = 0; i < activeBatches.size(); i++) {
                    currentBatch = activeBatches.get(i);
                                        
                    channelsProcessed.add(currentBatch.getChannelId());
                    
                    final OutgoingBatch extractBatch = currentBatch;
                    Callable<FutureOutgoingBatch> callable = () -> {
                        OutgoingBatch refreshedBatch = requeryIfEnoughTimeHasPassed(batchesSelectedAtMs, extractBatch);
                        return extractBatch(refreshedBatch, status, extractInfo, targetNode, dataWriter, mode, activeBatches);                            
                    };
                    
                    if (status.shouldExtractSkip) {
                        break;
                    }
                    
                    if (executor != null) {
                        futures.add(executor.submit(callable));
                    } else {
                        try {
                            FutureOutgoingBatch batch = callable.call();
                            futures.add(new FutureImpl<>(batch));
                        } catch (RuntimeException e) {
                            throw e;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                if (parameterService.is(ParameterConstants.SYNCHRONIZE_ALL_JOBS) && executor != null) {
                    executor.shutdown();
                    boolean isProcessed = false;
                    while (!isProcessed) {
                        try {
                            isProcessed = executor.awaitTermination(keepAliveMillis, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        if (!isProcessed) {
                            writeKeepAliveAck(writer, sourceNode, streamToFileEnabled);
                        }
                    }
                }

                final long initialLoadMaxBytesToSync = parameterService.getLong(ParameterConstants.INITIAL_LOAD_TRANSPORT_MAX_BYTES_TO_SYNC);
                long totalBytesSend = 0;
                boolean logMaxBytesReached = false;
                Iterator<OutgoingBatch> activeBatchIter = activeBatches.iterator();                
                for (int i = 0; i < futures.size(); i++) {
                    Future<FutureOutgoingBatch> future = futures.get(i);
                    currentBatch = activeBatchIter.next();
                    boolean isProcessed = false;
                    ProcessInfo transferInfo = null;
                    while (!isProcessed) {
                        try {
                            FutureOutgoingBatch extractBatch = future.get(keepAliveMillis, TimeUnit.MILLISECONDS);
                            
                            transferInfo = statisticManager.newProcessInfo(new ProcessInfoKey(nodeService.findIdentityNodeId(),
                                    extractInfo.getQueue(), targetNode.getNodeId(), extractInfo.getProcessType() == ProcessType.PUSH_JOB_EXTRACT ? ProcessType.PUSH_JOB_TRANSFER : ProcessType.PULL_HANDLER_TRANSFER));
                            transferInfo.setTotalDataCount(currentBatch.getExtractRowCount());

                            currentBatch = extractBatch.getOutgoingBatch();
                            
                            if (i == futures.size() - 1) {
                                extractInfo.setStatus(ProcessStatus.OK);
                            }
                            
                            if (extractBatch.isExtractSkipped) {
                                transferInfo.setStatus(ProcessStatus.OK);
                                break;
                            }

                            if (streamToFileEnabled || mode == ExtractMode.FOR_PAYLOAD_CLIENT || (currentBatch.isExtractJobFlag() && parameterService.is(ParameterConstants.INITIAL_LOAD_USE_EXTRACT_JOB))) {
                                
                                if(totalBytesSend > initialLoadMaxBytesToSync) {
                                    if(!logMaxBytesReached) {
                                        logMaxBytesReached = true;
                                        log.info(
                                                "Reached the total byte threshold for initial load after {} of {} batches were sent for node '{}' (sent {} bytes, the max is {}).  "
                                                        + "The remaining batches will be send on a subsequent sync.",
                                                new Object[] { i, futures.size(), targetNode.getNodeId(), totalBytesSend, initialLoadMaxBytesToSync });
                                    }
                                    transferInfo.setStatus(ProcessStatus.OK);
                                    break;
                                }
                                
                                transferInfo.setStatus(ProcessInfo.ProcessStatus.TRANSFERRING);
                                transferInfo.setCurrentLoadId(currentBatch.getLoadId());
                                boolean isRetry = extractBatch.isRetry() && extractBatch.getOutgoingBatch().getStatus() != OutgoingBatch.Status.IG;
                                
                                currentBatch = sendOutgoingBatch(transferInfo, targetNode, currentBatch, isRetry, 
                                        dataWriter, writer, mode);                                
                                
                                totalBytesSend += currentBatch.getByteCount();
                            }
                            
                            processedBatches.add(currentBatch);

                            if (currentBatch.getStatus() != Status.OK) {
                                currentBatch.setLoadCount(currentBatch.getLoadCount() + 1);
                                changeBatchStatus(Status.LD, currentBatch, mode);                                
                            }
                            
                            transferInfo.setCurrentTableName(currentBatch.getSummary());                            
                            transferInfo.setStatus(ProcessStatus.OK);
                            
                            isProcessed = true;
                        } catch (TimeoutException e) {
                            writeKeepAliveAck(writer, sourceNode, streamToFileEnabled);                            
                        } catch (Exception e) {
                            if (transferInfo != null && transferInfo.getStatus() != ProcessStatus.OK) {
                                transferInfo.setStatus(ProcessStatus.ERROR);
                            }
                            if (e instanceof ExecutionException) {
                                if (isNotBlank(e.getMessage()) && e.getMessage().contains("string truncation")) {
                                    throw new RuntimeException(
                                            "There is a good chance that the truncation error you are receiving is because contains_big_lobs on the '"
                                                    + currentBatch.getChannelId() + "' channel needs to be turned on.",
                                            e.getCause() != null ? e.getCause() : e);
                                }
                                throw new RuntimeException(e.getCause() != null ? e.getCause() : e);
                            } else if (!(e instanceof RuntimeException)) {
                                throw new RuntimeException(e);
                            } else if (e instanceof RuntimeException) {
                                throw (RuntimeException) e;
                            }
                        } 
                    }
                }
            } catch (RuntimeException e) {
                SQLException se = ExceptionUtils.unwrapSqlException(e);              
                if (currentBatch != null) {
                    try {
                        /* Reread batch in case the ignore flag has been set */
                        currentBatch = outgoingBatchService.
                                findOutgoingBatch(currentBatch.getBatchId(), currentBatch.getNodeId());
                        statisticManager.incrementDataExtractedErrors(currentBatch.getChannelId(), 1);
                        if (se != null) {
                            currentBatch.setSqlState(se.getSQLState());
                            currentBatch.setSqlCode(se.getErrorCode());
                            currentBatch.setSqlMessage(se.getMessage());
                        } else {
                            currentBatch.setSqlMessage(ExceptionUtils.getRootMessage(e));
                        }
                        currentBatch.revertStatsOnError();
                        if (currentBatch.getStatus() != Status.IG &&
                                currentBatch.getStatus() != Status.OK) {
                            currentBatch.setStatus(Status.ER);
                            currentBatch.setErrorFlag(true);
                        }
                        outgoingBatchService.updateOutgoingBatch(currentBatch);
                    } catch(Exception ex) {
                        log.error("Failed to update the outgoing batch status for failed batch {}", currentBatch, ex);
                    } finally {
                        if (!isStreamClosedByClient(e)) {
                            if (e instanceof ProtocolException) {
                                IStagedResource resource = getStagedResource(currentBatch);
                                if (resource != null) {
                                    resource.delete();
                                }
                            }
                            if (e.getCause() instanceof InterruptedException) {
                                log.info("Extract of batch {} was interrupted", currentBatch);
                            } else {
                                log.error("Failed to extract batch {}", currentBatch, e);
                            }
                        }
                        extractInfo.setStatus(ProcessInfo.ProcessStatus.ERROR);                        
                    }
                } else {
                    log.error("Could not log the outgoing batch status because the batch was null",
                            e);
                }
            } finally {
                if (executor != null) {                    
                    executor.shutdown(); 
                }
            } 

            // Next, we update the node channel controls to the
            // current timestamp
            Calendar now = Calendar.getInstance();

            for (String channelProcessed : channelsProcessed) {
                NodeChannel nodeChannel = configurationService.getNodeChannel(channelProcessed,
                        targetNode.getNodeId(), false);
                if (nodeChannel != null && nodeChannel.getExtractPeriodMillis() > 0) {
                    nodeChannel.setLastExtractTime(now.getTime());                    
                    configurationService.updateLastExtractTime(nodeChannel);
                }
            }

            return processedBatches;
        } else {
            return Collections.emptyList();
        }
    }
    
    protected FutureOutgoingBatch extractBatch(OutgoingBatch extractBatch, FutureExtractStatus status, ProcessInfo extractInfo,
            Node targetNode, IDataWriter dataWriter, ExtractMode mode, List<OutgoingBatch> activeBatches) throws Exception {
        extractInfo.setThread(Thread.currentThread());
        extractInfo.setCurrentLoadId(extractBatch.getLoadId());
        extractInfo.setTotalDataCount(extractBatch.getDataRowCount());
        FutureOutgoingBatch outgoingBatch = new FutureOutgoingBatch(extractBatch, false);
        final long maxBytesToSync = parameterService.getLong(ParameterConstants.TRANSPORT_MAX_BYTES_TO_SYNC);
        final boolean streamToFileEnabled = parameterService.is(ParameterConstants.STREAM_TO_FILE_ENABLED);
        if (!status.shouldExtractSkip) {
            if (extractBatch.isExtractJobFlag() && extractBatch.getStatus() != Status.IG) {
                if (parameterService.is(ParameterConstants.INITIAL_LOAD_USE_EXTRACT_JOB)) {
                    if (extractBatch.getStatus() != Status.RQ && extractBatch.getStatus() != Status.IG
                            && !isPreviouslyExtracted(extractBatch, false)) {
                        /*
                         * the batch must have been purged. it needs to be
                         * re-extracted
                         */
                        log.info("Batch {} is marked as ready but it has been deleted.  Rescheduling it for extraction",
                                extractBatch.getNodeBatchId());
                        if (mode != ExtractMode.EXTRACT_ONLY) {
                            resetExtractRequest(extractBatch);
                        }
                        status.shouldExtractSkip = outgoingBatch.isExtractSkipped = true;
                    } else if (extractBatch.getStatus() == Status.RQ) {
                        log.info("Batch {} is not ready for delivery.  It is currently scheduled for extraction",
                                extractBatch.getNodeBatchId());
                        status.shouldExtractSkip = outgoingBatch.isExtractSkipped = true;
                    }
                } else {
                    extractBatch.setStatus(Status.NE);
                    extractBatch.setExtractJobFlag(false);
                }
            } else {
                try {
                    boolean isRetry = isRetry(extractBatch, targetNode);
                    outgoingBatch = new FutureOutgoingBatch(
                            extractOutgoingBatch(extractInfo, targetNode, dataWriter, extractBatch, streamToFileEnabled, true, mode, null),
                            isRetry);
                    status.batchExtractCount++;
                    status.byteExtractCount += extractBatch.getByteCount();

                    if (status.byteExtractCount >= maxBytesToSync && status.batchExtractCount < activeBatches.size()) {
                        log.info(
                                "Reached the total byte threshold after {} of {} batches were extracted for node '{}' (extracted {} bytes, the max is {}).  "
                                        + "The remaining batches will be extracted on a subsequent sync.",
                                new Object[] { status.batchExtractCount, activeBatches.size(), targetNode.getNodeId(), status.byteExtractCount, maxBytesToSync });
                        status.shouldExtractSkip = true;
                    }
                } catch (Exception e) {
                    status.shouldExtractSkip = outgoingBatch.isExtractSkipped = true;
                    throw e;
                }
            }
        } else {
            outgoingBatch.isExtractSkipped = true;
        }
        return outgoingBatch;
    }

    protected void writeKeepAliveAck(BufferedWriter writer, Node sourceNode, boolean streamToFileEnabled) {
        try {
            if (writer != null && streamToFileEnabled) {
                writer.write(CsvConstants.NODEID + "," + sourceNode.getNodeId());
                writer.newLine();
                writer.flush();
            }
        } catch (IOException ex) {
        }                                
    }

    final protected boolean changeBatchStatus(Status status, OutgoingBatch currentBatch, ExtractMode mode) {
        if (currentBatch.getStatus() != Status.IG) {
            currentBatch.setStatus(status);
        }
        if (mode != ExtractMode.EXTRACT_ONLY) {
            long batchStatusUpdateMillis = parameterService.getLong(ParameterConstants.OUTGOING_BATCH_UPDATE_STATUS_MILLIS);
            int batchStatusUpdateDataCount = parameterService.getInt(ParameterConstants.OUTGOING_BATCH_UPDATE_STATUS_DATA_COUNT);
            Channel channel = configurationService.getChannel(currentBatch.getChannelId());
            
            if (currentBatch.getStatus() == Status.RQ || 
                    currentBatch.getStatus() == Status.LD ||
                    currentBatch.getLastUpdatedTime() == null ||
                    System.currentTimeMillis() - batchStatusUpdateMillis >= currentBatch.getLastUpdatedTime().getTime() ||
                    channel.isReloadFlag() || 
                    currentBatch.getDataRowCount() > batchStatusUpdateDataCount) {
                outgoingBatchService.updateOutgoingBatch(currentBatch);
                return true;
            }
            return false;
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

    protected OutgoingBatch extractOutgoingBatch(ProcessInfo extractInfo, Node targetNode,
            IDataWriter dataWriter, OutgoingBatch currentBatch, boolean useStagingDataWriter, 
            boolean updateBatchStatistics, ExtractMode mode, IDataProcessorListener listener) {
        
        if (currentBatch.getStatus() != Status.OK || ExtractMode.EXTRACT_ONLY == mode || ExtractMode.FOR_SYM_CLIENT == mode) {
            
            Node sourceNode = nodeService.findIdentity();

            IDataWriter writer = wrapWithTransformWriter(sourceNode, targetNode, extractInfo, dataWriter, useStagingDataWriter);

            long ts = System.currentTimeMillis();
            long extractTimeInMs = 0l;
            long byteCount = 0l;
            long transformTimeInMs = 0l;            

            if (currentBatch.getStatus() == Status.IG) {
                cleanupIgnoredBatch(sourceNode, targetNode, currentBatch, writer);
            } else if (!isPreviouslyExtracted(currentBatch, true)) {
                BatchLock lock = null;
                try {
                    log.debug("{} attempting to acquire lock for batch {}", targetNode.getNodeId(), currentBatch.getBatchId());
                    lock = acquireLock(currentBatch, useStagingDataWriter);
                    log.debug("{} acquired lock for batch {}", targetNode.getNodeId(), currentBatch.getBatchId());
                    if (!isPreviouslyExtracted(currentBatch, true)) {
                        log.debug("{} extracting batch {}", targetNode.getNodeId(), currentBatch.getBatchId());
                        currentBatch.setExtractCount(currentBatch.getExtractCount() + 1);
                        
                        if (currentBatch.getExtractStartTime() == null) {
                            currentBatch.setExtractStartTime(new Date());
                        }
                        if (updateBatchStatistics) {
                            changeBatchStatus(Status.QY, currentBatch, mode);
                        }

                        DataContext ctx = new DataContext();
                        ctx.put(Constants.DATA_CONTEXT_TARGET_NODE_ID, targetNode.getNodeId());
                        ctx.put(Constants.DATA_CONTEXT_TARGET_NODE_EXTERNAL_ID, targetNode.getExternalId());
                        ctx.put(Constants.DATA_CONTEXT_TARGET_NODE_GROUP_ID, targetNode.getNodeGroupId());
                        ctx.put(Constants.DATA_CONTEXT_TARGET_NODE, targetNode);
                        ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE, sourceNode);
                        ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE_ID, sourceNode.getNodeId());
                        ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE_EXTERNAL_ID, sourceNode.getExternalId());
                        ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE_GROUP_ID, sourceNode.getNodeGroupId());

                        extractInfo.setTotalDataCount(currentBatch.getDataRowCount());
                        currentBatch.resetStats();

                        IDataReader dataReader = buildExtractDataReader(sourceNode, targetNode, currentBatch, extractInfo);
                        new DataProcessor(dataReader, writer, listener, "extract").process(ctx);
                        extractTimeInMs = System.currentTimeMillis() - ts;
                        Statistics stats = getExtractStats(writer);
                        if (stats != null) {
                            transformTimeInMs = stats.get(DataWriterStatisticConstants.TRANSFORMMILLIS);                            
                            currentBatch.setDataRowCount(stats.get(DataWriterStatisticConstants.ROWCOUNT));
                            currentBatch.setDataInsertRowCount(stats.get(DataWriterStatisticConstants.INSERTCOUNT));
                            currentBatch.setDataUpdateRowCount(stats.get(DataWriterStatisticConstants.UPDATECOUNT));
                            currentBatch.setDataDeleteRowCount(stats.get(DataWriterStatisticConstants.DELETECOUNT));
                            currentBatch.setTransformExtractMillis(transformTimeInMs);
                            extractTimeInMs = extractTimeInMs - transformTimeInMs;
                            byteCount = stats.get(DataWriterStatisticConstants.BYTECOUNT);
                            statisticManager.incrementDataBytesExtracted(currentBatch.getChannelId(), byteCount);
                            statisticManager.incrementDataExtracted(currentBatch.getChannelId(),
                                    stats.get(DataWriterStatisticConstants.ROWCOUNT));
                            currentBatch.setByteCount(byteCount);
                            
                            if (currentBatch.isCommonFlag()) {
                                outgoingBatchService.updateCommonBatchExtractStatistics(currentBatch);
                            }
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
                    try {                        
                        IStagedResource resource = getStagedResource(currentBatch);
                        if (resource != null) {
                            resource.setState(State.DONE);
                        }
                    } finally {                        
                        releaseLock(lock, currentBatch, useStagingDataWriter);
                        log.debug("{} released lock for batch {}", targetNode.getNodeId(), currentBatch.getBatchId());
                    }
                }
            }

            if (updateBatchStatistics) {
                long dataEventCount = currentBatch.getDataRowCount();
                long insertEventCount = currentBatch.getDataInsertRowCount();
                
                currentBatch = requeryIfEnoughTimeHasPassed(ts, currentBatch);

                // preserve in the case of a reload event
                if (dataEventCount > currentBatch.getDataRowCount()) {
                    currentBatch.setDataRowCount(dataEventCount);
                }

                // preserve in the case of a reload event
                if (insertEventCount > currentBatch.getDataInsertRowCount()) {
                    currentBatch.setDataInsertRowCount(insertEventCount);
                }

                // only update the current batch after we have possibly
                // "re-queried"
                if (extractTimeInMs > 0) {
                    currentBatch.setExtractMillis(extractTimeInMs);
                }

                if (byteCount > 0) {
                    currentBatch.setByteCount(byteCount);
                }
                
                if (transformTimeInMs > 0) {
                    currentBatch.setTransformExtractMillis(transformTimeInMs);
                }

            }

        }
        return currentBatch;
    }
    
    protected String getSemaphoreKey(OutgoingBatch batch, boolean useStagingDataWriter) {
        return useStagingDataWriter ? Long.toString(batch.getBatchId()) : batch.getNodeBatchId();
    }   

    private BatchLock acquireLock(OutgoingBatch batch, boolean useStagingDataWriter) {
        String semaphoreKey = getSemaphoreKey(batch, useStagingDataWriter);        
        BatchLock lock = null;        
        synchronized (DataExtractorService.this) {
            lock = locks.get(semaphoreKey);
            if (lock == null) {
                lock = new BatchLock(semaphoreKey);
                locks.put(semaphoreKey, lock);
            }
            lock.referenceCount++;
        }
        try {
            lock.acquire(); // In-memory, intra-process lock.
            if (isStagingFileLockRequired(batch)) { // File-system, inter-process lock for clustering.
                StagingFileLock fileLock = acquireStagingFileLock(batch);
                if (fileLock.isAcquired()) {
                    lock.fileLock = fileLock;
                } else { // Didn't get the fileLock, ditch the in-memory lock as well.
                    locks.remove(semaphoreKey);
                    lock.release();
                    throw new SymmetricException("Failed to get extract lock on batch " + batch.getNodeBatchId());
                }
            }
        } catch (InterruptedException e) {
            throw new org.jumpmind.exception.InterruptedException(e);
        }
        
        log.debug("Acquired {}", lock);
        return lock;
    }

    protected StagingFileLock acquireStagingFileLock(OutgoingBatch batch) {
        boolean stagingFileAcquired = false;
        
        StagingFileLock fileLock = null;
        
        int iterations = 0;
        
        while (!stagingFileAcquired) {            
            fileLock = stagingManager.acquireFileLock(getLockingServerInfo(), Constants.STAGING_CATEGORY_OUTGOING,
                    batch.getStagedLocation(), batch.getBatchId());
            stagingFileAcquired = fileLock.isAcquired();
            if (!stagingFileAcquired) {
                if (fileLock.getLockFile() == null) {
                    log.warn("Staging lock file not acquired " + fileLock.getLockFailureMessage());
                    return fileLock;
                }
                long lockAge = fileLock.getLockAge(); 
                if (lockAge >= parameterService.getLong(ParameterConstants.LOCK_TIMEOUT_MS)) {
                    log.warn("Lock {} in place for {} > about to BREAK the lock.",  fileLock.getLockFile(), DurationFormatUtils.formatDurationWords(lockAge, true, true));
                    fileLock.breakLock();
                } else {
                    if ((iterations % 10) == 0) {
                        log.info("Lock {} in place for {}, waiting...",  fileLock.getLockFile(),  DurationFormatUtils.formatDurationWords(lockAge, true, true));    
                    } else {                        
                        log.debug("Lock {} in place for {}, waiting...",  fileLock.getLockFile(),  DurationFormatUtils.formatDurationWords(lockAge, true, true));
                    }
                    try {
                        Thread.sleep(parameterService.getLong(ParameterConstants.LOCK_WAIT_RETRY_MILLIS));
                    } catch (InterruptedException ex) {
                        log.debug("Interrupted.", ex);
                    }
                }
            }
            iterations++;
        }
        
        return fileLock;
    }

    private String getLockingServerInfo() {
        return String.format("Server: '%s' Host: '%s' IP: '%s'", clusterService.getServerId(), AppUtils.getHostName(), AppUtils.getIpAddress());
    }

    protected void releaseLock(BatchLock lock, OutgoingBatch batch, boolean useStagingDataWriter) {
        if (lock != null) {
            synchronized (DataExtractorService.this) {
                lock.referenceCount--;
                if (lock.referenceCount == 0) {
                    locks.remove(lock.semaphoreKey);
                }
                lock.release();
            }
            if (lock.fileLock != null) {
                lock.fileLock.releaseLock();
            }
        }
    }
    
    protected boolean isStagingFileLockRequired(OutgoingBatch batch) {
        return batch.isCommonFlag() 
                && parameterService.is(ParameterConstants.CLUSTER_STAGING_ENABLED);
    }    

    protected void triggerReExtraction(OutgoingBatch currentBatch) {
        // Allow user to reset batch status to NE in the DB to trigger a batch re-extract
        IStagedResource resource = getStagedResource(currentBatch);
        if (resource != null) {
            resource.delete();
        }
    }

    protected ExtractDataReader buildExtractDataReader(Node sourceNode, Node targetNode, OutgoingBatch currentBatch, ProcessInfo processInfo) {
        return new ExtractDataReader(symmetricDialect.getPlatform(), 
                new SelectFromSymDataSource(currentBatch, sourceNode, targetNode, processInfo));
    }

    protected Statistics getExtractStats(IDataWriter writer) {
        Map<Batch, Statistics> statisticsMap = null;
        if (writer instanceof TransformWriter) {
            statisticsMap = ((TransformWriter) writer).getNestedWriter().getStatistics();
        } else {
            statisticsMap = writer.getStatistics();
        }
        if (statisticsMap.size() > 0) {
            return statisticsMap.values().iterator().next();
        } else {
            return null;
        }
    }

    protected IDataWriter wrapWithTransformWriter(Node sourceNode, Node targetNode, ProcessInfo processInfo, IDataWriter dataWriter,
            boolean useStagingDataWriter) {
        TransformWriter transformExtractWriter = null;
        if (useStagingDataWriter) {
            long memoryThresholdInBytes = parameterService
                    .getLong(ParameterConstants.STREAM_TO_FILE_THRESHOLD);                
            transformExtractWriter = createTransformDataWriter(
                    sourceNode,
                    targetNode,
                    new ProcessInfoDataWriter(new StagingDataWriter(memoryThresholdInBytes, true, nodeService
                            .findIdentityNodeId(), Constants.STAGING_CATEGORY_OUTGOING,
                            stagingManager), processInfo));
        } else {
            transformExtractWriter = createTransformDataWriter(sourceNode, targetNode,
                    new ProcessInfoDataWriter(dataWriter, processInfo));
        }
        return transformExtractWriter;
    }

    protected void cleanupIgnoredBatch(Node sourceNode, Node targetNode, OutgoingBatch currentBatch, IDataWriter writer) {
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
            writer.open(ctx);
            writer.start(batch);
            writer.end(batch, false);
        } finally {
            writer.close();
        }
    }
    
    

    protected IStagedResource getStagedResource(OutgoingBatch currentBatch) {
        return stagingManager.find(Constants.STAGING_CATEGORY_OUTGOING,
                currentBatch.getStagedLocation(), currentBatch.getBatchId());
    }

    protected boolean isPreviouslyExtracted(OutgoingBatch currentBatch, boolean acquireReference) {
        IStagedResource previouslyExtracted = getStagedResource(currentBatch);
        if (previouslyExtracted != null && previouslyExtracted.exists() && previouslyExtracted.getState() != State.CREATE) {
            synchronized (DataExtractorService.this) {
                if (previouslyExtracted.exists()) {
                    if (log.isDebugEnabled()) {
                        log.debug("We have already extracted batch {}.  Using the existing extraction: {}", currentBatch.getBatchId(),
                            previouslyExtracted);
                    }
                    if (acquireReference) {
                        previouslyExtracted.reference();
                    }
                    return true;
                }
            }
        }
        return false;
    }

    protected boolean isRetry(OutgoingBatch currentBatch, Node remoteNode) {
        boolean offline = parameterService.is(ParameterConstants.NODE_OFFLINE, false);
        IStagedResource previouslyExtracted = getStagedResource(currentBatch);
        boolean cclient = StringUtils.equals(remoteNode.getDeploymentType(), Constants.DEPLOYMENT_TYPE_CCLIENT);
        return !offline && previouslyExtracted != null && previouslyExtracted.exists() && previouslyExtracted.getState() != State.CREATE
                && currentBatch.getStatus() != OutgoingBatch.Status.RS && currentBatch.getSentCount() > 0 && 
                remoteNode.isVersionGreaterThanOrEqualTo(3, 8, 0)
                && !cclient;
    }

    protected OutgoingBatch sendOutgoingBatch(ProcessInfo processInfo, Node targetNode,
            OutgoingBatch currentBatch, boolean isRetry, IDataWriter dataWriter, BufferedWriter writer, ExtractMode mode) {
        if (currentBatch.getStatus() != Status.OK || ExtractMode.EXTRACT_ONLY == mode) {
            currentBatch.setSentCount(currentBatch.getSentCount() + 1);

            if (currentBatch.getStatus() != Status.RS) {
                currentBatch.setTransferStartTime(new Date());
            }

            long ts = System.currentTimeMillis();
            IStagedResource extractedBatch = getStagedResource(currentBatch);
            if (extractedBatch != null) {
                if (mode == ExtractMode.FOR_SYM_CLIENT && writer != null) {                   
                    if (!isRetry && parameterService.is(ParameterConstants.OUTGOING_BATCH_COPY_TO_INCOMING_STAGING) &&
                            !parameterService.is(ParameterConstants.NODE_OFFLINE, false)) {
                        ISymmetricEngine targetEngine = AbstractSymmetricEngine.findEngineByUrl(targetNode.getSyncUrl());
                        
                        if (targetEngine != null && extractedBatch.isFileResource() && targetEngine.getParameterService().is(ParameterConstants.STREAM_TO_FILE_ENABLED)) {
                            Node sourceNode = nodeService.findIdentity();
                            Node targetNodeByEngine = targetEngine.getNodeService().findIdentity();
                            if(sourceNode.equals(targetNodeByEngine) || !targetNodeByEngine.equals(targetNode)) {
                            	log.warn("Target engine (NodeId {}) is the same engine as the current one and differs from the correct target (NodeId {}). This looks like a miss configuration of the sync urls '{}'", 
                            			targetNodeByEngine.getNodeId(), targetNode.getNodeId(), targetNode.getSyncUrl());
                            } else {
                            	IStagedResource targetResource = targetEngine.getStagingManager().create( 
                                        Constants.STAGING_CATEGORY_INCOMING, Batch.getStagedLocation(false, sourceNode.getNodeId()), 
                                        currentBatch.getBatchId());
                                try {
                                    SymmetricUtils.copyFile(extractedBatch.getFile(), targetResource.getFile());
                                    if(log.isDebugEnabled()) {
                                    	log.debug("Copied file to incoming staging of remote engine {}", targetResource.getFile().getAbsolutePath());
                                    }
                                    
                                    targetResource.setState(State.DONE);

                                    isRetry = true;
                                    
                                 if (currentBatch.getSentCount() == 1) {
	                                	statisticManager.incrementDataSent(currentBatch.getChannelId(), currentBatch.getDataRowCount());
	                            		statisticManager.incrementDataBytesSent(currentBatch.getChannelId(), extractedBatch.getFile().length());
                                 }
                                } catch (Exception e) {   
                                    FileUtils.deleteQuietly(targetResource.getFile());
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                    }
                    
                    Channel channel = configurationService.getChannel(currentBatch.getChannelId());
                    DataContext ctx = new DataContext();
                    transferFromStaging(mode, BatchType.EXTRACT, currentBatch, isRetry, extractedBatch, writer, ctx,
                            channel.getMaxKBytesPerSecond());
                } else {
                    IDataReader dataReader = new ProtocolDataReader(BatchType.EXTRACT,
                            currentBatch.getNodeId(), extractedBatch);
    
                    DataContext ctx = new DataContext();
                    ctx.put(Constants.DATA_CONTEXT_TARGET_NODE, targetNode);
                    ctx.put(Constants.DATA_CONTEXT_SOURCE_NODE, nodeService.findIdentity());
                    new DataProcessor(dataReader, new ProcessInfoDataWriter(dataWriter, processInfo), "send from stage")
                            .process(ctx);
                    if (dataReader.getStatistics().size() > 0 && currentBatch.getSentCount() == 1) {
                        Statistics stats = dataReader.getStatistics().values().iterator().next();
                        statisticManager.incrementDataSent(currentBatch.getChannelId(),
                                stats.get(DataReaderStatistics.READ_RECORD_COUNT));
                        long byteCount = stats.get(DataReaderStatistics.READ_BYTE_COUNT);
                        statisticManager.incrementDataBytesSent(currentBatch.getChannelId(), byteCount);
                    } else {
                        log.warn("Could not find recorded statistics for batch {}",
                                currentBatch.getNodeBatchId());
                    }
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

    protected void transferFromStaging(ExtractMode mode, BatchType batchType, OutgoingBatch batch, boolean isRetry, IStagedResource stagedResource,
            BufferedWriter writer, DataContext context, BigDecimal maxKBytesPerSec) {
        final int MAX_WRITE_LENGTH = 32768;
        BufferedReader reader = stagedResource.getReader();
        try {
            // Retry means we've sent this batch before, so let's ask to
            // retry the batch from the target's staging
            if (isRetry) {
                String line = null;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith(CsvConstants.BATCH)) {
                        if (nodeService.findNode(batch.getNodeId()).isVersionGreaterThanOrEqualTo(3, 9, 0)) {
                            writer.write(getBatchStatsColumns());
                            writer.newLine();
                            writer.write(getBatchStats(batch));
                            writer.newLine();

                        }
                        writer.write(CsvConstants.RETRY + "," + batch.getBatchId());
                        writer.newLine();
                        writer.write(CsvConstants.COMMIT + "," + batch.getBatchId());
                        writer.newLine();
                        break;
                    } else {
                        writer.write(line);
                        writer.newLine();
                    }
                }
                
                writer.flush();
            } else {
                long totalCharsRead = 0, totalBytesRead = 0;
                int numCharsRead = 0, numBytesRead = 0;
                long startTime = System.currentTimeMillis(), ts = startTime, bts = startTime;
                boolean isThrottled = maxKBytesPerSec != null && maxKBytesPerSec.compareTo(BigDecimal.ZERO) > 0;
                long totalThrottleTime = 0;
                int bufferSize = MAX_WRITE_LENGTH;

                if (isThrottled) {
                    bufferSize = maxKBytesPerSec.multiply(new BigDecimal(1024)).intValue();
                }
                char[] buffer = new char[bufferSize];

                boolean batchStatsWritten = false;
                String prevBuffer = "";
                while ((numCharsRead = reader.read(buffer)) != -1) {
                    if (!batchStatsWritten && nodeService.findNode(batch.getNodeId()).isVersionGreaterThanOrEqualTo(3, 9, 0)) {
                        batchStatsWritten = writeBatchStats(writer, buffer, numCharsRead, prevBuffer, batch);
                        prevBuffer = new String(buffer);
                    } else {
                        writer.write(buffer, 0, numCharsRead);
                    }
                    totalCharsRead += numCharsRead;

                    if (Thread.currentThread().isInterrupted()) {
                        throw new IoException("This thread was interrupted");
                    }

                    long batchStatusUpdateMillis = parameterService.getLong(ParameterConstants.OUTGOING_BATCH_UPDATE_STATUS_MILLIS);
                    if (System.currentTimeMillis() - ts > batchStatusUpdateMillis && batch.getStatus() != Status.SE && batch.getStatus() != Status.RS) {
                        changeBatchStatus(Status.SE, batch, mode);
                    }
                    if (System.currentTimeMillis() - ts > LOG_PROCESS_SUMMARY_THRESHOLD) {
                        log.info(
                                "Batch '{}', for node '{}', for process 'send from stage' has been processing for {} seconds.  "
                                        + "The following stats have been gathered: {}",
                                new Object[] { batch.getBatchId(), batch.getNodeId(), (System.currentTimeMillis() - startTime) / 1000,
                                        "CHARS=" + totalCharsRead });
                        ts = System.currentTimeMillis();
                    }

                    if (isThrottled) {
                        numBytesRead += new String(buffer, 0, numCharsRead).getBytes().length;
                        totalBytesRead += numBytesRead;
                        if (numBytesRead >= bufferSize) {
                            long expectedMillis = (long) (((numBytesRead / 1024f) / maxKBytesPerSec.floatValue()) * 1000);
                            long actualMillis = System.currentTimeMillis() - bts;
                            if (actualMillis < expectedMillis) {
                                totalThrottleTime += expectedMillis - actualMillis;
                                Thread.sleep(expectedMillis - actualMillis);
                            }
                            numBytesRead = 0;
                            bts = System.currentTimeMillis();
                        }
                    } else {
                        totalBytesRead += new String(buffer, 0, numCharsRead).getBytes().length;
                    }
                }
                
                if (batch.getSentCount() == 1) {
	                statisticManager.incrementDataSent(batch.getChannelId(), batch.getDataRowCount());
	                statisticManager.incrementDataBytesSent(batch.getChannelId(), totalBytesRead);
                }
                
                if (log.isDebugEnabled() && totalThrottleTime > 0) {
                    log.debug("Batch '{}' for node '{}' took {}ms for {} bytes and was throttled for {}ms because limit is set to {} KB/s",
                            batch.getBatchId(), batch.getNodeId(), (System.currentTimeMillis() - startTime), totalBytesRead,
                            totalThrottleTime, maxKBytesPerSec);
                }
            }
            
            if (writer instanceof BatchBufferedWriter) {
                ((BatchBufferedWriter)writer).getBatchIds().add(batch.getBatchId());
                
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            stagedResource.close();
            stagedResource.dereference();
            if (!stagedResource.isFileResource() && !stagedResource.isInUse()) {
                synchronized(DataExtractorService.this) {
                    if (!stagedResource.isFileResource() && !stagedResource.isInUse()) {
                        stagedResource.delete();
                    }
                }
            }
        }
    }
    
    protected int findStatsIndex(String bufferString, String prevBuffer) {
        int index = -1;
        String fullBuffer = prevBuffer + bufferString;

        String pattern = "\n" + CsvConstants.BATCH + "\\s*,\\s*\\d*\r*\n";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(fullBuffer);
        if (m.find()) {
            String group = m.group(0);
            int start = fullBuffer.indexOf(group);

            if (start + group.length() > prevBuffer.length()) {
                index = start + group.length() - prevBuffer.length();
            } else {
                index = start + group.length();
            }
        }

        return index;
    }

    protected boolean writeBatchStats(BufferedWriter writer, char[] buffer, int bufferSize, String prevBuffer, OutgoingBatch batch)
            throws IOException {
        String bufferString = new String(buffer);
        int index = findStatsIndex(bufferString, prevBuffer);
        
        if (index > 0) {
            char prefix[] = Arrays.copyOf(buffer, index);
            writer.write(prefix, 0, index);
        }
        if (index > -1) {
            String stats = getBatchStatsColumns() + System.lineSeparator() + getBatchStats(batch) + System.lineSeparator();
            
            char statsBuffer[] = stats.toCharArray();
            writer.write(statsBuffer, 0, statsBuffer.length);

            char suffix[] = Arrays.copyOfRange(buffer, index, buffer.length);
            writer.write(suffix, 0, bufferSize - index);
        } else {
            writer.write(buffer, 0, bufferSize);
        }

        return index > -1;
    }
    
    protected String getBatchStatsColumns() {
        return StringUtils.join(new String[] { CsvConstants.STATS_COLUMNS, DataReaderStatistics.LOAD_FLAG, DataReaderStatistics.EXTRACT_COUNT,
                DataReaderStatistics.SENT_COUNT, DataReaderStatistics.LOAD_COUNT, DataReaderStatistics.LOAD_ID,
                DataReaderStatistics.COMMON_FLAG, DataReaderStatistics.ROUTER_MILLIS, DataReaderStatistics.EXTRACT_MILLIS,
                DataReaderStatistics.TRANSFORM_EXTRACT_MILLIS, DataReaderStatistics.TRANSFORM_LOAD_MILLIS,
                DataReaderStatistics.RELOAD_ROW_COUNT, DataReaderStatistics.OTHER_ROW_COUNT, DataReaderStatistics.DATA_ROW_COUNT,
                DataReaderStatistics.DATA_INSERT_ROW_COUNT, DataReaderStatistics.DATA_UPDATE_ROW_COUNT,
                DataReaderStatistics.DATA_DELETE_ROW_COUNT, DataReaderStatistics.EXTRACT_ROW_COUNT,
                DataReaderStatistics.EXTRACT_INSERT_ROW_COUNT, DataReaderStatistics.EXTRACT_UPDATE_ROW_COUNT,
                DataReaderStatistics.EXTRACT_DELETE_ROW_COUNT, DataReaderStatistics.FAILED_DATA_ID }, ',');
    }

    protected String getBatchStats(OutgoingBatch batch) {
        return StringUtils.join(new String[] { CsvConstants.STATS, String.valueOf(batch.isLoadFlag() ? 1 : 0),
                String.valueOf(batch.getExtractCount()), String.valueOf(batch.getSentCount()), String.valueOf(batch.getLoadCount()),
                String.valueOf(batch.getLoadId()), String.valueOf(batch.isCommonFlag() ? 1 : 0), String.valueOf(batch.getRouterMillis()),
                String.valueOf(batch.getExtractMillis()), String.valueOf(batch.getTransformExtractMillis()),
                String.valueOf(batch.getTransformLoadMillis()), String.valueOf(batch.getReloadRowCount()),
                String.valueOf(batch.getOtherRowCount()), String.valueOf(batch.getDataRowCount()),
                String.valueOf(batch.getDataInsertRowCount()), String.valueOf(batch.getDataUpdateRowCount()),
                String.valueOf(batch.getDataDeleteRowCount()), String.valueOf(batch.getExtractRowCount()),
                String.valueOf(batch.getExtractInsertRowCount()), String.valueOf(batch.getExtractUpdateRowCount()),
                String.valueOf(batch.getExtractDeleteRowCount()), String.valueOf(batch.getFailedDataId()) }, ',');
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
        TransformWriter transformExtractWriter = new TransformWriter(symmetricDialect.getTargetPlatform(), TransformPoint.EXTRACT, extractWriter, 
                transformService.getColumnTransforms(), transforms);
        return transformExtractWriter;
    }

    protected Table lookupAndOrderColumnsAccordingToTriggerHistory(String routerId,
            TriggerHistory triggerHistory, Node sourceNode, Node targetNode, 
            boolean setTargetTableName, boolean useDatabaseDefinition) {
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
                table.setCatalog(replaceVariables(sourceNode, targetNode, router.getTargetCatalogName()));
            }

            if (StringUtils.equals(Constants.NONE_TOKEN, router.getTargetSchemaName())) {
                table.setSchema(null);
            } else if (StringUtils.isNotBlank(router.getTargetSchemaName())) {
                table.setSchema(replaceVariables(sourceNode, targetNode, router.getTargetSchemaName()));
            }

            if (StringUtils.isNotBlank(router.getTargetTableName())) {
                table.setName(router.getTargetTableName());
            }
        }
        return table;
    }

    protected String replaceVariables(Node sourceNode, Node targetNode, String str) {
        str = FormatUtils.replace("sourceNodeId", sourceNode.getNodeId(), str);
        str = FormatUtils.replace("sourceExternalId", sourceNode.getExternalId(), str);
        str = FormatUtils.replace("sourceNodeGroupId", sourceNode.getNodeGroupId(), str);
        str = FormatUtils.replace("targetNodeId", targetNode.getNodeGroupId(), str);
        str = FormatUtils.replace("targetExternalId", targetNode.getExternalId(), str);
        str = FormatUtils.replace("targetNodeGroupId", targetNode.getNodeGroupId(), str);
        return str;
    }
    
    public RemoteNodeStatuses queueWork(boolean force) {
        final RemoteNodeStatuses statuses = new RemoteNodeStatuses(configurationService.getChannels(false));
        Node identity = nodeService.findIdentity();
        if (identity != null) {
            if (force || clusterService.lock(ClusterConstants.INITIAL_LOAD_EXTRACT)) {
                try {
                    List<NodeQueuePair> nodes = getExtractRequestNodes();
                    for (NodeQueuePair pair : nodes) {
                        clusterService.refreshLock(ClusterConstants.INITIAL_LOAD_EXTRACT);
                        queue(pair.getNodeId(), pair.getQueue(), statuses);
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

    protected void queue(String nodeId, String queue, RemoteNodeStatuses statuses) {
        final NodeCommunication.CommunicationType TYPE = NodeCommunication.CommunicationType.EXTRACT;
        int availableThreads = nodeCommunicationService.getAvailableThreads(TYPE);
        NodeCommunication lock = nodeCommunicationService.find(nodeId, queue, TYPE);
        if (availableThreads > 0) {
            nodeCommunicationService.execute(lock, statuses, this);
        }
    }

    public List<NodeQueuePair> getExtractRequestNodes() {
        return sqlTemplate.query(getSql("selectNodeIdsForExtractSql"), new NodeQueuePairMapper(),
                ExtractStatus.NE.name());
    }

    private static class NodeQueuePair {
        private String nodeId;
        private String queue;
        public String getNodeId() {
            return nodeId;
        }
        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }
        public String getQueue() {
            return queue;
        }
        public void setQueue(String queue) {
            this.queue = queue;
        }
    }
    
    static class NodeQueuePairMapper implements ISqlRowMapper<NodeQueuePair> {
        @Override
        public NodeQueuePair mapRow(Row row) {
            NodeQueuePair pair = new NodeQueuePair();
            pair.setNodeId(row.getString("node_id"));
            pair.setQueue(row.getString("queue"));
            return pair;
        }
    }

    public List<ExtractRequest> getExtractRequestsForNode(NodeCommunication nodeCommunication) {
        return sqlTemplate.query(getSql("selectExtractRequestForNodeSql"),
                new ExtractRequestMapper(), nodeCommunication.getNodeId(), nodeCommunication.getQueue()
                , ExtractRequest.ExtractStatus.NE.name());
    }
    
    @Override
    public void resetExtractRequest(OutgoingBatch batch) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            batch.setStatus(Status.RQ);
            outgoingBatchService.updateOutgoingBatch(transaction, batch);

            transaction.prepareAndExecute(getSql("resetExtractRequestStatus"), ExtractStatus.NE.name(),
                batch.getBatchId(), batch.getBatchId(), batch.getNodeId());
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
    }

    public void requestExtractRequest(ISqlTransaction transaction, String nodeId, String queue,
            TriggerRouter triggerRouter, long startBatchId, long endBatchId) {
        long requestId = sequenceService.nextVal(transaction, Constants.SEQUENCE_EXTRACT_REQ);
        transaction.prepareAndExecute(getSql("insertExtractRequestSql"),
                new Object[] { requestId, nodeId, queue, ExtractStatus.NE.name(), startBatchId,
                        endBatchId, triggerRouter.getTrigger().getTriggerId(),
                        triggerRouter.getRouter().getRouterId() }, new int[] { Types.BIGINT, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.BIGINT, Types.BIGINT, Types.VARCHAR, Types.VARCHAR });
    }

    protected void updateExtractRequestStatus(ISqlTransaction transaction, long extractId,
            ExtractStatus status) {
        transaction.prepareAndExecute(getSql("updateExtractRequestStatus"), status.name(),
                extractId);
    }
    
    protected boolean canProcessExtractRequest(ExtractRequest request, CommunicationType communicationType) {
        Trigger trigger = this.triggerRouterService.getTriggerById(request.getTriggerId());
        if (trigger == null || !trigger.getSourceTableName().equalsIgnoreCase(TableConstants.getTableName(tablePrefix,
                TableConstants.SYM_FILE_SNAPSHOT))) {
            return true;
        } else {            
            return false;
        }
    }    

    /**
     * This is a callback method used by the NodeCommunicationService that extracts an initial load
     * in the background.
     */
    public void execute(NodeCommunication nodeCommunication, RemoteNodeStatus status) {
        if (!isApplicable(nodeCommunication)) {
            log.debug("{} failed isApplicable check and will not run.", this);
            return;
        }
        
        List<ExtractRequest> requests = getExtractRequestsForNode(nodeCommunication);
        long ts = System.currentTimeMillis();
        /*
         * Process extract requests until it has taken longer than 30 seconds, and then
         * allow the process to return so progress status can be seen.
         */
        for (int i = 0; i < requests.size()
                && (System.currentTimeMillis() - ts) <= Constants.LONG_OPERATION_THRESHOLD; i++) {
            ExtractRequest request = requests.get(i);
            if (!canProcessExtractRequest(request, nodeCommunication.getCommunicationType())){
                continue;
            }                
            Node identity = nodeService.findIdentity();
            Node targetNode = nodeService.findNode(nodeCommunication.getNodeId());
            log.info(
                    "Extracting batches for request {}. Starting at batch {}.  Ending at batch {}",
                    new Object[] { request.getRequestId(), request.getStartBatchId(),
                            request.getEndBatchId() });
            List<OutgoingBatch> batches = outgoingBatchService.getOutgoingBatchRange(
                    request.getStartBatchId(), request.getEndBatchId()).getBatches();

            ProcessInfo processInfo = statisticManager.newProcessInfo(new ProcessInfoKey(identity
                    .getNodeId(), nodeCommunication.getQueue(), nodeCommunication.getNodeId(),
                    getProcessType()));
            processInfo.setTotalBatchCount(batches.size());
            try {
                boolean areBatchesOk = true;

                /*
                 * check to see if batches have been OK'd by another reload
                 * request 
                 */
                for (OutgoingBatch outgoingBatch : batches) {
                    if (outgoingBatch.getStatus() != Status.OK) {
                        areBatchesOk = false;
                        break;
                    }
                }

                if (!areBatchesOk) {

                    Channel channel = configurationService
                            .getChannel(batches.get(0).getChannelId());
                    /*
                     * "Trick" the extractor to extract one reload batch, but we
                     * will split it across the N batches when writing it
                     */
                    OutgoingBatch firstBatch = batches.get(0);
                    processInfo.setCurrentLoadId(firstBatch.getLoadId());
                    IStagedResource resource = getStagedResource(firstBatch);
                    if (resource != null && resource.exists() && resource.getState() != State.CREATE) {
                        resource.delete();
                    }
                    
                    MultiBatchStagingWriter multiBatchStagingWriter = 
                            buildMultiBatchStagingWriter(request, identity, targetNode, batches, processInfo, channel);
                    
                    extractOutgoingBatch(processInfo, targetNode, multiBatchStagingWriter, 
                            firstBatch, false, false, ExtractMode.FOR_SYM_CLIENT, new ClusterLockRefreshListener(clusterService));
                    
                    for (OutgoingBatch outgoingBatch : batches) {
                        resource = getStagedResource(outgoingBatch);  
                        if (resource != null) {
                            resource.setState(State.DONE);        
                        }
                    }

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
                        break;
                    }
                }

                ISqlTransaction transaction = null;
                try {
                    transaction = sqlTemplate.startSqlTransaction();
                    updateExtractRequestStatus(transaction, request.getRequestId(),
                            ExtractStatus.OK);

                    if (!areBatchesOk) {
                        for (OutgoingBatch outgoingBatch : batches) {
                            if (!parameterService.is(ParameterConstants.INITIAL_LOAD_EXTRACT_AND_SEND_WHEN_STAGED, false)) {
                                outgoingBatch.setStatus(Status.NE);
                                outgoingBatchService.updateOutgoingBatch(transaction, outgoingBatch);
                            } else if (outgoingBatch.getStatus() == Status.RQ) {
                                log.info("Batch {} was empty after extract in background and will be ignored.",
                                        new Object[] { outgoingBatch.getNodeBatchId() });
                                outgoingBatch.setStatus(Status.IG);
                                outgoingBatchService.updateOutgoingBatch(transaction, outgoingBatch);

                            }
                        }
                    } else {
                        log.info("Batches already had an OK status for request {}, batches {} to {}.  Not updating the status to NE",
                                new Object[] { request.getRequestId(), request.getStartBatchId(), request.getEndBatchId() });
                    }
                    transaction.commit();
                    log.info("Done extracting {} batches for request {}", (request.getEndBatchId() - request.getStartBatchId()) + 1, request.getRequestId());
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
                processInfo.setStatus(ProcessInfo.ProcessStatus.OK);

            } catch (CancellationException ex) {
                log.info("Cancelled extract request {}. Starting at batch {}.  Ending at batch {}",
                        new Object[] { request.getRequestId(), request.getStartBatchId(),
                        request.getEndBatchId() });
                processInfo.setStatus(ProcessInfo.ProcessStatus.OK);
            } catch (RuntimeException ex) {
                log.warn(
                        "Failed to extract batches for request {}. Starting at batch {}.  Ending at batch {}",
                        new Object[] { request.getRequestId(), request.getStartBatchId(),
                                request.getEndBatchId() });
                processInfo.setStatus(ProcessInfo.ProcessStatus.ERROR);
                List<OutgoingBatch> checkBatches = outgoingBatchService.getOutgoingBatchRange(
                        request.getStartBatchId(), request.getEndBatchId()).getBatches();
                for (OutgoingBatch outgoingBatch : checkBatches) {
                    outgoingBatch.setStatus(Status.RQ);
                    IStagedResource resource = getStagedResource(outgoingBatch);
                    if (resource != null) {
                        resource.close();
                        resource.delete();
                    }
                    outgoingBatchService.updateOutgoingBatch(outgoingBatch);
                }                
                throw ex;
            }
        }
    }
    
    protected boolean isApplicable(NodeCommunication nodeCommunication) {
        return nodeCommunication.getCommunicationType() != CommunicationType.FILE_XTRCT;
    }    

    protected MultiBatchStagingWriter buildMultiBatchStagingWriter(ExtractRequest request, Node sourceNode, Node targetNode, List<OutgoingBatch> batches,
            ProcessInfo processInfo, Channel channel) {
        MultiBatchStagingWriter multiBatchStatingWriter = new MultiBatchStagingWriter(this, request, sourceNode.getNodeId(), stagingManager,
                batches, channel.getMaxBatchSize(), processInfo);
        return multiBatchStatingWriter;
    }
    
    protected ProcessType getProcessType() {
        return ProcessType.INITIAL_LOAD_EXTRACT_JOB;
    }
    
    protected boolean hasLobsThatNeedExtract(Table table, CsvData data) {
        if (table.containsLobColumns(platform)) {
            String[] colNames = table.getColumnNames();
            Map<String, String> colMap = data.toColumnNameValuePairs(colNames, CsvData.ROW_DATA);

            List<Column> lobColumns = table.getLobColumns(platform);
            for (Column c : lobColumns) {
                String value = colMap.get(c.getName());
                if (value != null && (value.equals("\b") || value.equals("08"))) {
                    return true;
                }
            }
        }
        return false;
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
            request.setTriggerId(row.getString("trigger_id"));
            request.setRouterId(row.getString("router_id"));
            request.setTriggerRouter(triggerRouterService.findTriggerRouterById(
                    row.getString("trigger_id"), row.getString("router_id"), false));
            request.setQueue(row.getString("queue"));
            return request;
        }
    }

    class ColumnsAccordingToTriggerHistory {
        class CacheKey{
            private String routerId;
            private int triggerHistoryId;
            private boolean setTargetTableName;
            private boolean useDatabaseDefinition;
            
            public CacheKey(String routerId, int triggerHistoryId, boolean setTargetTableName,
                    boolean useDatabaseDefinition) {
                 this.routerId = routerId;
                 this.triggerHistoryId = triggerHistoryId;
                 this.setTargetTableName = setTargetTableName;
                 this.useDatabaseDefinition = useDatabaseDefinition;
            }
            @Override
            public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + ((routerId == null) ? 0 : routerId.hashCode());
                result = prime * result + (setTargetTableName ? 1231 : 1237);
                result = prime * result + triggerHistoryId;
                result = prime * result + (useDatabaseDefinition ? 1231 : 1237);
                return result;
            }
            @Override
            public boolean equals(Object obj) {
                if (this == obj)
                    return true;
                if (obj == null)
                    return false;
                if (getClass() != obj.getClass())
                    return false;
                CacheKey other = (CacheKey) obj;
                if (routerId == null) {
                    if (other.routerId != null)
                        return false;
                } else if (!routerId.equals(other.routerId))
                    return false;
                if (setTargetTableName != other.setTargetTableName)
                    return false;
                if (triggerHistoryId != other.triggerHistoryId)
                    return false;
                if (useDatabaseDefinition != other.useDatabaseDefinition)
                    return false;
                return true;
            }
        }
        
        Map<CacheKey, Table> cache = new HashMap<CacheKey, Table>();
        Node sourceNode;
        Node targetNode;
        
        public ColumnsAccordingToTriggerHistory(Node sourceNode, Node targetNode) {
            this.sourceNode = sourceNode;
            this.targetNode = targetNode;
        }
        public Table lookup(String routerId, TriggerHistory triggerHistory, boolean setTargetTableName, boolean useDatabaseDefinition) {            
            CacheKey key = new CacheKey(routerId, triggerHistory.getTriggerHistoryId(), setTargetTableName, useDatabaseDefinition);
            Table table = cache.get(key);
            if (table == null) {
                table = lookupAndOrderColumnsAccordingToTriggerHistory(routerId, triggerHistory, sourceNode,
                        targetNode, setTargetTableName, useDatabaseDefinition);
                cache.put(key, table);
            }
            return table;
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
        
        private ColumnsAccordingToTriggerHistory columnsAccordingToTriggerHistory;

        public SelectFromSymDataSource(OutgoingBatch outgoingBatch, 
                Node sourceNode, Node targetNode, ProcessInfo processInfo) {
            this.processInfo = processInfo;
            this.outgoingBatch = outgoingBatch;
            this.batch = new Batch(BatchType.EXTRACT, outgoingBatch.getBatchId(),
                    outgoingBatch.getChannelId(), symmetricDialect.getBinaryEncoding(),
                    sourceNode.getNodeId(), outgoingBatch.getNodeId(), outgoingBatch.isCommonFlag());
            this.targetNode = targetNode;
            this.columnsAccordingToTriggerHistory = new ColumnsAccordingToTriggerHistory(sourceNode, targetNode);
            this.outgoingBatch.resetExtractRowStats();
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
                } else {
                    this.requiresLobSelectedFromSource = this.reloadSource.requiresLobsSelectedFromSource(data);
                }
                lastTriggerHistory = null;
            }

            if (data == null) {
                data = this.cursor.next();
                if (data != null) {
                    String routerId = data.getAttribute(CsvData.ATTRIBUTE_ROUTER_ID);

                    if (data.getDataEventType() == DataEventType.RELOAD) {
                        TriggerHistory triggerHistory = data.getTriggerHistory();
                        String triggerId = triggerHistory.getTriggerId();

                        TriggerRouter triggerRouter = triggerRouterService
                                .getTriggerRouterForCurrentNode(triggerId, routerId, false);
                        if (triggerRouter != null) {
                            processInfo.setCurrentTableName(triggerHistory.getSourceTableName());
                            
                            String initialLoadSelect = data.getRowData();
                            if (initialLoadSelect == null && triggerRouter.getTrigger().isStreamRow()) {
                                //if (sourceTable == null) {
                                    sourceTable = columnsAccordingToTriggerHistory.lookup(triggerRouter
                                            .getRouter().getRouterId(), triggerHistory, false, true);
                               // }
                                Column[] columns = sourceTable.getPrimaryKeyColumns();
                                DmlStatement dmlStmt = platform.createDmlStatement(DmlType.WHERE, sourceTable, null);
                                String[] pkData = data.getParsedData(CsvData.PK_DATA);
                                Row row = new Row(columns.length);
                                
                                for (int i = 0; i < columns.length; i++) {
                                    row.put(columns[i].getName(), pkData[i]);
                                }
                                initialLoadSelect = dmlStmt.buildDynamicSql(batch.getBinaryEncoding(), row, false, true, columns);
                                if (initialLoadSelect.endsWith(platform.getDatabaseInfo().getSqlCommandDelimiter())) {
                                    initialLoadSelect = initialLoadSelect.substring(0, 
                                            initialLoadSelect.length() - platform.getDatabaseInfo().getSqlCommandDelimiter().length());
                                }
                            }
                            
                            SelectFromTableEvent event = new SelectFromTableEvent(targetNode,
                                    triggerRouter, triggerHistory, initialLoadSelect);
                            this.reloadSource = new SelectFromTableSource(outgoingBatch, batch,
                                    event);
                            data = (Data) this.reloadSource.next();
                            this.sourceTable = reloadSource.getSourceTable();
                            this.targetTable = this.reloadSource.getTargetTable();
                            this.requiresLobSelectedFromSource = this.reloadSource.requiresLobsSelectedFromSource(data);
                            
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
                        TriggerHistory triggerHistory = data.getTriggerHistory();
                        Trigger trigger = triggerRouterService.getTriggerById(
                                triggerHistory.getTriggerId(), false);
                        boolean isFileParserRouter = triggerHistory.getTriggerId().equals(AbstractFileParsingRouter.TRIGGER_ID_FILE_PARSER);
                        if (trigger == null && !isFileParserRouter) {
                            log.warn(
                                    "Could not locate a trigger with the id of {} for table {} (data id {} with trigger hist id {}). It's possible this trigger was deleted before the batch could be extracted.",
                                    new Object[] { triggerHistory.getTriggerId(),
                                            triggerHistory.getSourceTableName(),
                                            data.getDataId(),
                                            triggerHistory.getTriggerHistoryId() });
                        }
                        
                        if (lastTriggerHistory == null || lastTriggerHistory
                                .getTriggerHistoryId() != triggerHistory.getTriggerHistoryId() || 
                                lastRouterId == null || !lastRouterId.equals(routerId)) {
                            
                            this.sourceTable = columnsAccordingToTriggerHistory.lookup(
                                        routerId, triggerHistory, false, !isFileParserRouter);
                            
                            this.targetTable = columnsAccordingToTriggerHistory.lookup(
                                    routerId, triggerHistory, true, false);
                            
                            if (trigger != null && trigger.isUseStreamLobs() || (data.getRowData() != null && hasLobsThatNeedExtract(sourceTable, data))) {
                                this.requiresLobSelectedFromSource = true;
                            } else {
                                this.requiresLobSelectedFromSource = false;
                            }
                        }

                        data.setNoBinaryOldData(requiresLobSelectedFromSource
                                || symmetricDialect.getName().equals(
                                        DatabaseNamesConstants.MSSQL2000)
                                || symmetricDialect.getName().equals(
                                        DatabaseNamesConstants.MSSQL2005)
                                || symmetricDialect.getName().equals(
                                        DatabaseNamesConstants.MSSQL2008));
                        
                        outgoingBatch.incrementExtractRowCount();
                        outgoingBatch.incrementExtractRowCount(data.getDataEventType());
                            
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
                            
                            this.targetTable = columnsAccordingToTriggerHistory.lookup(
                                    routerId, triggerHistory, true, true);
                            Table copyTargetTable = this.targetTable.copy();
                            
                            Database db = new Database();
                            db.setName("dataextractor");                            
                            db.setCatalog(copyTargetTable.getCatalog());
                            db.setSchema(copyTargetTable.getSchema());
                            db.addTable(copyTargetTable);
                            if (excludeDefaults) {
                                Column[] columns = copyTargetTable.getColumns();
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
                            	copyTargetTable.removeAllForeignKeys();
                            }
                            
                            if (parameterService.is(ParameterConstants.CREATE_TABLE_WITHOUT_PK_IF_SOURCE_WITHOUT_PK, false)
                            	&& sourceTable.getPrimaryKeyColumnCount() == 0
                            	&& copyTargetTable.getPrimaryKeyColumnCount() > 0) {
                            	
                            		for (Column column : copyTargetTable.getColumns()) {
                            			column.setPrimaryKey(false);
                            		}
                            	
                            }
                            if (parameterService.is(ParameterConstants.MYSQL_TINYINT_DDL_TO_BOOLEAN, false)) {
	                            	for (Column column : copyTargetTable.getColumns()) {
	                            		if (column.getJdbcTypeCode() == Types.TINYINT) {
	                            			column.setJdbcTypeCode(Types.BOOLEAN);
	                            			column.setMappedTypeCode(Types.BOOLEAN);
	                            		}
	                        		}
                            }
                            data.setRowData(CsvUtils.escapeCsvData(DatabaseXmlUtil.toXml(db)));
                        }
                    }

                    if (data != null) {
                        lastTriggerHistory = data.getTriggerHistory();
                        lastRouterId = routerId;
                    }
                } else {
                    closeCursor();
                }
            }
            return data;
        }

        public boolean requiresLobsSelectedFromSource(CsvData data) {
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
        
        private ColumnsAccordingToTriggerHistory columnsAccordingToTriggerHistory;
        
        private String overrideSelectSql;

        private boolean isSelfReferencingFk;
        
        private int selfRefLevel;
        
        private String selfRefParentColumnName;
        
        private String selfRefChildColumnName;
        
        boolean isFirstRow;

        public SelectFromTableSource(OutgoingBatch outgoingBatch, Batch batch,
                SelectFromTableEvent event) {
            this.outgoingBatch = outgoingBatch;
            List<SelectFromTableEvent> initialLoadEvents = new ArrayList<DataExtractorService.SelectFromTableEvent>(
                    1);
            initialLoadEvents.add(event);
            this.outgoingBatch.resetExtractRowStats();
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
            this.columnsAccordingToTriggerHistory = new ColumnsAccordingToTriggerHistory(nodeService.findIdentity(), node);
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
                outgoingBatch.incrementExtractRowCount();
                outgoingBatch.incrementExtractRowCount(data.getDataEventType());
            }

            return data;
        }

        protected CsvData selectNext() {
            CsvData data = null;
            if (this.currentInitialLoadEvent == null && selectFromTableEventsToSend.size() > 0) {
                this.currentInitialLoadEvent = selectFromTableEventsToSend.remove(0);
                TriggerHistory history = this.currentInitialLoadEvent.getTriggerHistory();
                this.isSelfReferencingFk = false;
                this.isFirstRow = true;
                if (this.currentInitialLoadEvent.containsData()) {
                    data = this.currentInitialLoadEvent.getData();
                    this.currentInitialLoadEvent = null;
                    this.sourceTable = columnsAccordingToTriggerHistory.lookup(
                            (String) data.getAttribute(CsvData.ATTRIBUTE_ROUTER_ID), history, false, true);
                    this.targetTable = columnsAccordingToTriggerHistory.lookup(
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
                    this.sourceTable = columnsAccordingToTriggerHistory.lookup(triggerRouter
                            .getRouter().getRouterId(), history, false, true);
                    this.targetTable = columnsAccordingToTriggerHistory.lookup(triggerRouter
                            .getRouter().getRouterId(), history, true, false);
                    
                    this.overrideSelectSql = currentInitialLoadEvent.getInitialLoadSelect();
                    if (overrideSelectSql != null && overrideSelectSql.trim().toUpperCase().startsWith("WHERE")) {
                        overrideSelectSql = overrideSelectSql.trim().substring(5);
                    }

                    ForeignKey fk = this.sourceTable.getSelfReferencingForeignKey();
                    if (fk != null) {
                        Reference[] refs = fk.getReferences();
                        if (refs.length == 1) {
                            this.isSelfReferencingFk = true;
                            this.selfRefParentColumnName = refs[0].getLocalColumnName();
                            this.selfRefChildColumnName = refs[0].getForeignColumnName();
                            this.selfRefLevel = 0;
                            log.info("Ordering rows for table {} using self-referencing foreign key {} -> {}", 
                                    this.sourceTable.getName(), this.selfRefParentColumnName, this.selfRefChildColumnName);
                        } else {
                            log.warn("Unable to order rows for self-referencing foreign key because it contains multiple columns");
                        }
                    }

                    this.startNewCursor(history, triggerRouter);
                }
            }

            if (this.cursor != null) {
                data = this.cursor.next();
                if (data == null) {
                    closeCursor();
                    if (isSelfReferencingFk && !this.isFirstRow) {
                        this.selfRefLevel++;
                        this.startNewCursor(this.currentInitialLoadEvent.getTriggerHistory(), triggerRouter);
                        this.isFirstRow = true;
                    } else {
                        this.currentInitialLoadEvent = null;
                    }
                    data = next();
                } else if (this.isFirstRow) {
                    this.isFirstRow = false;
                }
            }

            return data;
        }

        protected void closeCursor() {
            if (this.cursor != null) {
                this.cursor.close();
                this.cursor = null;
            }
        }

        protected void startNewCursor(final TriggerHistory triggerHistory,
                final TriggerRouter triggerRouter) {

            String selectSql = overrideSelectSql;
            if (isSelfReferencingFk) {
                if (selectSql == null) {
                    selectSql = "";
                } else if (StringUtils.isNotBlank(selectSql)) {
                    selectSql += " and ";
                }

                if (selfRefLevel == 0) {
                    selectSql += selfRefParentColumnName + " is null or " + selfRefParentColumnName + " = " + selfRefChildColumnName + " ";
                } else {
                    DatabaseInfo info = symmetricDialect.getPlatform().getDatabaseInfo();
                    String tableName = Table.getFullyQualifiedTableName(sourceTable.getCatalog(), sourceTable.getSchema(),
                            sourceTable.getName(), info.getDelimiterToken(), info.getCatalogSeparator(), info.getSchemaSeparator());                    
                    String refSql= "select " + selfRefChildColumnName + " from " + tableName + 
                            " where " + selfRefParentColumnName;
                    selectSql += selfRefParentColumnName + " in (";

                    for (int i = 1; i < selfRefLevel; i++) {
                        selectSql += refSql + " in (";
                    }
                    selectSql += refSql + " is null or " + selfRefChildColumnName + " = " + selfRefParentColumnName + " ) and " + 
                            selfRefParentColumnName + " != " + selfRefChildColumnName + StringUtils.repeat(")", selfRefLevel - 1);
                }
                log.info("Querying level {} for table {}: {}", selfRefLevel, sourceTable.getName(), selectSql);
            }
            
            String sql = symmetricDialect.createInitialLoadSqlFor(
                    this.currentInitialLoadEvent.getNode(), triggerRouter, sourceTable,
                    triggerHistory,
                    configurationService.getChannel(triggerRouter.getTrigger().getChannelId()),
                    selectSql);
            for (IReloadVariableFilter filter : extensionService.getExtensionPointList(IReloadVariableFilter.class)) {
                sql = filter.filterInitalLoadSql(sql, node, targetTable);
            }
            
            final String initialLoadSql = sql;
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

        public boolean requiresLobsSelectedFromSource(CsvData data) {
            if (this.currentInitialLoadEvent != null
                    && this.currentInitialLoadEvent.getTriggerRouter() != null) {
                if (this.currentInitialLoadEvent.getTriggerRouter().getTrigger().isUseStreamLobs()
                        || (data != null && hasLobsThatNeedExtract(sourceTable, data))) {
                    return true;
                }
                return this.currentInitialLoadEvent.getTriggerRouter().getTrigger().isUseStreamLobs();
            } else {
                return false;
            }
        }

        public void close() {
            closeCursor();
        }

    }
    
    @Override
    public void removeBatchFromStaging(OutgoingBatch batch) {
        IStagedResource resource = getStagedResource(batch);
        if (resource != null) {
            resource.delete();
        } else {
            log.info("Could not remove batch {} from staging because it did not exist", batch.getNodeBatchId());
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

    static class FutureExtractStatus {
        boolean shouldExtractSkip;
        int batchExtractCount;
        int byteExtractCount;
    }

    static class FutureOutgoingBatch {
        OutgoingBatch outgoingBatch;
        boolean isRetry;
        boolean isExtractSkipped;
        
        public FutureOutgoingBatch(OutgoingBatch outgoingBatch, boolean isRetry) {
            this.outgoingBatch = outgoingBatch;
            this.isRetry = isRetry;
        }

        public OutgoingBatch getOutgoingBatch() {
            return outgoingBatch;
        }

        public boolean isRetry() {
            return isRetry;
        }
    }
    
    static class BatchLock {
        public BatchLock(String semaphoreKey) {
            this.semaphoreKey = semaphoreKey;
        }
        
        public void acquire() throws InterruptedException {
            inMemoryLock.acquire();
        }
        
        public void release() {
            inMemoryLock.release();
        }
        
        String semaphoreKey;
        private Semaphore inMemoryLock = new Semaphore(1);
        StagingFileLock fileLock;
        int referenceCount = 0;
    }


}
