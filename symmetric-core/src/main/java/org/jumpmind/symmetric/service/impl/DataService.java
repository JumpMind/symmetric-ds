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

import java.sql.DataTruncation;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.UniqueKeyException;
import org.jumpmind.db.sql.mapper.NumberMapper;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.ext.IHeartbeatListener;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.reader.TableExtractDataReaderSource;
import org.jumpmind.symmetric.job.PushHeartbeatListener;
import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEvent;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.TableReloadRequest;
import org.jumpmind.symmetric.model.TableReloadRequestKey;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.IFileSyncService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.service.impl.TransformService.TransformTableNodeGroupLink;
import org.jumpmind.util.AppUtils;
import org.jumpmind.util.FormatUtils;

/**
 * @see IDataService
 */
public class DataService extends AbstractService implements IDataService {

    private ISymmetricEngine engine;

    private IExtensionService extensionService;

    private DataMapper dataMapper;

    public DataService(ISymmetricEngine engine, IExtensionService extensionService) {
        super(engine.getParameterService(), engine.getSymmetricDialect());
        this.engine = engine;
        this.dataMapper = new DataMapper();
        this.extensionService = extensionService;
        extensionService.addExtensionPoint(new PushHeartbeatListener(engine));
        setSqlMap(new DataServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    protected Map<IHeartbeatListener, Long> lastHeartbeatTimestamps = new HashMap<IHeartbeatListener, Long>();

    public boolean insertReloadEvent(TableReloadRequest request, boolean deleteAtClient) {
        boolean successful = false;
        if (request != null && request.isReloadEnabled()) {
            ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
            INodeService nodeService = engine.getNodeService();
            Node targetNode = nodeService.findNode(request.getTargetNodeId());
            if (targetNode != null) {
                TriggerRouter triggerRouter = triggerRouterService.getTriggerRouterForCurrentNode(
                        request.getTriggerId(), request.getRouterId(), false);
                if (triggerRouter != null) {
                    Trigger trigger = triggerRouter.getTrigger();
                    Router router = triggerRouter.getRouter();

                    NodeGroupLink link = router.getNodeGroupLink();
                    Node me = nodeService.findIdentity();
                    if (link.getSourceNodeGroupId().equals(me.getNodeGroupId())) {
                        if (link.getTargetNodeGroupId().equals(targetNode.getNodeGroupId())) {

                            TriggerHistory triggerHistory = lookupTriggerHistory(trigger);

                            ISqlTransaction transaction = null;
                            try {
                                transaction = sqlTemplate.startSqlTransaction();

                                if (parameterService
                                        .is(ParameterConstants.INITIAL_LOAD_DELETE_BEFORE_RELOAD)) {
                                    String overrideDeleteStatement = StringUtils.isNotBlank(request
                                            .getReloadDeleteStmt()) ? request.getReloadDeleteStmt()
                                            : null;
                                    insertPurgeEvent(transaction, targetNode, triggerRouter,
                                            triggerHistory, false, overrideDeleteStatement, -1,
                                            null);
                                }

                                insertReloadEvent(transaction, targetNode, triggerRouter,
                                        triggerHistory, request.getReloadSelect(), false, -1, null,
                                        Status.NE);

                                if (!targetNode.requires13Compatiblity() && deleteAtClient) {
                                    insertSqlEvent(
                                            transaction,
                                            triggerHistory,
                                            trigger.getChannelId(),
                                            targetNode,
                                            String.format(
                                                    "delete from %s where target_node_id='%s' and source_node_id='%s' and trigger_id='%s' and router_id='%s'",
                                                    TableConstants
                                                            .getTableName(
                                                                    tablePrefix,
                                                                    TableConstants.SYM_TABLE_RELOAD_REQUEST),
                                                    request.getTargetNodeId(), request
                                                            .getSourceNodeId(), request
                                                            .getTriggerId(), request.getRouterId()),
                                            false, -1, null);
                                }

                                deleteTableReloadRequest(transaction, request);

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

                        } else {
                            log.error(
                                    "Could not reload table for node {} because the router {} target node group id {} did not match",
                                    new Object[] { request.getTargetNodeId(),
                                            request.getRouterId(), link.getTargetNodeGroupId() });
                        }
                    } else {
                        log.error(
                                "Could not reload table for node {} because the router {} source node group id {} did not match",
                                new Object[] { request.getTargetNodeId(), request.getRouterId(),
                                        link.getSourceNodeGroupId() });
                    }
                } else {
                    log.error(
                            "Could not reload table for node {} because the trigger router ({}, {}) could not be found",
                            new Object[] { request.getTargetNodeId(), request.getTriggerId(),
                                    request.getRouterId() });
                }
            } else {
                log.error("Could not reload table for node {} because the node could not be found",
                        request.getTargetNodeId());
            }
        }
        return successful;

    }

    protected void deleteTableReloadRequest(ISqlTransaction sqlTransaction,
            TableReloadRequest request) {
        sqlTransaction.prepareAndExecute(
                getSql("deleteTableReloadRequest"),
                new Object[] { request.getSourceNodeId(), request.getTargetNodeId(),
                        request.getTriggerId(), request.getRouterId() }, new int[] { Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR });
    }

    public void saveTableReloadRequest(TableReloadRequest request) {
        Date time = new Date();
        request.setLastUpdateTime(time);
        if (0 == sqlTemplate.update(
                getSql("updateTableReloadRequest"),
                new Object[] { request.getReloadSelect(), request.getReloadDeleteStmt(),
                        request.isReloadEnabled() ? 1 : 0, request.getReloadTime(),
                        request.getCreateTime(), request.getLastUpdateBy(),
                        request.getLastUpdateTime(), request.getSourceNodeId(),
                        request.getTargetNodeId(), request.getTriggerId(), request.getRouterId() },
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.TIMESTAMP,
                        Types.TIMESTAMP, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR })) {
            request.setCreateTime(time);
            sqlTemplate.update(
                    getSql("insertTableReloadRequest"),
                    new Object[] { request.getReloadSelect(), request.getReloadDeleteStmt(),
                            request.isReloadEnabled() ? 1 : 0, request.getReloadTime(),
                            request.getCreateTime(), request.getLastUpdateBy(),
                            request.getLastUpdateTime(), request.getSourceNodeId(),
                            request.getTargetNodeId(), request.getTriggerId(),
                            request.getRouterId() }, new int[] { Types.VARCHAR, Types.VARCHAR,
                            Types.SMALLINT, Types.TIMESTAMP, Types.TIMESTAMP, Types.VARCHAR,
                            Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR });
        }
    }

    public TableReloadRequest getTableReloadRequest(final TableReloadRequestKey key) {
        return sqlTemplate.queryForObject(getSql("selectTableReloadRequest"),
                new ISqlRowMapper<TableReloadRequest>() {
                    public TableReloadRequest mapRow(Row rs) {
                        TableReloadRequest request = new TableReloadRequest(key);
                        request.setReloadSelect(rs.getString("reload_select"));
                        request.setReloadEnabled(rs.getBoolean("reload_enabled"));
                        request.setReloadTime(rs.getDateTime("reload_time"));
                        request.setReloadDeleteStmt(rs.getString("reload_delete_stmt"));
                        request.setCreateTime(rs.getDateTime("create_time"));
                        request.setLastUpdateBy(rs.getString("last_update_by"));
                        request.setLastUpdateTime(rs.getDateTime("last_update_time"));
                        return request;
                    }
                }, key.getSourceNodeId(), key.getTargetNodeId(), key.getTriggerId(),
                key.getRouterId());
    }

    /**
     * @return If isLoad then return the inserted batch id otherwise return the
     *         data id
     */
    public long insertReloadEvent(ISqlTransaction transaction, Node targetNode,
            TriggerRouter triggerRouter, TriggerHistory triggerHistory,
            String overrideInitialLoadSelect, boolean isLoad, long loadId, String createBy,
            Status status) {
        String channelId = getReloadChannelIdForTrigger(triggerRouter.getTrigger(), engine
                .getConfigurationService().getChannels(false));
        return insertReloadEvent(transaction, targetNode, triggerRouter, triggerHistory,
                overrideInitialLoadSelect, isLoad, loadId, createBy, status, channelId);
    }

    /**
     * @return If isLoad then return the inserted batch id otherwise return the
     *         data id
     */
    public long insertReloadEvent(ISqlTransaction transaction, Node targetNode,
            TriggerRouter triggerRouter, TriggerHistory triggerHistory,
            String overrideInitialLoadSelect, boolean isLoad, long loadId, String createBy,
            Status status, String channelId) {
        if (triggerHistory == null) {
            triggerHistory = lookupTriggerHistory(triggerRouter.getTrigger());
        }

        // initial_load_select for table can be overridden by populating the
        // row_data
        Data data = new Data(triggerHistory.getSourceTableName(), DataEventType.RELOAD,
                overrideInitialLoadSelect != null ? overrideInitialLoadSelect : triggerRouter
                        .getInitialLoadSelect(), null, triggerHistory, channelId,
                null, null);
        if (isLoad) {
            return insertDataAndDataEventAndOutgoingBatch(transaction, data,
                    targetNode.getNodeId(), triggerRouter.getRouter().getRouterId(), isLoad,
                    loadId, createBy, status);
        } else {
            data.setNodeList(targetNode.getNodeId());
            return insertData(transaction, data);
        }
    }

    private String getReloadChannelIdForTrigger(Trigger trigger, Map<String, Channel> channels) {
        String channelId = trigger != null ? trigger.getChannelId() : Constants.CHANNEL_DEFAULT;
        if (parameterService.is(ParameterConstants.INITIAL_LOAD_USE_RELOAD_CHANNEL)) {
            Channel normalChannel = channels.get(channelId);
            Channel reloadChannel = channels.get(trigger != null ? trigger.getReloadChannelId()
                    : Constants.CHANNEL_RELOAD);
            if (normalChannel.isFileSyncFlag()) {
                if (reloadChannel != null && reloadChannel.isFileSyncFlag()) {
                    channelId = reloadChannel.getChannelId();
                }
            } else {
                if (reloadChannel != null && reloadChannel.isReloadFlag()) {
                    channelId = reloadChannel.getChannelId();
                } else {
                    channelId = Constants.CHANNEL_RELOAD;
                }
            }
        }
        return channelId;
    }

    public void insertReloadEvents(Node targetNode, boolean reverse) {

        if (engine.getClusterService().lock(ClusterConstants.SYNCTRIGGERS)) {
            try {
                synchronized (engine.getTriggerRouterService()) {
                    engine.getClusterService().lock(ClusterConstants.SYNCTRIGGERS);

                    if (!reverse) {
                        log.info("Queueing up an initial load to " + targetNode.getNodeId());
                    } else {
                        log.info("Queueing up a reverse initial load to " + targetNode.getNodeId());
                    }
                    
                    /*
                     * Outgoing data events are pointless because we are
                     * reloading all data
                     */
                    engine.getOutgoingBatchService().markAllAsSentForNode(targetNode.getNodeId(),
                            false);

                    INodeService nodeService = engine.getNodeService();
                    ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();

                    Node sourceNode = nodeService.findIdentity();

                    boolean transactional = parameterService
                            .is(ParameterConstants.DATA_RELOAD_IS_BATCH_INSERT_TRANSACTIONAL);

                    String nodeIdRecord = reverse ? nodeService.findIdentityNodeId() : targetNode
                            .getNodeId();
                    NodeSecurity nodeSecurity = nodeService.findNodeSecurity(nodeIdRecord);

                    ISqlTransaction transaction = null;

                    try {

                        transaction = platform.getSqlTemplate().startSqlTransaction();

                        long loadId = engine.getSequenceService().nextVal(transaction,
                                Constants.SEQUENCE_OUTGOING_BATCH_LOAD_ID);
                        String createBy = reverse ? nodeSecurity.getRevInitialLoadCreateBy()
                                : nodeSecurity.getInitialLoadCreateBy();

                        List<TriggerHistory> triggerHistories = triggerRouterService
                                .getActiveTriggerHistories();

                        Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId = triggerRouterService
                                .fillTriggerRoutersByHistIdAndSortHist(sourceNode.getNodeGroupId(),
                                        targetNode.getNodeGroupId(), triggerHistories);

                        callReloadListeners(true, targetNode, transactional, transaction, loadId);

                        insertCreateSchemaScriptPriorToReload(targetNode, nodeIdRecord, loadId,
                                createBy, transactional, transaction);

                        insertSqlEventsPriorToReload(targetNode, nodeIdRecord, loadId, createBy,
                                transactional, transaction, reverse);

                        insertCreateBatchesForReload(targetNode, loadId, createBy,
                                triggerHistories, triggerRoutersByHistoryId, transactional,
                                transaction);

                        insertDeleteBatchesForReload(targetNode, loadId, createBy,
                                triggerHistories, triggerRoutersByHistoryId, transactional,
                                transaction);

                        insertLoadBatchesForReload(targetNode, loadId, createBy, triggerHistories,
                                triggerRoutersByHistoryId, transactional, transaction);

                        String afterSql = parameterService
                                .getString(reverse ? ParameterConstants.INITIAL_LOAD_REVERSE_AFTER_SQL
                                        : ParameterConstants.INITIAL_LOAD_AFTER_SQL);
                        if (isNotBlank(afterSql)) {
                            insertSqlEvent(transaction, targetNode, afterSql, true, loadId,
                                    createBy);
                        }

                        insertFileSyncBatchForReload(targetNode, loadId, createBy, transactional,
                                transaction);

                        callReloadListeners(false, targetNode, transactional, transaction, loadId);

                        if (!reverse) {
                            nodeService.setInitialLoadEnabled(transaction, nodeIdRecord, false,
                                    false, loadId, createBy);
                        } else {
                            nodeService.setReverseInitialLoadEnabled(transaction, nodeIdRecord,
                                    false, false, loadId, createBy);
                        }

                        if (!Constants.DEPLOYMENT_TYPE_REST.equals(targetNode.getDeploymentType())) {
                            insertNodeSecurityUpdate(transaction, nodeIdRecord,
                                    targetNode.getNodeId(), true, loadId, createBy);
                        }

                        engine.getStatisticManager().incrementNodesLoaded(1);

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

                    if (!reverse) {
                        /*
                         * Remove all incoming events for the node that we are
                         * starting a reload for
                         */
                        engine.getPurgeService().purgeAllIncomingEventsForNode(
                                targetNode.getNodeId());
                    }
                }
            } finally {
                engine.getClusterService().unlock(ClusterConstants.SYNCTRIGGERS);
            }
        } else {
            log.info("Not attempting to insert reload events because sync trigger is currently running");
        }

    }

    private void callReloadListeners(boolean before, Node targetNode, boolean transactional,
            ISqlTransaction transaction, long loadId) {
        for (IReloadListener listener : extensionService.getExtensionPointList(IReloadListener.class)) {
            if (before) {
                listener.beforeReload(transaction, targetNode, loadId);
            } else {
                listener.afterReload(transaction, targetNode, loadId);
            }

            if (!transactional) {
                transaction.commit();
            }
        }
    }
    
    private void insertCreateSchemaScriptPriorToReload(Node targetNode, String nodeIdRecord, long loadId,
            String createBy, boolean transactional, ISqlTransaction transaction) {
        String dumpCommand = parameterService.getString(ParameterConstants.INITIAL_LOAD_SCHEMA_DUMP_COMMAND);
        String loadCommand = parameterService.getString(ParameterConstants.INITIAL_LOAD_SCHEMA_LOAD_COMMAND);
        if (isNotBlank(dumpCommand) && isNotBlank(loadCommand)) {
            try {            
                log.info("Dumping schema using the following dump command: " + dumpCommand);
                
                ProcessBuilder pb = new ProcessBuilder(FormatUtils.splitOnSpacePreserveQuotedStrings(dumpCommand));
                pb.redirectErrorStream(true);
                Process process = pb.start();
                java.io.InputStream is = process.getInputStream();
                java.io.StringWriter ow = new java.io.StringWriter();
                IOUtils.copy(is, ow);
                String output = ow.toString();
                output = StringEscapeUtils.escapeJavaScript(output);
                
                String script = IOUtils.toString(getClass().getResourceAsStream("/load-schema-at-target.bsh"));
                script = script.replace("${data}", output);
                script = script.replace("${commands}", formatCommandForScript(loadCommand));
                
                if (process.waitFor() != 0) {
                    throw new IoException(output.toString());
                }
                log.info("Inserting script to load dump at client");
                engine.getDataService().insertScriptEvent(transaction, Constants.CHANNEL_RELOAD, targetNode,
                        script, true, loadId, "reload listener");
            } catch (Exception e) {
                throw new IoException(e);
            }
        }
    }
    
    private String formatCommandForScript(String command) {
        String[] tokens = FormatUtils.splitOnSpacePreserveQuotedStrings(command);
        StringBuilder builder = new StringBuilder();
        for (String string : tokens) {
            builder.append("\"" + StringEscapeUtils.escapeJava(string) + "\",");
        }
        return builder.substring(0, builder.length()-1);
    }

    private void insertSqlEventsPriorToReload(Node targetNode, String nodeIdRecord, long loadId,
            String createBy, boolean transactional, ISqlTransaction transaction, boolean reverse) {
        if (!Constants.DEPLOYMENT_TYPE_REST.equals(targetNode.getDeploymentType())) {
            /*
             * Insert node security so the client doing the initial load knows
             * that an initial load is currently happening
             */
            insertNodeSecurityUpdate(transaction, nodeIdRecord, targetNode.getNodeId(), true,
                    loadId, createBy);

            /*
             * Mark incoming batches as OK at the target node because we marked
             * outgoing batches as OK at the source
             */
            insertSqlEvent(
                    transaction,
                    targetNode,
                    String.format(
                            "update %s_incoming_batch set status='OK', error_flag=0 where node_id='%s' and status != 'OK'",
                            tablePrefix, engine.getNodeService().findIdentityNodeId()), true,
                    loadId, createBy);
        }
        
        String beforeSql = parameterService.getString(reverse ? ParameterConstants.INITIAL_LOAD_REVERSE_BEFORE_SQL
                : ParameterConstants.INITIAL_LOAD_BEFORE_SQL);
        if (isNotBlank(beforeSql)) {
            insertSqlEvent(
                    transaction,
                    targetNode,
                    beforeSql, true,
                    loadId, createBy);            
        }
    }

    private void insertCreateBatchesForReload(Node targetNode, long loadId, String createBy,
            List<TriggerHistory> triggerHistories,
            Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId, boolean transactional,
            ISqlTransaction transaction) {
        if (parameterService.is(ParameterConstants.INITIAL_LOAD_CREATE_SCHEMA_BEFORE_RELOAD)) {
            for (TriggerHistory triggerHistory : triggerHistories) {
                List<TriggerRouter> triggerRouters = triggerRoutersByHistoryId.get(triggerHistory
                        .getTriggerHistoryId());
                for (TriggerRouter triggerRouter : triggerRouters) {
                    if (triggerRouter.getInitialLoadOrder() >= 0
                            && engine.getGroupletService().isTargetEnabled(triggerRouter,
                                    targetNode)) {
                        insertCreateEvent(transaction, targetNode, triggerHistory, triggerRouter.getRouter().getRouterId(), true,
                                loadId, createBy);
                        if (!transactional) {
                            transaction.commit();
                        }
                    }
                }
            }
        }
    }

    private void insertDeleteBatchesForReload(Node targetNode, long loadId, String createBy,
            List<TriggerHistory> triggerHistories,
            Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId, boolean transactional,
            ISqlTransaction transaction) {
        if (parameterService.is(ParameterConstants.INITIAL_LOAD_DELETE_BEFORE_RELOAD)) {
            for (ListIterator<TriggerHistory> triggerHistoryIterator = triggerHistories
                    .listIterator(triggerHistories.size()); triggerHistoryIterator.hasPrevious();) {
                TriggerHistory triggerHistory = triggerHistoryIterator.previous();
                List<TriggerRouter> triggerRouters = triggerRoutersByHistoryId.get(triggerHistory
                        .getTriggerHistoryId());
                for (ListIterator<TriggerRouter> iterator = triggerRouters
                        .listIterator(triggerRouters.size()); iterator.hasPrevious();) {
                    TriggerRouter triggerRouter = iterator.previous();
                    if (triggerRouter.getInitialLoadOrder() >= 0
                            && engine.getGroupletService().isTargetEnabled(triggerRouter,
                                    targetNode)
                            && (!StringUtils.isBlank(parameterService
                                    .getString(ParameterConstants.INITIAL_LOAD_DELETE_FIRST_SQL)) || !StringUtils
                                    .isEmpty(triggerRouter.getInitialLoadDeleteStmt()))) {
                        insertPurgeEvent(transaction, targetNode, triggerRouter, triggerHistory,
                                true, null, loadId, createBy);
                        if (!transactional) {
                            transaction.commit();
                        }
                    }
                }
            }
        }
    }

    private void insertLoadBatchesForReload(Node targetNode, long loadId, String createBy,
            List<TriggerHistory> triggerHistories,
            Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId, boolean transactional,
            ISqlTransaction transaction) {
        Map<String, Channel> channels = engine.getConfigurationService().getChannels(false);
        DatabaseInfo dbInfo = platform.getDatabaseInfo();
        String quote = dbInfo.getDelimiterToken();
        String catalogSeparator = dbInfo.getCatalogSeparator();
        String schemaSeparator = dbInfo.getSchemaSeparator();
        for (TriggerHistory triggerHistory : triggerHistories) {
            List<TriggerRouter> triggerRouters = triggerRoutersByHistoryId.get(triggerHistory
                    .getTriggerHistoryId());
            for (TriggerRouter triggerRouter : triggerRouters) {
                if (triggerRouter.getInitialLoadOrder() >= 0
                        && engine.getGroupletService().isTargetEnabled(triggerRouter, targetNode)) {
                    if (parameterService.is(ParameterConstants.INITIAL_LOAD_USE_EXTRACT_JOB)) {
                        Trigger trigger = triggerRouter.getTrigger();
                        String reloadChannel = getReloadChannelIdForTrigger(trigger, channels);
                        Channel channel = channels.get(reloadChannel);
                        // calculate the number of batches needed for table.
                        int numberOfBatches = triggerRouter.getInitialLoadBatchCount();
                        if (numberOfBatches <= 0) {                            
                            Table table = platform.getTableFromCache(
                                    triggerHistory.getSourceCatalogName(), triggerHistory.getSourceSchemaName(),
                                    triggerHistory.getSourceTableName(), false);                            
                            String sql = String.format("select count(*) from %s where %s", table
                                    .getQualifiedTableName(quote, catalogSeparator, schemaSeparator),
                                    StringUtils.isBlank(triggerRouter.getInitialLoadSelect()) ? Constants.ALWAYS_TRUE_CONDITION
                                            : triggerRouter.getInitialLoadSelect());
                            sql = FormatUtils.replace("groupId", targetNode.getNodeGroupId(), sql);
                            sql = FormatUtils
                                    .replace("externalId", targetNode.getExternalId(), sql);
                            sql = FormatUtils.replace("nodeId", targetNode.getNodeId(), sql);
                            int rowCount = sqlTemplate.queryForInt(sql);
                            int transformMultiplier = 0;
                            for (TransformService.TransformTableNodeGroupLink transform : engine.getTransformService().getTransformTables(false)) {
                            	if (triggerRouter.getRouter().getNodeGroupLink().equals(transform.getNodeGroupLink()) && 
                            			transform.getSourceTableName().equals(table.getName())) {
                            		transformMultiplier++;
                            	}
                            }
                            if (transformMultiplier == 0) { transformMultiplier = 1; }
                            
                            if (rowCount > 0) {
                                numberOfBatches = (rowCount * transformMultiplier / channel.getMaxBatchSize()) + 1;
                            } else {
                                numberOfBatches = 1;
                            }
                        }

                        long startBatchId = -1;
                        long endBatchId = -1;
                        for (int i = 0; i < numberOfBatches; i++) {
                            // needs to grab the start and end batch id
                            endBatchId = insertReloadEvent(transaction, targetNode, triggerRouter,
                                    triggerHistory, null, true, loadId, createBy, Status.RQ);
                            if (startBatchId == -1) {
                                startBatchId = endBatchId;
                            }

                        }
                        engine.getDataExtractorService().requestExtractRequest(transaction,
                                targetNode.getNodeId(), triggerRouter, startBatchId, endBatchId);
                    } else {
                        insertReloadEvent(transaction, targetNode, triggerRouter, triggerHistory,
                                null, true, loadId, createBy, Status.NE);
                    }

                    if (!transactional) {
                        transaction.commit();
                    }
                }
            }
        }
    }

    private void insertFileSyncBatchForReload(Node targetNode, long loadId, String createBy,
            boolean transactional, ISqlTransaction transaction) {
        if (parameterService.is(ParameterConstants.FILE_SYNC_ENABLE)
                && !Constants.DEPLOYMENT_TYPE_REST.equals(targetNode.getDeploymentType())) {
            ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
            IFileSyncService fileSyncService = engine.getFileSyncService();
            if (fileSyncService.getFileTriggerRoutersForCurrentNode().size() > 0) {
                TriggerHistory fileSyncSnapshotHistory = triggerRouterService.findTriggerHistory(
                        null, null,
                        TableConstants.getTableName(tablePrefix, TableConstants.SYM_FILE_SNAPSHOT));
                String routerid = triggerRouterService.buildSymmetricTableRouterId(
                        fileSyncSnapshotHistory.getTriggerId(), parameterService.getNodeGroupId(),
                        targetNode.getNodeGroupId());
                TriggerRouter fileSyncSnapshotTriggerRouter = triggerRouterService
                        .getTriggerRouterForCurrentNode(fileSyncSnapshotHistory.getTriggerId(),
                                routerid, true);

                List<Channel> channels = engine.getConfigurationService().getFileSyncChannels();
                for (Channel channel : channels) {
                    if (channel.isReloadFlag()) {
                        insertReloadEvent(transaction, targetNode, fileSyncSnapshotTriggerRouter,
                                fileSyncSnapshotHistory,
                                "reload_channel_id='" + channel.getChannelId() + "'", true, loadId,
                                createBy, Status.NE, channel.getChannelId());
                        if (!transactional) {
                            transaction.commit();
                        }
                    }
                }
            }
        }
    }

    private TriggerHistory lookupTriggerHistory(Trigger trigger) {
        TriggerHistory history = engine.getTriggerRouterService()
                .getNewestTriggerHistoryForTrigger(trigger.getTriggerId(),
                        trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                        trigger.getSourceTableName());

        if (history == null) {
            engine.getTriggerRouterService().syncTriggers();
            history = engine.getTriggerRouterService().getNewestTriggerHistoryForTrigger(
                    trigger.getTriggerId(), null, null, null);
        }

        if (history == null) {
            throw new RuntimeException("Cannot find history for trigger " + trigger.getTriggerId()
                    + ", " + trigger.getSourceTableName());
        }
        return history;
    }

    protected void insertPurgeEvent(ISqlTransaction transaction, Node targetNode,
            TriggerRouter triggerRouter, TriggerHistory triggerHistory, boolean isLoad,
            String overrideDeleteStatement, long loadId, String createBy) {

        Node sourceNode = engine.getNodeService().findIdentity();
        
        List<TransformTableNodeGroupLink> transforms = 
                this.engine.getTransformService().findTransformsFor(
                        sourceNode.getNodeGroupId(), targetNode.getNodeGroupId(), triggerRouter.getTargetTable(triggerHistory));
        
        String sql = StringUtils.isNotBlank(overrideDeleteStatement) ? overrideDeleteStatement
                : symmetricDialect.createPurgeSqlFor(targetNode, triggerRouter, triggerHistory, transforms);
        sql = FormatUtils.replace("groupId", targetNode.getNodeGroupId(), sql);
        sql = FormatUtils.replace("externalId", targetNode.getExternalId(), sql);
        sql = FormatUtils.replace("nodeId", targetNode.getNodeId(), sql);
        sql = FormatUtils.replace("targetGroupId", targetNode.getNodeGroupId(), sql);
        sql = FormatUtils.replace("targetExternalId", targetNode.getExternalId(), sql);
        sql = FormatUtils.replace("targetNodeId", targetNode.getNodeId(), sql);
        sql = FormatUtils.replace("sourceGroupId", sourceNode.getNodeGroupId(), sql);
        sql = FormatUtils.replace("sourceExternalId", sourceNode.getExternalId(), sql);
        sql = FormatUtils.replace("sourceNodeId", sourceNode.getNodeId(), sql);
        String channelId = getReloadChannelIdForTrigger(triggerRouter.getTrigger(), engine
                .getConfigurationService().getChannels(false));
        Data data = new Data(triggerHistory.getSourceTableName(), DataEventType.SQL,
                CsvUtils.escapeCsvData(sql), null, triggerHistory, channelId, null, null);
        if (isLoad) {
            insertDataAndDataEventAndOutgoingBatch(transaction, data, targetNode.getNodeId(),
                    triggerRouter.getRouter().getRouterId(), isLoad, loadId, createBy, Status.NE);
        } else {
            data.setNodeList(targetNode.getNodeId());
            insertData(transaction, data);
        }
    }

    public void insertSqlEvent(Node targetNode, String sql, boolean isLoad, long loadId,
            String createBy) {
        TriggerHistory history = engine.getTriggerRouterService()
                .findTriggerHistoryForGenericSync();
        Trigger trigger = engine.getTriggerRouterService().getTriggerById(history.getTriggerId(),
                false);
        String reloadChannelId = getReloadChannelIdForTrigger(trigger, engine
                .getConfigurationService().getChannels(false));

        Data data = new Data(history.getSourceTableName(), DataEventType.SQL,
                CsvUtils.escapeCsvData(sql), null, history, isLoad ? reloadChannelId
                        : Constants.CHANNEL_CONFIG, null, null);
        if (isLoad) {
            insertDataAndDataEventAndOutgoingBatch(data, targetNode.getNodeId(),
                    Constants.UNKNOWN_ROUTER_ID, isLoad, loadId, createBy);
        } else {
            data.setNodeList(targetNode.getNodeId());
            insertData(data);
        }
    }

    public void insertSqlEvent(ISqlTransaction transaction, Node targetNode, String sql,
            boolean isLoad, long loadId, String createBy) {
        TriggerHistory history = engine.getTriggerRouterService()
                .findTriggerHistoryForGenericSync();
        insertSqlEvent(transaction, history, Constants.CHANNEL_CONFIG, targetNode, sql, isLoad,
                loadId, createBy);
    }

    protected void insertSqlEvent(ISqlTransaction transaction, TriggerHistory history,
            String channelId, Node targetNode, String sql, boolean isLoad, long loadId,
            String createBy) {
        Trigger trigger = engine.getTriggerRouterService().getTriggerById(history.getTriggerId(),
                false);
        String reloadChannelId = getReloadChannelIdForTrigger(trigger, engine
                .getConfigurationService().getChannels(false));
        Data data = new Data(history.getSourceTableName(), DataEventType.SQL,
                CsvUtils.escapeCsvData(sql), null, history, isLoad ? reloadChannelId : channelId,
                null, null);
        if (isLoad) {
            insertDataAndDataEventAndOutgoingBatch(transaction, data, targetNode.getNodeId(),
                    Constants.UNKNOWN_ROUTER_ID, isLoad, loadId, createBy, Status.NE);
        } else {
            data.setNodeList(targetNode.getNodeId());
            insertData(transaction, data);
        }
    }

    public void insertScriptEvent(ISqlTransaction transaction, String channelId,
            Node targetNode, String script, boolean isLoad, long loadId, String createBy) {
        TriggerHistory history = engine.getTriggerRouterService()
                .findTriggerHistoryForGenericSync();
        Trigger trigger = engine.getTriggerRouterService().getTriggerById(history.getTriggerId(),
                false);
        String reloadChannelId = getReloadChannelIdForTrigger(trigger, engine
                .getConfigurationService().getChannels(false));
        Data data = new Data(history.getSourceTableName(), DataEventType.BSH,
                CsvUtils.escapeCsvData(script), null, history,
                isLoad ? reloadChannelId : channelId, null, null);
        if (isLoad) {
            insertDataAndDataEventAndOutgoingBatch(transaction, data, targetNode.getNodeId(),
                    Constants.UNKNOWN_ROUTER_ID, isLoad, loadId, createBy, Status.NE);
        } else {
            data.setNodeList(targetNode.getNodeId());
            insertData(transaction, data);
        }
    }

    public int countDataInRange(long firstDataId, long secondDataId) {
        return sqlTemplate.queryForInt(getSql("countDataInRangeSql"), firstDataId, secondDataId);
    }

    public void checkForAndUpdateMissingChannelIds(long firstDataId, long lastDataId) {
        int numberUpdated = sqlTemplate.update(getSql("checkForAndUpdateMissingChannelIdSql"),
                Constants.CHANNEL_DEFAULT, firstDataId, lastDataId);
        if (numberUpdated > 0) {
            log.warn(
                    "There were {} data records found between {} and {} that an invalid channel_id.  Updating them to be on the '{}' channel.",
                    new Object[] { numberUpdated, firstDataId, lastDataId,
                            Constants.CHANNEL_DEFAULT });
        }
    }

    public void insertCreateEvent(final Node targetNode, TriggerHistory triggerHistory, String routerId,
            boolean isLoad, long loadId, String createBy) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            insertCreateEvent(transaction, targetNode, triggerHistory, routerId, isLoad, loadId,
                    createBy);
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

    public void insertCreateEvent(ISqlTransaction transaction, Node targetNode,
            TriggerHistory triggerHistory, String routerId, boolean isLoad, long loadId, String createBy) {

        Trigger trigger = engine.getTriggerRouterService().getTriggerById(
                triggerHistory.getTriggerId(), false);
        String reloadChannelId = getReloadChannelIdForTrigger(trigger, engine
                .getConfigurationService().getChannels(false));

        Data data = new Data(triggerHistory.getSourceTableName(), DataEventType.CREATE,
                null, null, triggerHistory, isLoad ? reloadChannelId
                        : Constants.CHANNEL_CONFIG, null, null);
        try {
            if (isLoad) {
                insertDataAndDataEventAndOutgoingBatch(transaction, data, targetNode.getNodeId(),
                        routerId, isLoad, loadId, createBy, Status.NE);
            } else {
                data.setNodeList(targetNode.getNodeId());
                insertData(transaction, data);
            }
        } catch (UniqueKeyException e) {
            if (e.getRootCause() != null && e.getRootCause() instanceof DataTruncation) {
                log.error("Table data definition XML was too large and failed.  The feature to send table creates during the initial load may be limited on your platform.  You may need to set the initial.load.create.first parameter to false.");
            }
            throw e;
        }
    }

    public long insertData(Data data) {
        ISqlTransaction transaction = null;
        long dataId = -1;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            dataId = insertData(transaction, data);
            transaction.commit();
            return dataId;
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

    protected long insertData(ISqlTransaction transaction, final Data data) {
        long id = transaction.insertWithGeneratedKey(
                getSql("insertIntoDataSql"),
                symmetricDialect.getSequenceKeyName(SequenceIdentifier.DATA),
                symmetricDialect.getSequenceName(SequenceIdentifier.DATA),
                new Object[] {
                        data.getTableName(),
                        data.getDataEventType().getCode(),
                        data.getRowData(),
                        data.getPkData(),
                        data.getOldData(),
                        data.getTriggerHistory() != null ? data.getTriggerHistory()
                                .getTriggerHistoryId() : -1, data.getChannelId(),
                        data.getExternalData(), data.getNodeList() }, new int[] { Types.VARCHAR,
                        Types.CHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.NUMERIC,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR });
        data.setDataId(id);
        return id;
    }

    protected void insertDataEvent(ISqlTransaction transaction, DataEvent dataEvent) {
        this.insertDataEvent(transaction, dataEvent.getDataId(), dataEvent.getBatchId(),
                dataEvent.getRouterId());
    }

    protected void insertDataEvent(ISqlTransaction transaction, long dataId, long batchId,
            String routerId) {
        try {
            transaction
                    .prepareAndExecute(getSql("insertIntoDataEventSql"),
                            new Object[] {
                                    dataId,
                                    batchId,
                                    StringUtils.isBlank(routerId) ? Constants.UNKNOWN_ROUTER_ID
                                            : routerId }, new int[] { Types.NUMERIC, Types.NUMERIC,
                                    Types.VARCHAR });
        } catch (RuntimeException ex) {
            log.error("Could not insert a data event: data_id={} batch_id={} router_id={}",
                    new Object[] { dataId, batchId, routerId });
            log.error("", ex);
            throw ex;
        }
    }

    public void insertDataEvents(ISqlTransaction transaction, final List<DataEvent> events) {
        if (events.size() > 0) {
            transaction.prepare(getSql("insertIntoDataEventSql"));
            for (DataEvent dataEvent : events) {
                String routerId = dataEvent.getRouterId();
                transaction.addRow(
                        dataEvent,
                        new Object[] {
                                dataEvent.getDataId(),
                                dataEvent.getBatchId(),
                                StringUtils.isBlank(routerId) ? Constants.UNKNOWN_ROUTER_ID
                                        : routerId }, new int[] { Types.NUMERIC, Types.NUMERIC,
                                Types.VARCHAR });
            }
            transaction.flush();
        }
    }

    public void insertDataAndDataEventAndOutgoingBatch(Data data, String channelId,
            List<Node> nodes, String routerId, boolean isLoad, long loadId, String createBy) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            long dataId = insertData(transaction, data);
            for (Node node : nodes) {
                insertDataEventAndOutgoingBatch(transaction, dataId, channelId, node.getNodeId(),
                        data.getDataEventType(), routerId, isLoad, loadId, createBy, Status.NE);
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
    }

    /**
     * @return The inserted batch id
     */
    public long insertDataAndDataEventAndOutgoingBatch(Data data, String nodeId, String routerId,
            boolean isLoad, long loadId, String createBy) {
        long batchId = 0;
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            batchId = insertDataAndDataEventAndOutgoingBatch(transaction, data, nodeId, routerId,
                    isLoad, loadId, createBy, Status.NE);
            transaction.commit();
            return batchId;
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

    /**
     * @return The inserted batch id
     */
    public long insertDataAndDataEventAndOutgoingBatch(ISqlTransaction transaction, Data data,
            String nodeId, String routerId, boolean isLoad, long loadId, String createBy,
            Status status) {
        long dataId = insertData(transaction, data);
        String channelId = null;
        if (isLoad) {
            TriggerHistory history = data.getTriggerHistory();
            if (history != null && channelId == null) {
                Trigger trigger = engine.getTriggerRouterService().getTriggerById(
                        history.getTriggerId());
                channelId = getReloadChannelIdForTrigger(trigger, engine.getConfigurationService()
                        .getChannels(false));
            }
        } else {
            channelId = data.getChannelId();
        }
        return insertDataEventAndOutgoingBatch(transaction, dataId, channelId, nodeId,
                data.getDataEventType(), routerId, isLoad, loadId, createBy, status);
    }

    /**
     * @param status
     *            TODO
     * @return The inserted batch id
     */
    protected long insertDataEventAndOutgoingBatch(ISqlTransaction transaction, long dataId,
            String channelId, String nodeId, DataEventType eventType, String routerId,
            boolean isLoad, long loadId, String createBy, Status status) {
        OutgoingBatch outgoingBatch = new OutgoingBatch(nodeId, channelId, status);
        outgoingBatch.setLoadId(loadId);
        outgoingBatch.setCreateBy(createBy);
        outgoingBatch.setLoadFlag(isLoad);
        outgoingBatch.incrementEventCount(eventType);
        if (status == Status.RQ) {
            outgoingBatch.setExtractJobFlag(true);
        }
        engine.getOutgoingBatchService().insertOutgoingBatch(transaction, outgoingBatch);
        insertDataEvent(transaction, new DataEvent(dataId, outgoingBatch.getBatchId(), routerId));
        return outgoingBatch.getBatchId();
    }

    public String reloadNode(String nodeId, boolean reverseLoad, String createBy) {
        INodeService nodeService = engine.getNodeService();
        Node targetNode = engine.getNodeService().findNode(nodeId);
        if (targetNode == null) {
            return String.format("Unknown node %s", nodeId);
        } else if (reverseLoad
                && nodeService.setReverseInitialLoadEnabled(nodeId, true, true, -1, createBy)) {
            return String.format("Successfully enabled reverse initial load for node %s", nodeId);
        } else if (nodeService.setInitialLoadEnabled(nodeId, true, true, -1, createBy)) {
            return String.format("Successfully enabled initial load for node %s", nodeId);
        } else {
            return String.format("Could not enable initial load for %s", nodeId);
        }
    }

    private void insertNodeSecurityUpdate(ISqlTransaction transaction, String nodeIdRecord,
            String targetNodeId, boolean isLoad, long loadId, String createBy) {
        Data data = createData(transaction, null, null, tablePrefix + "_node_security",
                " t.node_id = '" + nodeIdRecord + "'");
        if (data != null) {
            insertDataAndDataEventAndOutgoingBatch(transaction, data, targetNodeId,
                    Constants.UNKNOWN_ROUTER_ID, isLoad, loadId, createBy, Status.NE);
        }
    }

    public void sendScript(String nodeId, String script, boolean isLoad) {
        Node targetNode = engine.getNodeService().findNode(nodeId);
        TriggerHistory history = engine.getTriggerRouterService()
                .findTriggerHistoryForGenericSync();
        Data data = new Data(history.getSourceTableName(), DataEventType.BSH,
                CsvUtils.escapeCsvData(script), null, history, Constants.CHANNEL_CONFIG, null, null);
        data.setNodeList(nodeId);
        if (!isLoad) {
            insertData(data);
        } else {
            insertDataAndDataEventAndOutgoingBatch(data, targetNode.getNodeId(),
                    Constants.UNKNOWN_ROUTER_ID, isLoad, -1, null);
        }
    }

    public boolean sendSchema(String nodeId, String catalogName, String schemaName,
            String tableName, boolean isLoad) {
        Node sourceNode = engine.getNodeService().findIdentity();
        Node targetNode = engine.getNodeService().findNode(nodeId);
        if (targetNode == null) {
            log.error("Could not send schema to the node {}.  It does not exist", nodeId);
            return false;
        }

        ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
        List<TriggerHistory> triggerHistories = triggerRouterService.findTriggerHistories(
                catalogName, schemaName, tableName);
        Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId = triggerRouterService
                .fillTriggerRoutersByHistIdAndSortHist(sourceNode.getNodeGroupId(),
                        targetNode.getNodeGroupId(), triggerHistories);
        int eventCount = 0;
        for (TriggerHistory triggerHistory : triggerHistories) {
            List<TriggerRouter> triggerRouters = triggerRoutersByHistoryId.get(triggerHistory
                    .getTriggerHistoryId());
            for (TriggerRouter triggerRouter : triggerRouters) {
                eventCount++;
                insertCreateEvent(targetNode, triggerHistory, triggerRouter.getRouter().getRouterId(), false, -1, null);
            }
        }

        if (eventCount > 0) {
            return true;
        } else {
            return false;
        }
    }

    public String sendSQL(String nodeId, String catalogName, String schemaName, String tableName,
            String sql) {
        Node sourceNode = engine.getNodeService().findIdentity();
        Node targetNode = engine.getNodeService().findNode(nodeId);
        if (targetNode == null) {
            return "Unknown node " + nodeId;
        }

        ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
        TriggerHistory triggerHistory = triggerRouterService.findTriggerHistory(catalogName,
                schemaName, tableName);

        if (triggerHistory == null) {
            return "Trigger for table " + tableName + " does not exist from node "
                    + sourceNode.getNodeGroupId();
        } else {
            Trigger trigger = triggerRouterService.getTriggerById(triggerHistory.getTriggerId());
            if (trigger != null) {
                ISqlTransaction transaction = null;
                try {
                    transaction = sqlTemplate.startSqlTransaction();

                    insertSqlEvent(transaction, triggerHistory, trigger.getChannelId(), targetNode,
                            sql, false, -1, null);
                    transaction.commit();
                    return "Successfully create SQL event for node " + targetNode.getNodeId();
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
            } else {
                return "Trigger for table " + tableName + " does not exist from node "
                        + sourceNode.getNodeGroupId();
            }
        }
    }

    public String reloadTable(String nodeId, String catalogName, String schemaName, String tableName) {
        return reloadTable(nodeId, catalogName, schemaName, tableName, null);
    }

    public String reloadTable(String nodeId, String catalogName, String schemaName,
            String tableName, String overrideInitialLoadSelect) {
        Node sourceNode = engine.getNodeService().findIdentity();
        Node targetNode = engine.getNodeService().findNode(nodeId);
        if (targetNode == null) {
            return "Unknown node " + nodeId;
        }

        ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
        List<TriggerHistory> triggerHistories = triggerRouterService.findTriggerHistories(
                catalogName, schemaName, tableName);
        Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId = triggerRouterService
                .fillTriggerRoutersByHistIdAndSortHist(sourceNode.getNodeGroupId(),
                        targetNode.getNodeGroupId(), triggerHistories);
        int eventCount = 0;
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();

            for (TriggerHistory triggerHistory : triggerHistories) {
                List<TriggerRouter> triggerRouters = triggerRoutersByHistoryId.get(triggerHistory
                        .getTriggerHistoryId());
                if (triggerRouters != null && triggerRouters.size() > 0) {
                    for (TriggerRouter triggerRouter : triggerRouters) {
                        eventCount++;
                        insertReloadEvent(transaction, targetNode, triggerRouter, triggerHistory,
                                overrideInitialLoadSelect, false, -1, "reloadTable", Status.NE);
                    }
                }
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

        if (eventCount > 0) {
            return "Successfully created " + (eventCount > 1 ? eventCount + " events" : "event")
                    + " to reload table " + tableName + " for node "

                    + targetNode.getNodeId();
        } else {
            return "Trigger for table " + tableName + " does not exist for source node group of "
                    + sourceNode.getNodeGroupId();
        }

    }

    /**
     * Because we can't add a trigger on the _node table, we are artificially
     * generating heartbeat events.
     * 
     * @param node
     */
    public void insertHeartbeatEvent(Node node, boolean isReload) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            String tableName = TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE_HOST);
            List<NodeGroupLink> links = engine.getConfigurationService().getNodeGroupLinksFor(
                    parameterService.getNodeGroupId(), false);
            for (NodeGroupLink nodeGroupLink : links) {
                if (nodeGroupLink.getDataEventAction() == NodeGroupLinkAction.P) {
                    Set<TriggerRouter> triggerRouters = engine.getTriggerRouterService()
                            .getTriggerRouterForTableForCurrentNode(nodeGroupLink, null, null,
                                    tableName, false);
                    if (triggerRouters != null && triggerRouters.size() > 0) {
                        Data data = createData(transaction, triggerRouters.iterator().next()
                                .getTrigger(), String.format(" t.node_id = '%s'", node.getNodeId()));
                        if (data != null) {
                            insertData(transaction, data);
                        } else {
                            log.warn("Not generating data/data events for table {} "
                                    + "because a trigger or trigger hist is not created yet.",
                                    tableName);
                        }
                    } else {
                        log.warn("Not generating data/data events for table {} "
                                + "because a trigger or trigger hist is not created yet.",
                                tableName);
                    }
                }
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

    }

    public Data createData(String catalogName, String schemaName, String tableName) {
        return createData(catalogName, schemaName, tableName, null);
    }

    public Data createData(String catalogName, String schemaName, String tableName,
            String whereClause) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            Data data = createData(transaction, catalogName, schemaName, tableName, whereClause);
            transaction.commit();
            return data;
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

    public Data createData(ISqlTransaction transaction, String catalogName, String schemaName,
            String tableName, String whereClause) {
        Data data = null;
        Set<TriggerRouter> triggerRouters = engine.getTriggerRouterService()
                .getTriggerRouterForTableForCurrentNode(catalogName, schemaName, tableName, false);
        if (triggerRouters != null && triggerRouters.size() > 0) {
            data = createData(transaction, triggerRouters.iterator().next().getTrigger(),
                    whereClause);
        }
        return data;
    }

    protected Data createData(ISqlTransaction transaction, Trigger trigger, String whereClause) {
        Data data = null;
        if (trigger != null) {
            TriggerHistory triggerHistory = engine.getTriggerRouterService()
                    .getNewestTriggerHistoryForTrigger(trigger.getTriggerId(),
                            trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                            trigger.getSourceTableName());
            if (triggerHistory == null) {
                triggerHistory = engine.getTriggerRouterService().findTriggerHistory(
                        trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                        trigger.getSourceTableName());
                if (triggerHistory == null) {
                    triggerHistory = engine.getTriggerRouterService().findTriggerHistory(
                            trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                            trigger.getSourceTableName().toUpperCase());
                }
            }
            if (triggerHistory != null) {
                String rowData = null;
                String pkData = null;
                if (whereClause != null) {
                    rowData = (String) transaction.queryForObject(symmetricDialect
                            .createCsvDataSql(trigger, triggerHistory, engine
                                    .getConfigurationService().getChannel(trigger.getChannelId()),
                                    whereClause), String.class);
                    if (rowData != null) {
                        rowData = rowData.trim();
                    }
                    pkData = (String) transaction.queryForObject(symmetricDialect
                            .createCsvPrimaryKeySql(trigger, triggerHistory, engine
                                    .getConfigurationService().getChannel(trigger.getChannelId()),
                                    whereClause), String.class);
                    if (pkData != null) {
                        pkData = pkData.trim();
                    }
                }
                data = new Data(trigger.getSourceTableName(), DataEventType.UPDATE, rowData,
                        pkData, triggerHistory, trigger.getChannelId(), null, null);
            }
        }
        return data;
    }

    public List<DataGap> findDataGapsByStatus(DataGap.Status status) {
        return sqlTemplate.query(getSql("findDataGapsByStatusSql"), new ISqlRowMapper<DataGap>() {
            public DataGap mapRow(Row rs) {
                return new DataGap(rs.getLong("start_id"), rs.getLong("end_id"), rs
                        .getDateTime("create_time"));
            }
        }, status.name());
    }

    public List<DataGap> findDataGaps() {
        final long maxDataToSelect = parameterService
                .getLong(ParameterConstants.ROUTING_LARGEST_GAP_SIZE);
        List<DataGap> gaps = findDataGapsByStatus(DataGap.Status.GP);
        boolean lastGapExists = false;
        long maxDataEventId = 0;
        for (DataGap dataGap : gaps) {
            lastGapExists |= dataGap.gapSize() >= maxDataToSelect - 1;
            maxDataEventId = maxDataEventId < dataGap.getEndId() ? dataGap.getEndId() : maxDataEventId;
        }

        if (!lastGapExists) {
            maxDataEventId = maxDataEventId == 0 ? findMaxDataEventDataId() : maxDataEventId;
            if (maxDataEventId > 0) {
                maxDataEventId++;
            }
            DataGap gap = new DataGap(maxDataEventId, maxDataEventId + maxDataToSelect);
            insertDataGap(gap);
            log.info("Inserting last data gap: {}", gap);
            gaps = findDataGaps();
        }
        return gaps;

    }

    public long findMaxDataEventDataId() {
        return sqlTemplate.queryForLong(getSql("selectMaxDataEventDataIdSql"));
    }
    
    public void insertDataGap(DataGap gap) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            insertDataGap(transaction, gap);
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

    public void insertDataGap(ISqlTransaction transaction, DataGap gap) {
        transaction.prepareAndExecute(getSql("insertDataGapSql"),
                new Object[] { DataGap.Status.GP.name(), AppUtils.getHostName(), gap.getStartId(), gap.getEndId() }, new int[] {
                        Types.VARCHAR, Types.VARCHAR, Types.NUMERIC, Types.NUMERIC });
    }

    public void updateDataGap(DataGap gap, DataGap.Status status) {
        sqlTemplate.update(
                getSql("updateDataGapSql"),
                new Object[] { status.name(), AppUtils.getHostName(), gap.getStartId(),
                        gap.getEndId() }, new int[] { Types.VARCHAR, Types.VARCHAR,
                        symmetricDialect.getSqlTypeForIds(), symmetricDialect.getSqlTypeForIds() });
    }

    
    @Override
    public void deleteDataGap(DataGap gap) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            deleteDataGap(transaction, gap);
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
    
    @Override
    public void deleteDataGap(ISqlTransaction transaction, DataGap gap) {
        transaction.prepareAndExecute(
                getSql("deleteDataGapSql"),
                new Object[] { gap.getStartId(), gap.getEndId() },
                new int[] { symmetricDialect.getSqlTypeForIds(),
                        symmetricDialect.getSqlTypeForIds() });
    }

    public Date findCreateTimeOfEvent(long dataId) {
        return sqlTemplate.queryForObject(getSql("findDataEventCreateTimeSql"), Date.class, dataId);
    }

    public Date findCreateTimeOfData(long dataId) {
        return sqlTemplate.queryForObject(getSql("findDataCreateTimeSql"), Date.class, dataId);
    }
    
    public Date findNextCreateTimeOfDataStartingAt(long dataId) {
        return findCreateTimeOfData(sqlTemplate.queryForObject(getSql("findMinDataSql"), Long.class, dataId));
    }

    /**
     * Get a list of {@link IHeartbeatListener}s that are ready for a heartbeat
     * according to
     * {@link IHeartbeatListener#getTimeBetweenHeartbeatsInSeconds()}
     * 
     * @param force
     *            if true, then return the entire list of
     *            {@link IHeartbeatListener}s
     */
    protected List<IHeartbeatListener> getHeartbeatListeners(boolean force) {
        if (force) {
            return extensionService.getExtensionPointList(IHeartbeatListener.class);
        } else {
            List<IHeartbeatListener> listeners = new ArrayList<IHeartbeatListener>();
            if (listeners != null) {
                long ts = System.currentTimeMillis();
                for (IHeartbeatListener iHeartbeatListener : extensionService.getExtensionPointList(IHeartbeatListener.class)) {
                    Long lastHeartbeatTimestamp = lastHeartbeatTimestamps.get(iHeartbeatListener);
                    if (lastHeartbeatTimestamp == null
                            || lastHeartbeatTimestamp <= ts
                                    - (iHeartbeatListener.getTimeBetweenHeartbeatsInSeconds() * 1000)) {
                        listeners.add(iHeartbeatListener);
                    }
                }
            }
            return listeners;
        }
    }

    protected void updateLastHeartbeatTime(List<IHeartbeatListener> listeners) {
        if (listeners != null) {
            Long ts = System.currentTimeMillis();
            for (IHeartbeatListener iHeartbeatListener : listeners) {
                lastHeartbeatTimestamps.put(iHeartbeatListener, ts);
            }
        }
    }

    /**
     * @see IDataService#heartbeat()
     */
    public void heartbeat(boolean force) {
        List<IHeartbeatListener> listeners = getHeartbeatListeners(force);
        if (listeners.size() > 0) {
            Node me = engine.getNodeService().findIdentity();
            if (me != null) {
                for (IHeartbeatListener l : listeners) {
                    l.heartbeat(me);
                }
                updateLastHeartbeatTime(listeners);
            } else {
                log.debug("Did not run the heartbeat process because the node has not been configured");
            }
        }
    }

    public List<Number> listDataIds(long batchId, String nodeId) {
        return sqlTemplate.query(getSql("selectEventDataIdsSql", " order by d.data_id asc"),
                new NumberMapper(), batchId, nodeId);
    }

    public List<Data> listData(long batchId, String nodeId, long startDataId, String channelId,
            final int maxRowsToRetrieve) {
        return sqlTemplate.query(getDataSelectSql(batchId, startDataId, channelId),
                maxRowsToRetrieve, this.dataMapper, new Object[] {batchId, nodeId, startDataId}, 
                new int[] { symmetricDialect.getSqlTypeForIds(), Types.VARCHAR, symmetricDialect.getSqlTypeForIds()});
    }

    public Data mapData(Row row) {
        return dataMapper.mapRow(row);
    }

    public ISqlReadCursor<Data> selectDataFor(Batch batch) {
        return sqlTemplate.queryForCursor(
                getDataSelectSql(batch.getBatchId(), -1l, batch.getChannelId()), dataMapper,
                new Object[] { batch.getBatchId(), batch.getTargetNodeId() },
                new int[] { symmetricDialect.getSqlTypeForIds(), Types.VARCHAR });
    }

    public ISqlReadCursor<Data> selectDataFor(Long batchId, String channelId) {
        return sqlTemplate.queryForCursor(getDataSelectByBatchSql(batchId, -1l, channelId),
                dataMapper, new Object[] { batchId }, new int[] { symmetricDialect.getSqlTypeForIds() });
    }

    protected String getDataSelectByBatchSql(long batchId, long startDataId, String channelId) {
        String startAtDataIdSql = startDataId >= 0l ? " and d.data_id >= ? " : "";
        return symmetricDialect.massageDataExtractionSql(
                getSql("selectEventDataByBatchIdSql", startAtDataIdSql, " order by d.data_id asc"),
                engine.getConfigurationService().getNodeChannel(channelId, false).getChannel());
    }

    protected String getDataSelectSql(long batchId, long startDataId, String channelId) {
        String startAtDataIdSql = startDataId >= 0l ? " and d.data_id >= ? " : "";
        return symmetricDialect.massageDataExtractionSql(
                getSql("selectEventDataToExtractSql", startAtDataIdSql, " order by d.data_id asc"),
                engine.getConfigurationService().getNodeChannel(channelId, false).getChannel());
    }

    public long findMaxDataId() {
        return sqlTemplate.queryForLong(getSql("selectMaxDataIdSql"));
    }
    
    
    @Override
    public void deleteCapturedConfigChannelData() {
        int count = sqlTemplate.update(getSql("deleteCapturedConfigChannelDataSql"));
        if (count > 0) {
            log.info("Deleted {} data rows that were on the config channel", count);
        }
    }

    public class DataMapper implements ISqlRowMapper<Data> {
        public Data mapRow(Row row) {
            Data data = new Data();
            data.putCsvData(CsvData.ROW_DATA, row.getString("ROW_DATA", false));
            data.putCsvData(CsvData.PK_DATA, row.getString("PK_DATA", false));
            data.putCsvData(CsvData.OLD_DATA, row.getString("OLD_DATA", false));
            data.putAttribute(CsvData.ATTRIBUTE_CHANNEL_ID, row.getString("CHANNEL_ID"));
            data.putAttribute(CsvData.ATTRIBUTE_TX_ID, row.getString("TRANSACTION_ID", false));
            data.putAttribute(CsvData.ATTRIBUTE_TABLE_NAME, row.getString("TABLE_NAME"));
            data.setDataEventType(DataEventType.getEventType(row.getString("EVENT_TYPE")));
            data.putAttribute(CsvData.ATTRIBUTE_SOURCE_NODE_ID, row.getString("SOURCE_NODE_ID"));
            data.putAttribute(CsvData.ATTRIBUTE_EXTERNAL_DATA, row.getString("EXTERNAL_DATA"));
            data.putAttribute(CsvData.ATTRIBUTE_NODE_LIST, row.getString("NODE_LIST"));
            data.putAttribute(CsvData.ATTRIBUTE_DATA_ID, row.getLong("DATA_ID"));
            data.putAttribute(CsvData.ATTRIBUTE_CREATE_TIME, row.getDateTime("CREATE_TIME"));
            data.putAttribute(CsvData.ATTRIBUTE_ROUTER_ID, row.getString("ROUTER_ID", false));
            int triggerHistId = row.getInt("TRIGGER_HIST_ID");
            data.putAttribute(CsvData.ATTRIBUTE_TABLE_ID, triggerHistId);
            TriggerHistory triggerHistory = engine.getTriggerRouterService().getTriggerHistory(
                    triggerHistId);
            if (triggerHistory == null) {
                triggerHistory = new TriggerHistory(triggerHistId);
            } else {
                if (!triggerHistory.getSourceTableName().equals(data.getTableName())) {
                    log.warn("There was a mismatch between the data table name {} and the trigger_hist "
                            + "table name {} for data_id {}.  Attempting to look up a valid trigger_hist row by table name",
                            new Object[] { data.getTableName(),
                                    triggerHistory.getSourceTableName(), data.getDataId() });
                    List<TriggerHistory> list = engine.getTriggerRouterService()
                            .getActiveTriggerHistories(data.getTableName());
                    if (list.size() > 0) {
                        triggerHistory = list.get(0);
                    }
                }
            }
            data.setTriggerHistory(triggerHistory);
            return data;
        }
    }

}
