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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
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
import org.jumpmind.symmetric.model.ProcessInfo;
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
        if (request != null) {
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
                                            .getBeforeCustomSql()) ? request.getBeforeCustomSql()
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
    
    public void insertTableReloadRequest(TableReloadRequest request) {
        Date time = new Date();
        request.setLastUpdateTime(time);
        if (request.getCreateTime() == null) {
            request.setCreateTime(time);
        }
        sqlTemplate.update(
                getSql("insertTableReloadRequest"),
                new Object[] { request.getReloadSelect(), request.getBeforeCustomSql(),
                        request.getCreateTime(), request.getLastUpdateBy(),
                        request.getLastUpdateTime(), request.getSourceNodeId(),
                        request.getTargetNodeId(), request.getTriggerId(),
                        request.getRouterId(), request.isCreateTable() ? 1 : 0, 
                        request.isDeleteFirst() ? 1 : 0 });
    }

    public TableReloadRequest getTableReloadRequest(final TableReloadRequestKey key) {
        return sqlTemplate.queryForObject(getSql("selectTableReloadRequest"),
                new ISqlRowMapper<TableReloadRequest>() {
                    public TableReloadRequest mapRow(Row rs) {
                        TableReloadRequest request = new TableReloadRequest(key);
                        request.setReloadSelect(rs.getString("reload_select"));
                        request.setReloadTime(rs.getDateTime("reload_time"));
                        request.setBeforeCustomSql(rs.getString("before_custom_sql"));
                        request.setCreateTime(rs.getDateTime("create_time"));
                        request.setLastUpdateBy(rs.getString("last_update_by"));
                        request.setLastUpdateTime(rs.getDateTime("last_update_time"));
                        return request;
                    }
                }, key.getSourceNodeId(), key.getTargetNodeId(), key.getTriggerId(),
                key.getRouterId());
    }
    
    public List<TableReloadRequest> getTableReloadRequestToProcess(final String sourceNodeId) {
        return sqlTemplate.query(getSql("selectTableReloadRequestToProcess"),
                new ISqlRowMapper<TableReloadRequest>() {
                    public TableReloadRequest mapRow(Row rs) {
                        TableReloadRequest request = new TableReloadRequest();
                        request.setSourceNodeId(sourceNodeId);
                        request.setTargetNodeId(rs.getString("target_node_id"));
                        request.setCreateTable(rs.getBoolean("create_table"));
                        request.setDeleteFirst(rs.getBoolean("delete_first"));
                        request.setReloadSelect(rs.getString("reload_select"));
                        request.setReloadTime(rs.getDateTime("reload_time"));
                        request.setBeforeCustomSql(rs.getString("before_custom_sql"));
                        request.setChannelId(rs.getString("channel_id"));
                        request.setTriggerId(rs.getString("trigger_id"));
                        request.setRouterId(rs.getString("router_id"));
                        request.setCreateTime(rs.getDateTime("create_time"));
                        request.setLastUpdateBy(rs.getString("last_update_by"));
                        request.setLastUpdateTime(rs.getDateTime("last_update_time"));
                        return request;
                    }
                }, sourceNodeId);
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
        data.setNodeList(targetNode.getNodeId());
        if (isLoad) {
            return insertDataAndDataEventAndOutgoingBatch(transaction, data,
                    targetNode.getNodeId(), triggerRouter.getRouter().getRouterId(), isLoad,
                    loadId, createBy, status);
        } else {
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
    
    public void insertReloadEvents(Node targetNode, boolean reverse, ProcessInfo processInfo) {
        insertReloadEvents(targetNode, reverse, null, processInfo);
    }
    
    public void insertReloadEvents(Node targetNode, boolean reverse, List<TableReloadRequest> reloadRequests, ProcessInfo processInfo) {

        if (engine.getClusterService().lock(ClusterConstants.SYNCTRIGGERS)) {
            try {
                synchronized (engine.getTriggerRouterService()) {
                    engine.getClusterService().lock(ClusterConstants.SYNCTRIGGERS);

                    boolean isFullLoad = reloadRequests == null 
                            || (reloadRequests.size() == 1 && reloadRequests.get(0).isFullLoadRequest());
                    
                    if (!reverse) {
                        log.info("Queueing up " + (isFullLoad ? "an initial" : "a") + " load to node " + targetNode.getNodeId());
                    } else {
                        log.info("Queueing up a reverse " + (isFullLoad ? "initial" : "") + " load to node " + targetNode.getNodeId());
                    }
                    
                    /*
                     * Outgoing data events are pointless because we are
                     * reloading all data
                     */
                    if (isFullLoad) {
                        engine.getOutgoingBatchService().markAllAsSentForNode(targetNode.getNodeId(),
                                false);
                    }
                    
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
                        processInfo.setCurrentLoadId(loadId);
                        
                        String createBy = reverse ? nodeSecurity.getRevInitialLoadCreateBy()
                                : nodeSecurity.getInitialLoadCreateBy();

                        List<TriggerHistory> triggerHistories = new ArrayList<TriggerHistory>();

                        if (isFullLoad) {
                            triggerHistories = triggerRouterService.getActiveTriggerHistories();
                        }
                        else {
                            for (TableReloadRequest reloadRequest : reloadRequests) {
                                triggerHistories.addAll(engine.getTriggerRouterService()
                                        .getActiveTriggerHistories(new Trigger(reloadRequest.getTriggerId(), null)));
                            }
                        }
                        processInfo.setDataCount(triggerHistories.size());
                        
                        Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId = triggerRouterService
                                .fillTriggerRoutersByHistIdAndSortHist(sourceNode.getNodeGroupId(),
                                        targetNode.getNodeGroupId(), triggerHistories);

                        if (isFullLoad) {
                            callReloadListeners(true, targetNode, transactional, transaction, loadId);

                            insertCreateSchemaScriptPriorToReload(targetNode, nodeIdRecord, loadId,
                                createBy, transactional, transaction);
                        }
                        Map<String, TableReloadRequest> mapReloadRequests = convertReloadListToMap(reloadRequests);
                        
                        String symNodeSecurityReloadChannel = null;
                        try {
                        	symNodeSecurityReloadChannel = triggerRoutersByHistoryId.get(triggerHistories.get(0)
                        			.getTriggerHistoryId()).get(0).getTrigger().getReloadChannelId();
                        }
                        catch (Exception e) { }
                        
                        if (isFullLoad || (reloadRequests != null && reloadRequests.size() > 0)) {
                            insertSqlEventsPriorToReload(targetNode, nodeIdRecord, loadId, createBy,
                                transactional, transaction, reverse, 
                                triggerHistories, triggerRoutersByHistoryId, 
                                mapReloadRequests, isFullLoad, symNodeSecurityReloadChannel);
                        }
                        
                        insertCreateBatchesForReload(targetNode, loadId, createBy,
                                triggerHistories, triggerRoutersByHistoryId, transactional,
                                transaction, mapReloadRequests);

                        insertDeleteBatchesForReload(targetNode, loadId, createBy,
                                triggerHistories, triggerRoutersByHistoryId, transactional,
                                transaction, mapReloadRequests);

                        insertSQLBatchesForReload(targetNode, loadId, createBy,
                                triggerHistories, triggerRoutersByHistoryId, transactional,
                                transaction, mapReloadRequests);

                        insertLoadBatchesForReload(targetNode, loadId, createBy, triggerHistories,
                                triggerRoutersByHistoryId, transactional, transaction, mapReloadRequests, processInfo);
                        
                        
                        if (isFullLoad) {
                            String afterSql = parameterService
                                    .getString(reverse ? ParameterConstants.INITIAL_LOAD_REVERSE_AFTER_SQL
                                            : ParameterConstants.INITIAL_LOAD_AFTER_SQL);
                            if (isNotBlank(afterSql)) {
                                insertSqlEvent(transaction, targetNode, afterSql, true, loadId,
                                        createBy);
                            }
                        }
                        insertFileSyncBatchForReload(targetNode, loadId, createBy, transactional,
                                transaction, processInfo);

                        if (isFullLoad) {
                            callReloadListeners(false, targetNode, transactional, transaction, loadId);
                            if (!reverse) {
                                nodeService.setInitialLoadEnabled(transaction, nodeIdRecord, false,
                                    false, loadId, createBy);
                            } else {
                                nodeService.setReverseInitialLoadEnabled(transaction, nodeIdRecord,
                                        false, false, loadId, createBy);
                            }
                        }
                                                
                        if (!Constants.DEPLOYMENT_TYPE_REST.equals(targetNode.getDeploymentType())) {
                        	insertNodeSecurityUpdate(transaction, nodeIdRecord,
                                    targetNode.getNodeId(), true, loadId, createBy, symNodeSecurityReloadChannel);
                        }

                        engine.getStatisticManager().incrementNodesLoaded(1);

                        if (reloadRequests != null && reloadRequests.size() > 0) {
                            for (TableReloadRequest request : reloadRequests) {
                                transaction.prepareAndExecute(getSql("updateProcessedTableReloadRequest"), loadId, new Date(),
                                        request.getTargetNodeId(), request.getSourceNodeId(), request.getTriggerId(), 
                                        request.getRouterId(), request.getCreateTime());
                            }
                            log.info("Table reload request(s) for load id " + loadId + " have been processed.");
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

    protected Map<String, TableReloadRequest> convertReloadListToMap(List<TableReloadRequest> reloadRequests) {
        if (reloadRequests == null) {
            return null;
        }
        Map<String, TableReloadRequest> reloadMap = new HashMap<String, TableReloadRequest>();
        for (TableReloadRequest item : reloadRequests) {
            reloadMap.put(item.getIdentifier(), item);
        }
        return reloadMap;
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
            String createBy, boolean transactional, ISqlTransaction transaction, boolean reverse,
            List<TriggerHistory> triggerHistories,
            Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId,
            Map<String, TableReloadRequest> reloadRequests, boolean isFullLoad, String channelId) {

            if (!Constants.DEPLOYMENT_TYPE_REST.equals(targetNode.getDeploymentType())) {
                /*
                 * Insert node security so the client doing the initial load knows
                 * that an initial load is currently happening
                 */
                insertNodeSecurityUpdate(transaction, nodeIdRecord, targetNode.getNodeId(), true,
                        loadId, createBy, channelId);
    
                if (isFullLoad) {
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
            }
            
            if (!isFullLoad && reloadRequests != null && reloadRequests.size() > 0) {
                String beforeSql = "";
                int beforeSqlSent = 0;
                
                for (TriggerHistory triggerHistory : triggerHistories) {
                    List<TriggerRouter> triggerRouters = triggerRoutersByHistoryId.get(triggerHistory
                            .getTriggerHistoryId());
                    for (TriggerRouter triggerRouter : triggerRouters) {
                        TableReloadRequest currentRequest = reloadRequests.get(triggerRouter.getTriggerId() + triggerRouter.getRouterId());
                        beforeSql = currentRequest.getBeforeCustomSql();
                        
                        if (isNotBlank(beforeSql)) {
                            String tableName = triggerRouter.qualifiedTargetTableName(triggerHistory);
                            String formattedBeforeSql = String.format(beforeSql, tableName) + ";";
                            
                            insertSqlEvent(
                                    transaction,
                                    targetNode,
                                    formattedBeforeSql, true,
                                    loadId, createBy);  
                            beforeSqlSent++;
                        }
                    }
                }
                if (beforeSqlSent > 0) {
                    log.info("Before sending load {} to target node {} the before sql [{}] was sent for {} tables", new Object[] {
                            loadId, targetNode, beforeSql, beforeSqlSent });
                }
                
            } else {
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
    }

    private void insertCreateBatchesForReload(Node targetNode, long loadId, String createBy,
            List<TriggerHistory> triggerHistories,
            Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId, boolean transactional,
            ISqlTransaction transaction, Map<String, TableReloadRequest> reloadRequests) {
        
        if (reloadRequests != null && reloadRequests.size() > 0) {
            int createEventsSent = 0;
            for (TriggerHistory triggerHistory : triggerHistories) {
                List<TriggerRouter> triggerRouters = triggerRoutersByHistoryId.get(triggerHistory
                        .getTriggerHistoryId());
                
                TableReloadRequest currentRequest = reloadRequests.get(ParameterConstants.ALL + ParameterConstants.ALL);
                boolean fullLoad = currentRequest == null ? false : true;
                
                for (TriggerRouter triggerRouter : triggerRouters) {
                    if (!fullLoad) {
                        currentRequest = reloadRequests.get(triggerRouter.getTriggerId() + triggerRouter.getRouterId());
                    }
                    
                    //Check the create flag on the specific table reload request
                    if (currentRequest != null && currentRequest.isCreateTable()
                            && engine.getGroupletService().isTargetEnabled(triggerRouter,
                                    targetNode)) {
                        insertCreateEvent(transaction, targetNode, triggerHistory, triggerRouter.getRouter().getRouterId(), true,
                                loadId, createBy);
                        createEventsSent++;
                        if (!transactional) {
                            transaction.commit();
                        }
                    }
                }
            }
            if (createEventsSent > 0) {
                log.info("Before sending load {} to target node {} create table events were sent for {} tables", new Object[] {
                        loadId, targetNode, createEventsSent });
            }
        }
        else {
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
    }

    private void insertDeleteBatchesForReload(Node targetNode, long loadId, String createBy,
            List<TriggerHistory> triggerHistories,
            Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId, boolean transactional,
            ISqlTransaction transaction, Map<String, TableReloadRequest> reloadRequests) {
        
        if (reloadRequests != null && reloadRequests.size() > 0) {
            int deleteEventsSent = 0;
            
            for (ListIterator<TriggerHistory> triggerHistoryIterator = triggerHistories
                    .listIterator(triggerHistories.size()); triggerHistoryIterator.hasPrevious();) {
                TriggerHistory triggerHistory = triggerHistoryIterator.previous();
                List<TriggerRouter> triggerRouters = triggerRoutersByHistoryId.get(triggerHistory
                        .getTriggerHistoryId());
                TableReloadRequest currentRequest = reloadRequests.get(ParameterConstants.ALL + ParameterConstants.ALL);
                boolean fullLoad = currentRequest == null ? false : true;

                for (ListIterator<TriggerRouter> iterator = triggerRouters
                        .listIterator(triggerRouters.size()); iterator.hasPrevious();) {
                    TriggerRouter triggerRouter = iterator.previous();                
                    if (!fullLoad) {
                        currentRequest = reloadRequests.get(triggerRouter.getTriggerId() + triggerRouter.getRouterId());
                    }
                    
                    //Check the delete flag on the specific table reload request
                    if (currentRequest != null && currentRequest.isDeleteFirst()
                            && engine.getGroupletService().isTargetEnabled(triggerRouter,
                                    targetNode)) {
                        insertPurgeEvent(transaction, targetNode, triggerRouter, triggerHistory,
                                true, currentRequest.getBeforeCustomSql(), loadId, createBy);
                        deleteEventsSent++;
                        if (!transactional) {
                            transaction.commit();
                        }
                    }
                }
            }
            if (deleteEventsSent > 0) {
                log.info("Before sending load {} to target node {} delete data events were sent for {} tables", new Object[] {
                        loadId, targetNode, deleteEventsSent });
            }
        }
        else {
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
    }

    private void insertSQLBatchesForReload(Node targetNode, long loadId, String createBy,
            List<TriggerHistory> triggerHistories,
            Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId, boolean transactional,
            ISqlTransaction transaction, Map<String, TableReloadRequest> reloadRequests) {
        
        if (reloadRequests != null && reloadRequests.size() > 0) {
            int sqlEventsSent = 0;
            for (TriggerHistory triggerHistory : triggerHistories) {
                List<TriggerRouter> triggerRouters = triggerRoutersByHistoryId.get(triggerHistory
                        .getTriggerHistoryId());
                
                TableReloadRequest currentRequest = reloadRequests.get(ParameterConstants.ALL + ParameterConstants.ALL);
                boolean fullLoad = currentRequest == null ? false : true;
                
                for (TriggerRouter triggerRouter : triggerRouters) {
                    if (!fullLoad) {
                        currentRequest = reloadRequests.get(triggerRouter.getTriggerId() + triggerRouter.getRouterId());
                    }
                    
                    //Check the before custom sql is present on the specific table reload request
                    if (currentRequest != null 
                            && currentRequest.getBeforeCustomSql() != null 
                            && currentRequest.getBeforeCustomSql().length() > 0
                            && engine.getGroupletService().isTargetEnabled(triggerRouter,
                                    targetNode)) {
                        
                        String tableName = triggerRouter.qualifiedTargetTableName(triggerHistory);
                        String formattedBeforeSql = String.format(currentRequest.getBeforeCustomSql(), tableName) + ";";
                        
                        insertSqlEvent(transaction, triggerHistory, triggerRouter.getTrigger().getChannelId(),
                                targetNode, formattedBeforeSql,
                                true, loadId, createBy);
                        sqlEventsSent++;
                        if (!transactional) {
                            transaction.commit();
                        }
                    }
                }
            }
            if (sqlEventsSent > 0) {
                log.info("Before sending load {} to target node {} SQL data events were sent for {} tables", new Object[] {
                        loadId, targetNode, sqlEventsSent });
            }
        }
    }
    
    private void insertLoadBatchesForReload(Node targetNode, long loadId, String createBy,
            List<TriggerHistory> triggerHistories,
            Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId, boolean transactional,
            ISqlTransaction transaction, Map<String, TableReloadRequest> reloadRequests, ProcessInfo processInfo) {
        Map<String, Channel> channels = engine.getConfigurationService().getChannels(false);
        
        for (TriggerHistory triggerHistory : triggerHistories) {
            List<TriggerRouter> triggerRouters = triggerRoutersByHistoryId.get(triggerHistory
                    .getTriggerHistoryId());
            
            processInfo.incrementCurrentDataCount();
            
            for (TriggerRouter triggerRouter : triggerRouters) {
                if (triggerRouter.getInitialLoadOrder() >= 0
                        && engine.getGroupletService().isTargetEnabled(triggerRouter, targetNode)) {
                    
                    String selectSql = null;
                    if (reloadRequests != null) {
                        TableReloadRequest reloadRequest = reloadRequests.get(triggerRouter.getTriggerId() + triggerRouter.getRouterId());
                        selectSql = reloadRequest != null ? reloadRequest.getReloadSelect() : null;
                    }
                    if (StringUtils.isBlank(selectSql)) {
                        selectSql = StringUtils.isBlank(triggerRouter.getInitialLoadSelect()) 
                                    ? Constants.ALWAYS_TRUE_CONDITION
                                    : triggerRouter.getInitialLoadSelect();
                    }
                    
                    if (parameterService.is(ParameterConstants.INITIAL_LOAD_USE_EXTRACT_JOB)) {
                        Trigger trigger = triggerRouter.getTrigger();
                        String reloadChannel = getReloadChannelIdForTrigger(trigger, channels);
                        Channel channel = channels.get(reloadChannel);
                                                   
                        Table table = platform.getTableFromCache(
                                triggerHistory.getSourceCatalogName(), triggerHistory.getSourceSchemaName(),
                                triggerHistory.getSourceTableName(), false);  
                        
                        processInfo.setCurrentTableName(table.getName());
                        
                        int numberOfBatches = getNumberOfReloadBatches(table, triggerRouter, 
                                channel, targetNode, selectSql);                        

                        long startBatchId = -1;
                        long endBatchId = -1;
                        for (int i = 0; i < numberOfBatches; i++) {
                            // needs to grab the start and end batch id
                            endBatchId = insertReloadEvent(transaction, targetNode, triggerRouter,
                                    triggerHistory, selectSql, true, loadId, createBy, Status.RQ);
                            if (startBatchId == -1) {
                                startBatchId = endBatchId;
                            }
                        }
                        
                        engine.getDataExtractorService().requestExtractRequest(transaction,
                                targetNode.getNodeId(), channel.getQueue(), triggerRouter, startBatchId, endBatchId);
                    } else {
                        insertReloadEvent(transaction, targetNode, triggerRouter, triggerHistory,
                                selectSql, true, loadId, createBy, Status.NE);
                    }

                    if (!transactional) {
                        transaction.commit();
                    }
                }
                
            }
        }
    }
    
    protected int getNumberOfReloadBatches(Table table, TriggerRouter triggerRouter, Channel channel, Node targetNode, String selectSql) {
        int rowCount = getDataCountForReload(table, targetNode, selectSql);        
        int transformMultiplier = getTransformMultiplier(table, triggerRouter);
        
        // calculate the number of batches needed for table.
        int numberOfBatches = 1;        
        
        if (rowCount > 0) {
            numberOfBatches = (rowCount * transformMultiplier / channel.getMaxBatchSize()) + 1;
        }
        
        return numberOfBatches;
    }

    protected int getDataCountForReload(Table table, Node targetNode, String selectSql) {
        DatabaseInfo dbInfo = platform.getDatabaseInfo();
        String quote = dbInfo.getDelimiterToken();
        String catalogSeparator = dbInfo.getCatalogSeparator();
        String schemaSeparator = dbInfo.getSchemaSeparator();
                                          
        String sql = String.format("select count(*) from %s where %s", table
                .getQualifiedTableName(quote, catalogSeparator, schemaSeparator), selectSql);
        sql = FormatUtils.replace("groupId", targetNode.getNodeGroupId(), sql);
        sql = FormatUtils.replace("externalId", targetNode.getExternalId(), sql);
        sql = FormatUtils.replace("nodeId", targetNode.getNodeId(), sql);
        
        int rowCount = sqlTemplate.queryForInt(sql);
        return rowCount;
    }

    protected int getTransformMultiplier(Table table, TriggerRouter triggerRouter) {
        int transformMultiplier = 0;
        for (TransformService.TransformTableNodeGroupLink transform : engine.getTransformService().getTransformTables(false)) {
            if (triggerRouter.getRouter().getNodeGroupLink().equals(transform.getNodeGroupLink()) && 
                    transform.getSourceTableName().equals(table.getName())) {
                transformMultiplier++;
            }
        }
        transformMultiplier = Math.max(1, transformMultiplier);
        return transformMultiplier;
    }    

    private void insertFileSyncBatchForReload(Node targetNode, long loadId, String createBy,
            boolean transactional, ISqlTransaction transaction, ProcessInfo processInfo) {
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
                
                List<TriggerHistory> triggerHistories = Arrays.asList(fileSyncSnapshotHistory);
                List<TriggerRouter> triggerRouters = Arrays.asList(fileSyncSnapshotTriggerRouter);
                Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId = new HashMap<Integer, List<TriggerRouter>>();
                triggerRoutersByHistoryId.put(fileSyncSnapshotHistory.getTriggerHistoryId(), triggerRouters);
                
                if (parameterService.is(ParameterConstants.INITIAL_LOAD_USE_EXTRACT_JOB)) {            
                    insertLoadBatchesForReload(targetNode, loadId, createBy, triggerHistories, 
                            triggerRoutersByHistoryId, transactional, transaction, null, processInfo);
                } else {                    
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
        data.setNodeList(targetNode.getNodeId());
        if (isLoad) {
            insertDataAndDataEventAndOutgoingBatch(transaction, data, targetNode.getNodeId(),
                    Constants.UNKNOWN_ROUTER_ID, isLoad, loadId, createBy, Status.NE);
        } else {
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
        data.setNodeList(targetNode.getNodeId());
        if (isLoad) {
            insertDataAndDataEventAndOutgoingBatch(transaction, data, targetNode.getNodeId(),
                    Constants.UNKNOWN_ROUTER_ID, isLoad, loadId, createBy, Status.NE);
        } else {
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
        data.setNodeList(targetNode.getNodeId());
        try {
            if (isLoad) {
                insertDataAndDataEventAndOutgoingBatch(transaction, data, targetNode.getNodeId(),
                        routerId, isLoad, loadId, createBy, Status.NE);
            } else {
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
                        data.getDataEventType(), routerId, isLoad, loadId, createBy, Status.NE, data.getTableName());
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
            Status status, String overrideChannelId) {
        long dataId = insertData(transaction, data);
        String channelId = null;
        if (isLoad) {
        	if (overrideChannelId != null) {
        		channelId = overrideChannelId;
        	}
        	else {
	            TriggerHistory history = data.getTriggerHistory();
	            if (history != null && channelId == null) {
	                Trigger trigger = engine.getTriggerRouterService().getTriggerById(
	                        history.getTriggerId());
	                channelId = getReloadChannelIdForTrigger(trigger, engine.getConfigurationService()
	                        .getChannels(false));
	            }
        	}
        } else {
            channelId = data.getChannelId();
        }
        return insertDataEventAndOutgoingBatch(transaction, dataId, channelId, nodeId,
                data.getDataEventType(), routerId, isLoad, loadId, createBy, status, data.getTableName());
    }
    
    public long insertDataAndDataEventAndOutgoingBatch(ISqlTransaction transaction, Data data,
            String nodeId, String routerId, boolean isLoad, long loadId, String createBy,
            Status status) {
    	return insertDataAndDataEventAndOutgoingBatch(transaction, data, nodeId, routerId, isLoad, loadId, createBy, status, null);
    }

    protected long insertDataEventAndOutgoingBatch(ISqlTransaction transaction, long dataId,
            String channelId, String nodeId, DataEventType eventType, String routerId,
            boolean isLoad, long loadId, String createBy, Status status, String tableName) {
        OutgoingBatch outgoingBatch = new OutgoingBatch(nodeId, channelId, status);
        outgoingBatch.setLoadId(loadId);
        outgoingBatch.setCreateBy(createBy);
        outgoingBatch.setLoadFlag(isLoad);
        outgoingBatch.incrementEventCount(eventType);
        if (tableName != null) {            
            outgoingBatch.incrementTableCount(tableName.toLowerCase());
        }
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
            String targetNodeId, boolean isLoad, long loadId, String createBy, String channelId) {
        Data data = createData(transaction, null, null, tablePrefix + "_node_security",
                " t.node_id = '" + nodeIdRecord + "'");
        if (data != null) {
        	insertDataAndDataEventAndOutgoingBatch(transaction, data, targetNodeId,
                    Constants.UNKNOWN_ROUTER_ID, isLoad, loadId, createBy, Status.NE, channelId);
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

    public void reloadMissingForeignKeyRows(String nodeId, long dataId) {
        Data data = findData(dataId);
        Table table = platform.getTableFromCache(data.getTableName(), false);
        Map<String, String> dataMap = data.toColumnNameValuePairs(table.getColumnNames(), CsvData.ROW_DATA);

        List<TableRow> tableRows = new ArrayList<TableRow>();
        Row row = new Row(dataMap.size());
        row.putAll(dataMap);
        tableRows.add(new TableRow(table, row, null));
        List<TableRow> foreignTableRows;
        try {
            foreignTableRows = getForeignTableRows(tableRows, new HashSet<TableRow>());
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        
        Collections.reverse(foreignTableRows);
        for (TableRow foreignTableRow : foreignTableRows) {
            Table foreignTable = foreignTableRow.getTable();
            String catalog = foreignTable.getCatalog();
            String schema = foreignTable.getSchema();
            if (StringUtils.equals(platform.getDefaultCatalog(), catalog)) {
                catalog = null;
            }
            if (StringUtils.equals(platform.getDefaultSchema(), schema)) {
                schema = null;
            }

            reloadTable(nodeId, catalog, schema, foreignTable.getName(), foreignTableRow.getWhereSql());
        }        
    }

    protected List<TableRow> getForeignTableRows(List<TableRow> tableRows, Set<TableRow> visited) throws CloneNotSupportedException {
        List<TableRow> fkDepList = new ArrayList<TableRow>();
        for (TableRow tableRow : tableRows) {
            if (visited.add(tableRow)) {
                for (ForeignKey fk : tableRow.getTable().getForeignKeys()) {
                    Table table = platform.getTableFromCache(fk.getForeignTableName(), false);
                    if (table != null) {
                        Table foreignTable = (Table) table.clone();
                        for (Column column : foreignTable.getColumns()) {
                            column.setPrimaryKey(false);
                        }
                        Row whereRow = new Row(fk.getReferenceCount());
                        for (Reference ref : fk.getReferences()) {
                            Column foreignColumn = foreignTable.findColumn(ref.getForeignColumnName());
                            Object value = tableRow.getRow().get(ref.getLocalColumnName());
                            whereRow.put(foreignColumn.getName(), value);
                            foreignColumn.setPrimaryKey(true);
                        }
                        
                        DmlStatement whereSt = platform.createDmlStatement(DmlType.WHERE, foreignTable, null);
                        String whereSql = whereSt.buildDynamicSql(symmetricDialect.getBinaryEncoding(), whereRow, false, true, 
                                foreignTable.getPrimaryKeyColumns()).substring(6);
                        String delimiter = platform.getDatabaseInfo().getSqlCommandDelimiter();
                        if (delimiter != null && delimiter.length() > 0) {
                            whereSql = whereSql.substring(0, whereSql.length() - delimiter.length());
                        }
                        
                        Row foreignRow = new Row(foreignTable.getColumnCount());
                        if (foreignTable.getForeignKeyCount() > 0) {
                            DmlStatement selectSt = platform.createDmlStatement(DmlType.SELECT, foreignTable, null);
                            Map<String, Object> values = sqlTemplate.queryForMap(selectSt.getSql(), 
                                    whereRow.toArray(foreignTable.getPrimaryKeyColumnNames()));
                            foreignRow.putAll(values);
                        }
    
                        TableRow foreignTableRow = new TableRow(foreignTable, foreignRow, whereSql);
                        fkDepList.add(foreignTableRow);
                    }
                }
            }
        }
        if (fkDepList.size() > 0) {
            fkDepList.addAll(getForeignTableRows(fkDepList, visited));
        }
        return fkDepList;
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
                    rowData = getCsvDataFor(transaction, trigger, triggerHistory, whereClause, false);
                    pkData = getCsvDataFor(transaction, trigger, triggerHistory, whereClause, true);                    
                }
                data = new Data(trigger.getSourceTableName(), DataEventType.UPDATE, rowData,
                        pkData, triggerHistory, trigger.getChannelId(), null, null);
            }
        }
        return data;
    }
    
    protected String getCsvDataFor(ISqlTransaction transaction, Trigger trigger, TriggerHistory triggerHistory, String whereClause, boolean pkOnly) {
        String data = null;
        String sql = null;
        try {
            if (pkOnly) {
                sql = symmetricDialect.createCsvPrimaryKeySql(trigger, triggerHistory,
                        engine.getConfigurationService().getChannel(trigger.getChannelId()), whereClause);
            } else {
                sql = symmetricDialect.createCsvDataSql(trigger, triggerHistory,
                        engine.getConfigurationService().getChannel(trigger.getChannelId()), whereClause);
            }
        } catch (NotImplementedException e) {
        }
        
        if (isNotBlank(sql)) {
            data = transaction.queryForObject(sql, String.class);
        } else {
            DatabaseInfo databaseInfo = platform.getDatabaseInfo();
            String quote = databaseInfo.getDelimiterToken() == null || !parameterService.is(ParameterConstants.DB_DELIMITED_IDENTIFIER_MODE)
                    ? "" : databaseInfo.getDelimiterToken();
            sql = "select " + triggerHistory.getColumnNames() + " from "
                    + Table.getFullyQualifiedTableName(triggerHistory.getSourceCatalogName(), triggerHistory.getSourceSchemaName(),
                            triggerHistory.getSourceTableName(), quote, databaseInfo.getCatalogSeparator(),
                            databaseInfo.getSchemaSeparator()) + " t where " + whereClause;
            Row row = transaction.queryForRow(sql);
            if (row != null) {
                data = row.csvValue();
            }
        }
        
        if (data != null) {
            data = data.trim();
        }

        return data;
    }

    public long countDataGapsByStatus(DataGap.Status status) {
        return sqlTemplate.queryForLong(getSql("countDataGapsByStatusSql"), new Object[] { status.name() });
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
            log.info("Inserting missing last data gap: {}", gap);
            insertDataGap(gap);
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
        log.debug("Inserting data gap: {}", gap);
        transaction.prepareAndExecute(getSql("insertDataGapSql"),
                new Object[] { DataGap.Status.GP.name(), AppUtils.getHostName(), gap.getStartId(), gap.getEndId(),
                    gap.getLastUpdateTime(), gap.getCreateTime() }, new int[] {
                        Types.VARCHAR, Types.VARCHAR, Types.NUMERIC, Types.NUMERIC, Types.TIMESTAMP, Types.TIMESTAMP });
    }

    public void updateDataGap(DataGap gap, DataGap.Status status) {
        sqlTemplate.update(
                getSql("updateDataGapSql"),
                new Object[] { status.name(), AppUtils.getHostName(), gap.getLastUpdateTime(), gap.getStartId(),
                        gap.getEndId() }, new int[] { Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP,
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
        log.debug("Deleting data gap: {}", gap);
        int count = transaction.prepareAndExecute(
                getSql("deleteDataGapSql"),
                new Object[] { gap.getStartId(), gap.getEndId() },
                new int[] { symmetricDialect.getSqlTypeForIds(),
                        symmetricDialect.getSqlTypeForIds() });
        if (count == 0) {
            log.error("Failed to delete data gap: {}", gap);
        }
    }

    public void deleteAllDataGaps(ISqlTransaction transaction) {
        transaction.prepareAndExecute(getSql("deleteAllDataGapsSql"));
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
        return sqlTemplateDirty.query(getSql("selectEventDataIdsSql", " order by d.data_id asc"),
                new NumberMapper(), batchId, nodeId);
    }

    public List<Data> listData(long batchId, String nodeId, long startDataId, String channelId,
            final int maxRowsToRetrieve) {
        return sqlTemplateDirty.query(getDataSelectSql(batchId, startDataId, channelId),
                maxRowsToRetrieve, this.dataMapper, new Object[] {batchId, nodeId, startDataId}, 
                new int[] { symmetricDialect.getSqlTypeForIds(), Types.VARCHAR, symmetricDialect.getSqlTypeForIds()});
    }

    public Data findData(long dataId) {
        return sqlTemplateDirty.queryForObject(getSql("selectData"), dataMapper, dataId);       
    }
    
    public Data mapData(Row row) {
        return dataMapper.mapRow(row);
    }

    public ISqlReadCursor<Data> selectDataFor(Batch batch) {
        return sqlTemplateDirty.queryForCursor(
                getDataSelectSql(batch.getBatchId(), -1l, batch.getChannelId()), dataMapper,
                new Object[] { batch.getBatchId(), batch.getTargetNodeId() },
                new int[] { symmetricDialect.getSqlTypeForIds(), Types.VARCHAR });
    }

    public ISqlReadCursor<Data> selectDataFor(Long batchId, String channelId) {
        return sqlTemplateDirty.queryForCursor(getDataSelectByBatchSql(batchId, -1l, channelId),
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
        return sqlTemplateDirty.queryForLong(getSql("selectMaxDataIdSql"));
    }
    
    
    @Override
    public void deleteCapturedConfigChannelData() {
        int count = sqlTemplate.update(getSql("deleteCapturedConfigChannelDataSql"));
        if (count > 0) {
            log.info("Deleted {} data rows that were on the config channel", count);
        }
    }

    class TableRow {
        Table table;
        Row row;
        String whereSql;
        
        public TableRow(Table table, Row row, String whereSql) {
            this.table = table;
            this.row = row;
            this.whereSql = whereSql;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((table == null) ? 0 : table.hashCode());
            result = prime * result + ((whereSql == null) ? 0 : whereSql.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof TableRow) {
                TableRow tr = (TableRow) o;
                return tr.table.equals(table) && tr.whereSql.equals(whereSql);
            }
            return false;
        }
                
        public Table getTable() {
            return table;
        }
        
        public Row getRow() {
            return row;
        }
        
        public String getWhereSql() {
            return whereSql;
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
