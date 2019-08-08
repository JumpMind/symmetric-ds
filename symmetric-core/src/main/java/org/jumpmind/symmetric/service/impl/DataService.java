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

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
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
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.ext.IHeartbeatListener;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.job.OracleNoOrderHeartbeat;
import org.jumpmind.symmetric.job.PushHeartbeatListener;
import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.load.IReloadVariableFilter;
import org.jumpmind.symmetric.model.AbstractBatch.Status;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEvent;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.TableReloadRequest;
import org.jumpmind.symmetric.model.TableReloadRequestKey;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerReBuildReason;
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
        if (parameterService.is(ParameterConstants.DBDIALECT_ORACLE_SEQUENCE_NOORDER)) {
            extensionService.addExtensionPoint(new OracleNoOrderHeartbeat(engine));
        }
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
                                        Status.NE, -1);

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
                                    "Could not reload table {} for node {} because the router {} target node group id {} did not match",
                                    new Object[] { trigger.getSourceTableName(), request.getTargetNodeId(),
                                            request.getRouterId(), link.getTargetNodeGroupId() });
                        }
                    } else {
                        log.error(
                                "Could not reload table {}  for node {} because the router {} source node group id {} did not match",
                                new Object[] { trigger.getSourceTableName(), request.getTargetNodeId(), request.getRouterId(),
                                        link.getSourceNodeGroupId() });
                    }
                } else {
                    log.error(
                            "Could not reload table for node {} because the trigger router ({}, {}) could not be found",
                            new Object[] { request.getTargetNodeId(), request.getTriggerId(),
                                    request.getRouterId() });
                }
            } else {
                log.error("Could not reload table for node {} because the target node could not be found",
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
    
    public void insertTableReloadRequest(ISqlTransaction transaction, TableReloadRequest request) {
        Date time = new Date();
        request.setLastUpdateTime(time);
        if (request.getCreateTime() == null) {
            request.setCreateTime(time);
        }
        request.setCreateTime(new Date((request.getCreateTime().getTime() / 1000) * 1000));

        transaction.prepareAndExecute(
                getSql("insertTableReloadRequest"),
                new Object[] { request.getReloadSelect(), request.getBeforeCustomSql(),
                        request.getCreateTime(), request.getLastUpdateBy(),
                        request.getLastUpdateTime(), request.getSourceNodeId(),
                        request.getTargetNodeId(), request.getTriggerId(),
                        request.getRouterId(), request.isCreateTable() ? 1 : 0, 
                        request.isDeleteFirst() ? 1 : 0, request.getChannelId() });
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
            Status status, long estimatedBatchRowCount) {
        String channelId = getReloadChannelIdForTrigger(triggerRouter.getTrigger(), engine
                .getConfigurationService().getChannels(false));
        return insertReloadEvent(transaction, targetNode, triggerRouter, triggerHistory,
                overrideInitialLoadSelect, isLoad, loadId, createBy, status, channelId, estimatedBatchRowCount);
    }

    /**
     * @param estimatedBatchRowCount TODO
     * @return If isLoad then return the inserted batch id otherwise return the
     *         data id
     */
    public long insertReloadEvent(ISqlTransaction transaction, Node targetNode,
            TriggerRouter triggerRouter, TriggerHistory triggerHistory,
            String overrideInitialLoadSelect, boolean isLoad, long loadId, String createBy,
            Status status, String channelId, long estimatedBatchRowCount) {
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
                    loadId, createBy, status, null, estimatedBatchRowCount);
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
    
    @Override
    public void insertReloadEvents(Node targetNode, boolean reverse, ProcessInfo processInfo) {
        ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
        Node sourceNode = engine.getNodeService().findIdentity();
        insertReloadEvents(targetNode, reverse, null, processInfo, triggerRouterService.getActiveTriggerHistories(), triggerRouterService.getAllTriggerRoutersForReloadForCurrentNode(sourceNode.getNodeGroupId(), targetNode.getNodeGroupId()));
    }
    
    @Override
    public void insertReloadEvents(Node targetNode, boolean reverse, List<TableReloadRequest> reloadRequests, ProcessInfo processInfo) {
        ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
        Node sourceNode = engine.getNodeService().findIdentity();
        insertReloadEvents(targetNode, reverse, reloadRequests, processInfo, triggerRouterService.getActiveTriggerHistories(), triggerRouterService.getAllTriggerRoutersForReloadForCurrentNode(sourceNode.getNodeGroupId(), targetNode.getNodeGroupId()));
    }
    
    @Override
    public void insertReloadEvents(Node targetNode, boolean reverse, ProcessInfo processInfo, List<TriggerHistory> activeHistories, List<TriggerRouter> triggerRouters) {
        insertReloadEvents(targetNode, reverse, null, processInfo, activeHistories, triggerRouters);
    }    
    
    
    @Override
    public void insertReloadEvents(Node targetNode, boolean reverse, List<TableReloadRequest> reloadRequests, ProcessInfo processInfo, List<TriggerHistory> activeHistories, List<TriggerRouter> triggerRouters) {
        if (engine.getClusterService().lock(ClusterConstants.SYNC_TRIGGERS)) {
            try {
                INodeService nodeService = engine.getNodeService();
                ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
                
                synchronized (triggerRouterService) {

                    boolean isFullLoad = reloadRequests == null 
                            || (reloadRequests.size() == 1 && reloadRequests.get(0).isFullLoadRequest());
                    
                    boolean isChannelLoad = false;
                    String channelId = null;
                    if (reloadRequests != null 
                            && (reloadRequests.size() == 1 && reloadRequests.get(0).isChannelRequest())) {
                        isChannelLoad=true;
                        channelId = reloadRequests.get(0).getChannelId();
                    }

                    if (!reverse) {
                        log.info("Queueing up " + (isFullLoad ? "an initial" : "a") + " load to node " + targetNode.getNodeId() 
                            + (isChannelLoad ? " for channel " + channelId : ""));
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

                        if (isFullLoad || isChannelLoad) {
                            triggerHistories.addAll(activeHistories);
                            if (reloadRequests != null && reloadRequests.size() == 1) {
                                
                                if (channelId != null) {
                                    List<TriggerHistory> channelTriggerHistories = new ArrayList<TriggerHistory>();
    
                                    for (TriggerHistory history : triggerHistories) {
                                        if (channelId.equals(findChannelFor(history, triggerRouters))) {
                                            channelTriggerHistories.add(history);
                                        }
                                    }
                                    triggerHistories = channelTriggerHistories;
                                }
                            }
                            Database.logMissingDependentTableNames(triggerRouterService.getTablesFor(triggerHistories));
                        } else {
                            for (TableReloadRequest reloadRequest : reloadRequests) {
                                triggerHistories.addAll(engine.getTriggerRouterService()
                                        .getActiveTriggerHistories(new Trigger(reloadRequest.getTriggerId(), null)));
                            }
                        }                       
                        
                        Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId = triggerRouterService
                                .fillTriggerRoutersByHistIdAndSortHist(sourceNode.getNodeGroupId(),
                                        targetNode.getNodeGroupId(), triggerHistories, triggerRouters);

                        if (isFullLoad) {
                            callReloadListeners(true, targetNode, transactional, transaction, loadId);

                            if (reloadRequests == null || reloadRequests.size() == 0) {
                                insertCreateSchemaScriptPriorToReload(targetNode, nodeIdRecord, loadId,
                                    createBy, transactional, transaction);
                            }
                        }
                        Map<String, TableReloadRequest> mapReloadRequests = convertReloadListToMap(reloadRequests, triggerRouters, isFullLoad, isChannelLoad);
                        
                        String symNodeSecurityReloadChannel = null;
                        int totalTableCount = 0;
                        try {
                            for (List<TriggerRouter> triggerRouterList : triggerRoutersByHistoryId.values()) {
                                if (triggerRouterList.size() > 0) {
                                    TriggerRouter tr = triggerRouterList.get(0);
                                    symNodeSecurityReloadChannel = tr.getTrigger().getReloadChannelId();
                                }
                                totalTableCount += triggerRouterList.size();
                            }
                        } catch (Exception e) {
                        }
                        processInfo.setTotalDataCount(totalTableCount);
                        
                        insertSqlEventsPriorToReload(targetNode, nodeIdRecord, loadId, createBy,
                            transactional, transaction, reverse, 
                            triggerHistories, triggerRoutersByHistoryId, 
                            mapReloadRequests, isFullLoad, symNodeSecurityReloadChannel);
                        
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
                                triggerRoutersByHistoryId, transactional, transaction, mapReloadRequests, processInfo, null);
                        
                        insertSqlEventsAfterReload(targetNode, nodeIdRecord, loadId, createBy,
                                transactional, transaction, reverse, 
                                triggerHistories, triggerRoutersByHistoryId, 
                                mapReloadRequests, isFullLoad, symNodeSecurityReloadChannel);

                        insertFileSyncBatchForReload(targetNode, loadId, createBy, transactional,
                                transaction, mapReloadRequests, isFullLoad, processInfo);
                                
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
                                                
                        if (isFullLoad && !Constants.DEPLOYMENT_TYPE_REST.equals(targetNode.getDeploymentType())) {
                        	insertNodeSecurityUpdate(transaction, nodeIdRecord,
                                    targetNode.getNodeId(), true, loadId, createBy, symNodeSecurityReloadChannel);
                        }

                        engine.getStatisticManager().incrementNodesLoaded(1);

                        if (reloadRequests != null && reloadRequests.size() > 0) {
                            for (TableReloadRequest request : reloadRequests) {
                                int rowsAffected = transaction.prepareAndExecute(getSql("updateProcessedTableReloadRequest"), loadId, new Date(),
                                        request.getTargetNodeId(), request.getSourceNodeId(), request.getTriggerId(), 
                                        request.getRouterId(), request.getCreateTime()); 
                                if (rowsAffected == 0) {
                                    throw new SymmetricException(String.format("Failed to update a table_reload_request for loadId '%s' "
                                            + "targetNodeId '%s' sourceNodeId '%s' triggerId '%s' routerId '%s' createTime '%s'", 
                                            loadId, request.getTargetNodeId(), request.getSourceNodeId(), request.getTriggerId(), 
                                                    request.getRouterId(), request.getCreateTime()));
                                }
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

                    if (!reverse && isFullLoad) {
                        /*
                         * Remove all incoming events for the node that we are
                         * starting a reload for
                         */
                        engine.getPurgeService().purgeAllIncomingEventsForNode(
                                targetNode.getNodeId());
                    }
                }
            } finally {
                engine.getClusterService().unlock(ClusterConstants.SYNC_TRIGGERS);
            }
        } else {
            log.info("Not attempting to insert reload events because sync trigger is currently running");
        }

    }
    
    private String findChannelFor(TriggerHistory history, List<TriggerRouter> triggerRouters) {
        for (TriggerRouter triggerRouter : triggerRouters) {
            if (triggerRouter.getTrigger().getTriggerId().equals(history.getTriggerId())) {
                return triggerRouter.getTrigger().getChannelId();
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    protected Map<String, TableReloadRequest> convertReloadListToMap(List<TableReloadRequest> reloadRequests, List<TriggerRouter> triggerRouters, boolean isFullLoad, boolean isChannelLoad) {
        if (reloadRequests == null) {
            return null;
        }
        Map<String, TableReloadRequest> reloadMap = new CaseInsensitiveMap();
        for (TableReloadRequest reloadRequest : reloadRequests) {
            if (!isFullLoad && !isChannelLoad) {
                validate(reloadRequest, triggerRouters);
            }
            reloadMap.put(reloadRequest.getIdentifier(), reloadRequest);
        }
        return reloadMap;
    }
    
    protected void validate(TableReloadRequest reloadRequest, List<TriggerRouter> triggerRouters) {
        boolean validMatch = false;
        for (TriggerRouter triggerRouter : triggerRouters) {
            if (ObjectUtils.equals(triggerRouter.getTriggerId(), reloadRequest.getTriggerId())
                    && ObjectUtils.equals(triggerRouter.getRouterId(), reloadRequest.getRouterId())) {
                validMatch = true;
                break;
            }
        }
        
        if (!validMatch) {
            throw new SymmetricException("Table reload request submitted which does not have a valid trigger/router "
                    + "combination in sym_trigger_router. Request trigger id: '" + reloadRequest.getTriggerId() + "' router id: '" 
                    + reloadRequest.getRouterId() + "' create time: " + reloadRequest.getCreateTime());
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
            String createBy, boolean transactional, ISqlTransaction transaction, boolean reverse,
            List<TriggerHistory> triggerHistories,
            Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId,
            Map<String, TableReloadRequest> reloadRequests, boolean isFullLoad, String channelId) {

        if (isFullLoad && !Constants.DEPLOYMENT_TYPE_REST.equals(targetNode.getDeploymentType())) {
            /*
             * Insert node security so the client doing the initial load knows
             * that an initial load is currently happening
             */
            insertNodeSecurityUpdate(transaction, nodeIdRecord, targetNode.getNodeId(), true,
                    loadId, createBy, channelId);

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

        if (isFullLoad) {
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

    private void insertSqlEventsAfterReload(Node targetNode, String nodeIdRecord, long loadId,
            String createBy, boolean transactional, ISqlTransaction transaction, boolean reverse,
            List<TriggerHistory> triggerHistories,
            Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId,
            Map<String, TableReloadRequest> reloadRequests, boolean isFullLoad, String channelId) {

        if (isFullLoad) {
            String afterSql = parameterService.getString(reverse ? ParameterConstants.INITIAL_LOAD_REVERSE_AFTER_SQL
                            : ParameterConstants.INITIAL_LOAD_AFTER_SQL);
            if (isNotBlank(afterSql)) {
                insertSqlEvent(transaction, targetNode, afterSql, true, loadId, createBy);
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
            
            List<TriggerHistory> copyTriggerHistories = new ArrayList<TriggerHistory>(triggerHistories);
            Collections.reverse(copyTriggerHistories);
            
            for (TriggerHistory triggerHistory : copyTriggerHistories) {
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
                        
                        List<String> sqlStatements = resolveTargetTables(currentRequest.getBeforeCustomSql(), 
                                triggerRouter, triggerHistory, targetNode);
                        
                        for (String sql : sqlStatements) {
                            insertSqlEvent(transaction, triggerHistory, triggerRouter.getTrigger().getChannelId(),
                                    targetNode, sql,
                                    true, loadId, createBy);
                            sqlEventsSent++;
                        }
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
            ISqlTransaction transaction, Map<String, TableReloadRequest> reloadRequests, ProcessInfo processInfo,
            String selectSqlOverride) {
        Map<String, Channel> channels = engine.getConfigurationService().getChannels(false);
        
        for (TriggerHistory triggerHistory : triggerHistories) {
            List<TriggerRouter> triggerRouters = triggerRoutersByHistoryId.get(triggerHistory
                    .getTriggerHistoryId());
            
            processInfo.incrementCurrentDataCount();
            
            for (TriggerRouter triggerRouter : triggerRouters) {
                if (triggerRouter.getInitialLoadOrder() >= 0
                        && engine.getGroupletService().isTargetEnabled(triggerRouter, targetNode)) {
                    
                    String selectSql = selectSqlOverride;
                    if (StringUtils.isEmpty(selectSql)) {
                        
                        if (reloadRequests != null) {
                            TableReloadRequest reloadRequest = reloadRequests.get(triggerRouter.getTriggerId() + triggerRouter.getRouterId());
                            selectSql = reloadRequest != null ? reloadRequest.getReloadSelect() : null;
                        }
                        if (StringUtils.isBlank(selectSql)) {
                            selectSql = StringUtils.isBlank(triggerRouter.getInitialLoadSelect()) 
                                    ? Constants.ALWAYS_TRUE_CONDITION
                                            : triggerRouter.getInitialLoadSelect();
                        }
                    }
                    
                    if (parameterService.is(ParameterConstants.INITIAL_LOAD_USE_EXTRACT_JOB)) {
                        Trigger trigger = triggerRouter.getTrigger();
                        String reloadChannel = getReloadChannelIdForTrigger(trigger, channels);
                        Channel channel = channels.get(reloadChannel);
                                                   
                        Table table = platform.getTableFromCache(
                                triggerHistory.getSourceCatalogName(), triggerHistory.getSourceSchemaName(),
                                triggerHistory.getSourceTableName(), false);  
                        
                        if (table != null) {
                            processInfo.setCurrentTableName(table.getName());

                            long rowCount = getDataCountForReload(table, targetNode, selectSql);
                            long transformMultiplier = getTransformMultiplier(table, triggerRouter);

                            // calculate the number of batches needed for table.
                            long numberOfBatches = 1;
                            long lastBatchSize = channel.getMaxBatchSize();

                            if (rowCount > 0) {
                                numberOfBatches = (rowCount * transformMultiplier / channel.getMaxBatchSize()) + 1;
                                lastBatchSize = rowCount % numberOfBatches;
                            }

                            long startBatchId = -1;
                            long endBatchId = -1;
                            for (int i = 0; i < numberOfBatches; i++) {
                                long batchSize = i == numberOfBatches - 1 ? lastBatchSize : channel.getMaxBatchSize();
                                // needs to grab the start and end batch id
                                endBatchId = insertReloadEvent(transaction, targetNode, triggerRouter, triggerHistory, selectSql, true,
                                        loadId, createBy, Status.RQ, null, batchSize);
                                if (startBatchId == -1) {
                                    startBatchId = endBatchId;
                                }
                            }

                            engine.getDataExtractorService().requestExtractRequest(transaction, targetNode.getNodeId(), channel.getQueue(),
                                    triggerRouter, startBatchId, endBatchId);
                        } else {
                            log.warn("The table defined by trigger_hist row %d no longer exists.  A load will not be queue'd up for the table", triggerHistory.getTriggerHistoryId());
                            
                        }
                    } else {
                        insertReloadEvent(transaction, targetNode, triggerRouter, triggerHistory,
                                selectSql, true, loadId, createBy, Status.NE, null, -1);
                    }

                    if (!transactional) {
                        transaction.commit();
                    }
                }
                
            }
        }
    }
    
    protected long getDataCountForReload(Table table, Node targetNode, String selectSql) {
        long rowCount = -1;
        if (parameterService.is(ParameterConstants.INITIAL_LOAD_USE_ESTIMATED_COUNTS) &&
                (selectSql == null || StringUtils.isBlank(selectSql) || selectSql.replace(" ", "").equals("1=1"))) {
            rowCount = platform.getEstimatedRowCount(table);
        } 
        
        if (rowCount < 0) {
            DatabaseInfo dbInfo = platform.getDatabaseInfo();
            String quote = dbInfo.getDelimiterToken();
            String catalogSeparator = dbInfo.getCatalogSeparator();
            String schemaSeparator = dbInfo.getSchemaSeparator();
                                              
            String sql = String.format("select count(*) from %s t where %s", table
                    .getQualifiedTableName(quote, catalogSeparator, schemaSeparator), selectSql);
            sql = FormatUtils.replace("groupId", targetNode.getNodeGroupId(), sql);
            sql = FormatUtils.replace("externalId", targetNode.getExternalId(), sql);
            sql = FormatUtils.replace("nodeId", targetNode.getNodeId(), sql);
            for (IReloadVariableFilter filter : extensionService.getExtensionPointList(IReloadVariableFilter.class)) {
                sql = filter.filterPurgeSql(sql, targetNode, table);
            }
            
            try {            
                rowCount = sqlTemplate.queryForLong(sql);
            } catch (Exception ex) {
                throw new SymmetricException("Failed to execute row count SQL while starting reload.  If this is a syntax error, check your input and check "
                        +  engine.getTablePrefix() + "_table_reload_request. Statement attempted: \"" + sql + "\"", ex);
            }
        }
        return rowCount;
    }

    protected int getTransformMultiplier(Table table, TriggerRouter triggerRouter) {
        int transformMultiplier = 0;
        List<TransformTableNodeGroupLink> transforms = engine.getTransformService()
                .findTransformsFor(triggerRouter.getRouter().getNodeGroupLink(), TransformPoint.EXTRACT);
        if (transforms != null) {
            for (TransformService.TransformTableNodeGroupLink transform : transforms) {
                if (transform.getSourceTableName().equals(table.getName())) {
                    transformMultiplier++;
                }
            }
        }
        transformMultiplier = Math.max(1, transformMultiplier);
        return transformMultiplier;
    }

    private void insertFileSyncBatchForReload(Node targetNode, long loadId, String createBy,
            boolean transactional, ISqlTransaction transaction, Map<String, TableReloadRequest> reloadRequests, boolean isFullLoad, ProcessInfo processInfo) {
        if (parameterService.is(ParameterConstants.FILE_SYNC_ENABLE)
                && !Constants.DEPLOYMENT_TYPE_REST.equals(targetNode.getDeploymentType())) {
            ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
            IFileSyncService fileSyncService = engine.getFileSyncService();
            if (fileSyncService.getFileTriggerRoutersForCurrentNode(false).size() > 0) {
                TriggerHistory fileSyncSnapshotHistory = triggerRouterService.findTriggerHistory(
                        null, null,
                        TableConstants.getTableName(tablePrefix, TableConstants.SYM_FILE_SNAPSHOT));
                String routerid = triggerRouterService.buildSymmetricTableRouterId(
                        fileSyncSnapshotHistory.getTriggerId(), parameterService.getNodeGroupId(),
                        targetNode.getNodeGroupId());
                TriggerRouter fileSyncSnapshotTriggerRouter = triggerRouterService
                        .getTriggerRouterForCurrentNode(fileSyncSnapshotHistory.getTriggerId(),
                                routerid, true);

                if(!isFullLoad && reloadRequests != null && reloadRequests.get(fileSyncSnapshotTriggerRouter.getTriggerId() + fileSyncSnapshotTriggerRouter.getRouterId()) == null){
                    return;
                }
                
                List<TriggerHistory> triggerHistories = Arrays.asList(fileSyncSnapshotHistory);
                List<TriggerRouter> triggerRouters = Arrays.asList(fileSyncSnapshotTriggerRouter);
                Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId = new HashMap<Integer, List<TriggerRouter>>();
                triggerRoutersByHistoryId.put(fileSyncSnapshotHistory.getTriggerHistoryId(), triggerRouters);
                
                if (parameterService.is(ParameterConstants.INITIAL_LOAD_USE_EXTRACT_JOB)) {      
                    
                    final String FILTER_ENABLED_FILE_SYNC_TRIGGER_ROUTERS = 
                            String.format("1=(select initial_load_enabled from %s tr where t.trigger_id = tr.trigger_id AND t.router_id = tr.router_id)",
                                    TableConstants.getTableName(tablePrefix, TableConstants.SYM_FILE_TRIGGER_ROUTER));
                    insertLoadBatchesForReload(targetNode, loadId, createBy, triggerHistories, 
                            triggerRoutersByHistoryId, transactional, transaction, null, processInfo, FILTER_ENABLED_FILE_SYNC_TRIGGER_ROUTERS);
                } else {                    
                    List<Channel> channels = engine.getConfigurationService().getFileSyncChannels();
                    for (Channel channel : channels) {
                        if (channel.isReloadFlag()) {
                            insertReloadEvent(transaction, targetNode, fileSyncSnapshotTriggerRouter,
                                    fileSyncSnapshotHistory,
                                    "reload_channel_id='" + channel.getChannelId() + "'", true, loadId,
                                    createBy, Status.NE, channel.getChannelId(), -1);
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
        
        if (StringUtils.isNotBlank(overrideDeleteStatement)) {
            List<String> sqlStatements = resolveTargetTables(overrideDeleteStatement, triggerRouter, triggerHistory, targetNode);
            for (String sql : sqlStatements) {
                createPurgeEvent(transaction, sql, targetNode, sourceNode,
                        triggerRouter, triggerHistory, isLoad, loadId, createBy);
            }
            
        } else if (transforms != null && transforms.size() > 0) {
            List<String> sqlStatements = symmetricDialect.createPurgeSqlForMultipleTables(targetNode, triggerRouter, 
                    triggerHistory, transforms, null);
            for (String sql : sqlStatements) {
                createPurgeEvent(transaction, 
                        sql,
                        targetNode, sourceNode,
                        triggerRouter, triggerHistory, isLoad, loadId, createBy);
            }
        } else {
            createPurgeEvent(transaction, 
                symmetricDialect.createPurgeSqlFor(targetNode, triggerRouter, triggerHistory, transforms),
                targetNode, sourceNode,
                triggerRouter, triggerHistory, isLoad, loadId, createBy);
        }
        
    }
    
    public List<String> resolveTargetTables(String sql, TriggerRouter triggerRouter, TriggerHistory triggerHistory, Node targetNode) {
        if (sql == null) { return null; }
        
        List<String> sqlStatements = new ArrayList<String>();                  
        if (sql != null && sql.contains("%s")) {
            Set<String> tableNames = new HashSet<String>();
            Node sourceNode = engine.getNodeService().findIdentity();
            String sourceTableName = triggerRouter.qualifiedTargetTableName(triggerHistory);
            
            List<TransformTableNodeGroupLink> transforms = 
                    this.engine.getTransformService().findTransformsFor(
                            sourceNode.getNodeGroupId(), targetNode.getNodeGroupId(), triggerRouter.getTargetTable(triggerHistory));
            
            if (transforms != null) {
                for (TransformTableNodeGroupLink transform : transforms) {
                    tableNames.add(transform.getFullyQualifiedTargetTableName());
                }
            } else {
                tableNames.add(sourceTableName);
            }
            
            for (String tableName : tableNames) {
                sqlStatements.add(String.format(sql, tableName));
            }
        }
        else {
            sqlStatements.add(sql);
        }
        return sqlStatements;
        
    }

    protected void createPurgeEvent(ISqlTransaction transaction, String sql, Node targetNode, Node sourceNode,
            TriggerRouter triggerRouter, TriggerHistory triggerHistory, boolean isLoad, 
            long loadId, String createBy) {
        
        sql = FormatUtils.replace("groupId", targetNode.getNodeGroupId(), sql);
        sql = FormatUtils.replace("externalId", targetNode.getExternalId(), sql);
        sql = FormatUtils.replace("nodeId", targetNode.getNodeId(), sql);
        sql = FormatUtils.replace("targetGroupId", targetNode.getNodeGroupId(), sql);
        sql = FormatUtils.replace("targetExternalId", targetNode.getExternalId(), sql);
        sql = FormatUtils.replace("targetNodeId", targetNode.getNodeId(), sql);
        sql = FormatUtils.replace("sourceGroupId", sourceNode.getNodeGroupId(), sql);
        sql = FormatUtils.replace("sourceExternalId", sourceNode.getExternalId(), sql);
        sql = FormatUtils.replace("sourceNodeId", sourceNode.getNodeId(), sql);
        Table table = new Table(triggerHistory.getSourceCatalogName(), triggerHistory.getSourceSchemaName(),
                triggerHistory.getSourceTableName(), triggerHistory.getParsedColumnNames(), triggerHistory.getParsedPkColumnNames());
        for (IReloadVariableFilter filter : extensionService.getExtensionPointList(IReloadVariableFilter.class)) {
            sql = filter.filterPurgeSql(sql, targetNode, table);
        }

        String channelId = getReloadChannelIdForTrigger(triggerRouter.getTrigger(), engine
                .getConfigurationService().getChannels(false));
        Data data = new Data(triggerHistory.getSourceTableName(), DataEventType.SQL,
                CsvUtils.escapeCsvData(sql), null, triggerHistory, channelId, null, null);
        data.setNodeList(targetNode.getNodeId());
        if (isLoad) {
            insertDataAndDataEventAndOutgoingBatch(transaction, data, targetNode.getNodeId(),
                    triggerRouter.getRouter().getRouterId(), isLoad, loadId, createBy, Status.NE, null, -1);
        } else {
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
        data.setNodeList(targetNode.getNodeId());
        if (isLoad) {
            insertDataAndDataEventAndOutgoingBatch(data, targetNode.getNodeId(),
                    Constants.UNKNOWN_ROUTER_ID, isLoad, loadId, createBy);
        } else {
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

    public void insertSqlEvent(ISqlTransaction transaction, TriggerHistory history,
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
                    Constants.UNKNOWN_ROUTER_ID, isLoad, loadId, createBy, Status.NE, null, -1);
        } else {
            insertData(transaction, data);
        }
    }

    public void insertScriptEvent(String channelId, Node targetNode, String script, boolean isLoad,
            long loadId, String createBy) {
        ISqlTransaction transaction = null;
        try {
            transaction = platform.getSqlTemplate().startSqlTransaction();
            insertScriptEvent(transaction, channelId, targetNode, script, isLoad, loadId, createBy);
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
                    Constants.UNKNOWN_ROUTER_ID, isLoad, loadId, createBy, Status.NE, null, -1);
        } else {
            insertData(transaction, data);
        }
    }

    public int countDataInRange(long firstDataId, long secondDataId) {
        return sqlTemplate.queryForInt(getSql("countDataInRangeSql"), firstDataId, secondDataId);
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
        insertCreateEvent(transaction, targetNode, triggerHistory, isLoad ? reloadChannelId
                : Constants.CHANNEL_CONFIG, routerId, isLoad, loadId, createBy);
    }
    
    public void insertCreateEvent(ISqlTransaction transaction, Node targetNode,
            TriggerHistory triggerHistory, String channelId, String routerId, boolean isLoad, long loadId, String createBy) {

        Data data = new Data(triggerHistory.getSourceTableName(), DataEventType.CREATE,
                null, null, triggerHistory, channelId, null, null);
        data.setNodeList(targetNode.getNodeId());
        try {
            if (isLoad) {
                insertDataAndDataEventAndOutgoingBatch(transaction, data, targetNode.getNodeId(),
                        routerId, isLoad, loadId, createBy, Status.NE, null, -1);
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
        insertDataEvent(transaction, dataEvent.getDataId(), dataEvent.getBatchId(),
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
            throw new RuntimeException(String.format("Could not insert a data event: data_id=%s batch_id=%s router_id=%s",
                    dataId, batchId, routerId ), ex);
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
                        data.getDataEventType(), routerId, isLoad, loadId, createBy, Status.NE, data.getTableName(), -1);
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
                    isLoad, loadId, createBy, Status.NE, null, -1);
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
     * @param estimatedBatchRowCount TODO
     * @return The inserted batch id
     */
    public long insertDataAndDataEventAndOutgoingBatch(ISqlTransaction transaction, Data data, String nodeId, String routerId, boolean isLoad,
            long loadId, String createBy, Status status, String overrideChannelId, long estimatedBatchRowCount) {
        long dataId = insertData(transaction, data);
        String channelId = null;
        if (isLoad) {
            if (overrideChannelId != null) {
                channelId = overrideChannelId;
            } else {
                TriggerHistory history = data.getTriggerHistory();
                if (history != null && channelId == null) {
                    Trigger trigger = engine.getTriggerRouterService().getTriggerById(history.getTriggerId(), false);
                    channelId = getReloadChannelIdForTrigger(trigger, engine.getConfigurationService().getChannels(false));
                }
            }
        } else {
            channelId = data.getChannelId();
        }
        return insertDataEventAndOutgoingBatch(transaction, dataId, channelId, nodeId, data.getDataEventType(), routerId, isLoad, loadId,
                createBy, status, data.getTableName(), estimatedBatchRowCount);
    }
    
    public long insertDataAndDataEventAndOutgoingBatch(ISqlTransaction transaction, Data data,
            String nodeId, String routerId, boolean isLoad, long loadId, String createBy,
            Status status, long estimatedBatchRowCount) {
    	    return insertDataAndDataEventAndOutgoingBatch(transaction, data, nodeId, routerId, isLoad, loadId, createBy, status, null, estimatedBatchRowCount);
    }

    protected long insertDataEventAndOutgoingBatch(ISqlTransaction transaction, long dataId,
            String channelId, String nodeId, DataEventType eventType, String routerId,
            boolean isLoad, long loadId, String createBy, Status status, String tableName, long estimatedBatchRowCount) {
        OutgoingBatch outgoingBatch = new OutgoingBatch(nodeId, channelId, status);
        outgoingBatch.setLoadId(loadId);
        outgoingBatch.setCreateBy(createBy);
        outgoingBatch.setLoadFlag(isLoad);
        outgoingBatch.incrementRowCount(eventType);
        if (estimatedBatchRowCount > 0) {
            outgoingBatch.setDataRowCount(estimatedBatchRowCount);
        } else {
            outgoingBatch.incrementDataRowCount();
        }
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
                    Constants.UNKNOWN_ROUTER_ID, isLoad, loadId, createBy, Status.NE, channelId, -1);
        } else {
            throw new SymmetricException(String.format("Unable to issue an update for %s_node_security. " + 
                    " Check the %s_trigger_hist for %s_node_security.", tablePrefix, tablePrefix,  tablePrefix ));
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
            log.error("Could not send schema to the node {}.  The target node does not exist", nodeId);
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
                        String channelId = getReloadChannelIdForTrigger(triggerRouter.getTrigger(), engine
                                .getConfigurationService().getChannels(false));
                        
                        insertReloadEvent(transaction, targetNode, triggerRouter, triggerHistory,
                                overrideInitialLoadSelect, false, -1, "reloadTable", Status.NE, channelId, -1);
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

    public void reloadMissingForeignKeyRowsReverse(String sourceNodeId, Table table, CsvData data, boolean sendCorrectionToPeers) {
        try {
            Map<String, String> dataMap = data.toColumnNameValuePairs(table.getColumnNames(), CsvData.ROW_DATA);
            List<TableRow> tableRows = new ArrayList<TableRow>();
            Row row = new Row(dataMap.size());
            row.putAll(dataMap);
            
            Table localTable = platform.getTableFromCache(table.getCatalog(), table.getSchema(), table.getName(), false);
            if (localTable == null) {
                log.info("Could not find table " + table.getFullyQualifiedTableName());
            }
            tableRows.add(new TableRow(localTable, row, null, null, null));
            List<TableRow> foreignTableRows;
            try {
                foreignTableRows = getForeignTableRows(tableRows, new HashSet<TableRow>());
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }

            if (foreignTableRows.isEmpty()) {
                log.info("Could not determine foreign table rows to fix foreign key violation for "
                        + "nodeId '{}' table '{}'", sourceNodeId, localTable.getName());
            }

            Collections.reverse(foreignTableRows);
            Set<TableRow> visited = new HashSet<TableRow>();
            Node sourceNode = engine.getNodeService().findNode(sourceNodeId);
            Node identity = engine.getNodeService().findIdentity();
            StringBuilder script = new StringBuilder();
            List<Node> targetNodes = new ArrayList<Node>();
            targetNodes.add(identity);
            
            if (sendCorrectionToPeers) {
                targetNodes.addAll(engine.getNodeService().findEnabledNodesFromNodeGroup(sourceNode.getNodeGroupId()));
                targetNodes.remove(sourceNode);
            }

            for (TableRow foreignTableRow : foreignTableRows) {
                if (visited.add(foreignTableRow)) {
                    Table foreignTable = foreignTableRow.getTable();
                    String catalog = foreignTable.getCatalog();
                    String schema = foreignTable.getSchema();
                    if (StringUtils.equals(platform.getDefaultCatalog(), catalog)) {
                        catalog = null;
                    }
                    if (StringUtils.equals(platform.getDefaultSchema(), schema)) {
                        schema = null;
                    }

                    log.info(
                            "Requesting foreign key correction reload "
                                    + "nodeId {} catalog '{}' schema '{}' foreign table name '{}' fk name '{}' where sql '{}' "
                                    + "to correct table '{}' for column '{}'",
                            sourceNodeId, catalog, schema, foreignTable.getName(), foreignTableRow.getFkName(),
                            foreignTableRow.getWhereSql(), localTable.getName(), foreignTableRow.getReferenceColumnName());
             
                    for (Node targetNode : targetNodes) {
                        script.append("engine.getDataService().reloadTable(\"" + targetNode.getNodeId() + "\", " +
                                ((schema == null) ? schema : "\"" + schema + "\"") + ", " +
                                ((catalog == null) ? catalog : "\"" + catalog + "\"") + ", \"" +
                                foreignTable.getName().replace("\"", "\\\"") + "\", \"" +
                                foreignTableRow.getWhereSql().replace("\"", "\\\"") + "\");\n");
                    }
                }
            }

            if (script.length() > 0) {
                insertScriptEvent("config", sourceNode, script.toString(), false, -1, "fk");
            }
        } catch (Exception e) {
            log.error("Unknown exception while processing foreign key for node id: " + sourceNodeId, e);
        }
    }

    public void reloadMissingForeignKeyRows(String nodeId, long dataId) {
        try {
            Data data = findData(dataId);
            log.debug("reloadMissingForeignKeyRows for nodeId '{}' dataId '{}' table '{}'", nodeId, dataId, data.getTableName());
            TriggerHistory hist = data.getTriggerHistory();
            Table table = platform.getTableFromCache(hist.getSourceCatalogName(), hist.getSourceSchemaName(), hist.getSourceTableName(), false);
            table = table.copyAndFilterColumns(hist.getParsedColumnNames(), hist.getParsedPkColumnNames(), true);
            Map<String, String> dataMap = data.toColumnNameValuePairs(table.getColumnNames(), CsvData.ROW_DATA);
    
            List<TableRow> tableRows = new ArrayList<TableRow>();
            Row row = new Row(dataMap.size());
            row.putAll(dataMap);
            tableRows.add(new TableRow(table, row, null, null, null));
            List<TableRow> foreignTableRows;
            try {
                foreignTableRows = getForeignTableRows(tableRows, new HashSet<TableRow>());
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
            
            if (foreignTableRows.isEmpty()) {
                log.info("Could not determine foreign table rows to fix foreign key violation for "
                        + "nodeId '{}' dataId '{}' table '{}'", nodeId, dataId, data.getTableName());
            }
            
            Collections.reverse(foreignTableRows);
            Set<TableRow> visited = new HashSet<TableRow>();
            
            for (TableRow foreignTableRow : foreignTableRows) {
                if (visited.add(foreignTableRow)) {
                    Table foreignTable = foreignTableRow.getTable();
                    String catalog = foreignTable.getCatalog();
                    String schema = foreignTable.getSchema();
                    if (StringUtils.equals(platform.getDefaultCatalog(), catalog)) {
                        catalog = null;
                    }
                    if (StringUtils.equals(platform.getDefaultSchema(), schema)) {
                        schema = null;
                    }
                
                    log.info("Issuing foreign key correction reload "
                            + "nodeId {} catalog '{}' schema '{}' foreign table name '{}' fk name '{}' where sql '{}' "
                            + "to correct dataId '{}' table '{}' for column '{}'",
                            nodeId, catalog, schema, foreignTable.getName(), foreignTableRow.getFkName(), foreignTableRow.getWhereSql(), 
                            dataId, data.getTableName(), foreignTableRow.getReferenceColumnName());
                    reloadTable(nodeId, catalog, schema, foreignTable.getName(), foreignTableRow.getWhereSql());
                }
            }        
        }
        catch (Exception e) {
            log.error("Unknown exception while processing foreign key for node id: " + nodeId + " data id " + dataId, e);
        }
    }

    protected List<TableRow> getForeignTableRows(List<TableRow> tableRows, Set<TableRow> visited) throws CloneNotSupportedException {
        List<TableRow> fkDepList = new ArrayList<TableRow>();
        for (TableRow tableRow : tableRows) {
            if (!visited.contains(tableRow)) {
                visited.add(tableRow);
                for (ForeignKey fk : tableRow.getTable().getForeignKeys()) {
                    Table table = platform.getTableFromCache(fk.getForeignTableName(), false);
                    if (table == null) {
                        table = fk.getForeignTable();
                        if (table == null) {
                            table = platform.getTableFromCache(tableRow.getTable().getCatalog(), tableRow.getTable().getSchema(),
                                    fk.getForeignTableName(), false);
                        }
                    }
                    if (table != null) {
                        Table foreignTable = (Table) table.clone();
                        for (Column column : foreignTable.getColumns()) {
                            column.setPrimaryKey(false);
                        }
                        Row whereRow = new Row(fk.getReferenceCount());
                        String referenceColumnName = null;
                        boolean[] nullValues = new boolean[fk.getReferenceCount()];
                        int index = 0;
                        for (Reference ref : fk.getReferences()) {
                            Column foreignColumn = foreignTable.findColumn(ref.getForeignColumnName());
                            Object value = tableRow.getRow().get(ref.getLocalColumnName());
                            nullValues[index++] = value == null;
                            referenceColumnName = ref.getLocalColumnName();
                            whereRow.put(foreignColumn.getName(), value);
                            foreignColumn.setPrimaryKey(true);
                        }

                        boolean allNullValues = true;
                        for (boolean b : nullValues) {
                            if (!b) {
                                allNullValues = false;
                                break;
                            }
                        }

                        if (!allNullValues) {
                            DmlStatement whereSt = platform.createDmlStatement(DmlType.WHERE, foreignTable.getCatalog(),
                                    foreignTable.getSchema(), foreignTable.getName(), foreignTable.getPrimaryKeyColumns(),
                                    foreignTable.getColumns(), nullValues, null);
                            String whereSql = whereSt.buildDynamicSql(symmetricDialect.getBinaryEncoding(), whereRow, false, true,
                                    foreignTable.getPrimaryKeyColumns()).substring(6);
                            String delimiter = platform.getDatabaseInfo().getSqlCommandDelimiter();
                            if (delimiter != null && delimiter.length() > 0) {
                                whereSql = whereSql.substring(0, whereSql.length() - delimiter.length());
                            }

                            Row foreignRow = new Row(foreignTable.getColumnCount());
                            if (foreignTable.getForeignKeyCount() > 0) {
                                DmlStatement selectSt = platform.createDmlStatement(DmlType.SELECT, foreignTable, null);
                                Object[] keys = whereRow.toArray(foreignTable.getPrimaryKeyColumnNames());
                                Map<String, Object> values = sqlTemplate.queryForMap(selectSt.getSql(), keys);
                                if (values == null) {
                                    log.warn(
                                            "Unable to reload rows for missing foreign key data for table '{}', parent data not found.  Using sql='{}' with keys '{}'",
                                            table.getName(), selectSt.getSql(), keys);
                                } else {
                                    foreignRow.putAll(values);
                                }
                            }

                            TableRow foreignTableRow = new TableRow(foreignTable, foreignRow, whereSql, referenceColumnName, fk.getName());
                            fkDepList.add(foreignTableRow);
                            log.debug("Add foreign table reference '{}' whereSql='{}'", foreignTable.getName(), whereSql);
                        } else {
                            log.debug("The foreign table reference was null for {}", foreignTable.getName());
                        }
                    } else {
                        log.debug("Foreign table '{}' not found for foreign key '{}'", fk.getForeignTableName(), fk.getName());
                    }
                    if (fkDepList.size() > 0) {
                        fkDepList.addAll(getForeignTableRows(fkDepList, visited));
                    }
                }
            }
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
        long lastGapStartId = 0;
        for (DataGap dataGap : gaps) {
            lastGapExists |= dataGap.gapSize() >= maxDataToSelect - 1;
            lastGapStartId = Math.max(lastGapStartId, dataGap.getEndId());
        }

        if (!lastGapExists) {
            if (lastGapStartId == 0) {
                long maxRoutedDataId = findMaxDataEventDataId();
                long minDataId = findMinDataId() - 1; // -1 to make sure the ++ operation doesn't move past a piece of unrouted data.
                // At this point, determine the startId as the GREATER of the smallest known data id 
                // or the largest known data id that was already routed.
                lastGapStartId = Math.max(minDataId, maxRoutedDataId); 
            }
            if (lastGapStartId > -1) {
                lastGapStartId++;
            }
            DataGap gap = new DataGap(lastGapStartId, lastGapStartId + maxDataToSelect);
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
        return sqlTemplateDirty.query(getSql("selectEventDataIdsSql", getDataOrderBy()),
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
                getSql("selectEventDataByBatchIdSql", startAtDataIdSql, getDataOrderBy()),
                engine.getConfigurationService().getNodeChannel(channelId, false).getChannel());
    }

    protected String getDataSelectSql(long batchId, long startDataId, String channelId) {
        String startAtDataIdSql = startDataId >= 0l ? " and d.data_id >= ? " : "";
        return symmetricDialect.massageDataExtractionSql(
                getSql("selectEventDataToExtractSql", startAtDataIdSql, getDataOrderBy()),
                engine.getConfigurationService().getNodeChannel(channelId, false).getChannel());
    }

    protected String getDataOrderBy() {
        String orderBy = "";
        if (parameterService.is(ParameterConstants.DBDIALECT_ORACLE_SEQUENCE_NOORDER, false)) {
            orderBy = " order by d.create_time asc, d.data_id asc";
        } else if (parameterService.is(ParameterConstants.ROUTING_DATA_READER_ORDER_BY_DATA_ID_ENABLED, true)) {
            orderBy = " order by d.data_id asc";
        }
        return orderBy;
    }

    public long findMaxDataId() {
        return sqlTemplateDirty.queryForLong(getSql("selectMaxDataIdSql"));
    }
    
    public long findMinDataId() {
        return sqlTemplateDirty.queryForLong(getSql("selectMinDataIdSql"));
    }
    
    
    @Override
    public void deleteCapturedConfigChannelData() {
        int count = sqlTemplate.update(getSql("deleteCapturedConfigChannelDataSql"));
        if (count > 0) {
            log.info("Deleted {} data rows that were on the config channel", count);
        }
    }

    @Override
    public Map<String, Date> getLastDataCaptureByChannel() {
        Map<String, Date> captureMap = new HashMap<String, Date>();
        LastCaptureByChannelMapper mapper = new LastCaptureByChannelMapper(captureMap);
        sqlTemplate.query(getSql("findLastCaptureTimeByChannelSql"), mapper);
        return mapper.getCaptureMap();
    }
    
    @Override
    public boolean fixLastDataGap() {
        boolean fixed = false;
        long maxDataId = findMaxDataId();
        List<DataGap> gaps = findDataGaps();
        if (gaps.size() > 0) {
            DataGap lastGap = gaps.get(gaps.size()-1);
            if (lastGap.getEndId() < maxDataId) {
                fixed = true;
                log.warn("The last data id of {} was bigger than the last gap's end_id of {}.  Increasing the gap size", maxDataId, lastGap.getEndId());
                final long maxDataToSelect = parameterService
                        .getLong(ParameterConstants.ROUTING_LARGEST_GAP_SIZE);
                ISqlTransaction transaction = null;
                try {
                    transaction = sqlTemplate.startSqlTransaction();
                    deleteDataGap(transaction, lastGap);
                    insertDataGap(transaction, new DataGap(lastGap.getStartId(), maxDataId+maxDataToSelect));
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
        }
        return fixed;
    }
    
    static class TableRow {
        Table table;
        Row row;
        String whereSql;
        String referenceColumnName;
        String fkName;
        String fkColumnValues = null;
        
        public TableRow(Table table, Row row, String whereSql, String referenceColumnName, String fkName) {
            this.table = table;
            this.row = row;
            this.whereSql = whereSql;
            this.referenceColumnName = referenceColumnName;
            this.fkName = fkName;
        }
        
        protected String getFkColumnValues() {
            if (fkColumnValues == null) {
                StringBuilder builder = new StringBuilder();
                ForeignKey[] keys = table.getForeignKeys();
                for (ForeignKey foreignKey : keys) {
                    if (foreignKey.getName().equals(fkName)) {
                        Reference[] refs = foreignKey.getReferences();
                        for (Reference ref : refs) {
                            Object value = row.get(ref.getLocalColumnName());
                            if (value != null) {
                                builder.append("\"").append(value).append("\",");
                            } else {
                                builder.append("null,");
                            }
                        }
                    }
                }
                fkColumnValues = builder.toString();
            }
            return fkColumnValues;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((table == null) ? 0 : table.hashCode());
            result = prime * result + ((whereSql == null) ? 0 : whereSql.hashCode());
            result = prime * result + ((getFkColumnValues() == null) ? 0 : getFkColumnValues().hashCode());
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof TableRow) {
                TableRow tr = (TableRow) o;
                return tr.table.equals(table) && tr.whereSql.equals(whereSql) 
                        && tr.getFkColumnValues().equals(getFkColumnValues().toString());
            }
            return false;
        }
        
        @Override
        public String toString() {
            return table.getFullyQualifiedTableName() + ":" + whereSql + ":" + getFkColumnValues();
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
        public String getReferenceColumnName() {
            return referenceColumnName;
        }
        public String getFkName() {
            return fkName;
        }
                        
    }

    public class DataMapper implements ISqlRowMapper<Data> {
        public Data mapRow(Row row) {
            Data data = new Data();
            String rowData = row.getString("ROW_DATA", false);
            data.putCsvData(CsvData.ROW_DATA, isNotBlank(rowData) ? rowData : null);
            String pkData = row.getString("PK_DATA", false);
            data.putCsvData(CsvData.PK_DATA, isNotBlank(pkData) ? pkData : null);
            String oldData = row.getString("OLD_DATA", false);
            data.putCsvData(CsvData.OLD_DATA, isNotBlank(oldData) ? oldData : null);
            data.putAttribute(CsvData.ATTRIBUTE_CHANNEL_ID, row.getString("CHANNEL_ID"));
            data.putAttribute(CsvData.ATTRIBUTE_TX_ID, row.getString("TRANSACTION_ID", false));
            String tableName = row.getString("TABLE_NAME");
            data.putAttribute(CsvData.ATTRIBUTE_TABLE_NAME, tableName);
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
                Trigger trigger = null;
                Table table = null;
                List<TriggerRouter> triggerRouters = engine.getTriggerRouterService().getAllTriggerRoutersForCurrentNode(engine.getNodeService().findIdentity().getNodeGroupId());
                for (TriggerRouter triggerRouter : triggerRouters) {
                    if (triggerRouter.getTrigger().getSourceTableName().equalsIgnoreCase(tableName)) {
                        trigger = triggerRouter.getTrigger();
                        table = platform.getTableFromCache(trigger.getSourceCatalogName(), trigger.getSourceSchemaName(), tableName, false);
                        break;
                    }
                }
                
                if (table != null && trigger != null) {
                    List<TriggerHistory> activeTriggerHistories = engine.getTriggerRouterService().getActiveTriggerHistories();
                    triggerHistory = new TriggerHistory(table, trigger, engine.getSymmetricDialect().getTriggerTemplate());
                    triggerHistory.setTriggerHistoryId(triggerHistId);
                    triggerHistory.setLastTriggerBuildReason(TriggerReBuildReason.TRIGGER_HIST_MISSIG);
                    triggerHistory.setNameForInsertTrigger(engine.getTriggerRouterService().getTriggerName(DataEventType.INSERT,
                            symmetricDialect.getMaxTriggerNameLength(), trigger, table, activeTriggerHistories, null));
                    triggerHistory.setNameForUpdateTrigger(engine.getTriggerRouterService().getTriggerName(DataEventType.UPDATE,
                            symmetricDialect.getMaxTriggerNameLength(), trigger, table, activeTriggerHistories, null));
                    triggerHistory.setNameForDeleteTrigger(engine.getTriggerRouterService().getTriggerName(DataEventType.DELETE,
                            symmetricDialect.getMaxTriggerNameLength(), trigger, table, activeTriggerHistories, null));
                    engine.getTriggerRouterService().insert(triggerHistory);
                    log.warn("Could not find a trigger history row for the table {} for data_id {}.  \"Attempting\" to generate a new trigger history row", tableName, data.getDataId());
                } else {
                    triggerHistory = new TriggerHistory(-1);
                    log.warn("A captured data row could not be matched with an existing trigger history "
                            + "row and we could not find a matching trigger.  The data_id of {} (table {}) will be ignored", data.getDataId(), data.getTableName());
                }
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
    
    public static class LastCaptureByChannelMapper implements ISqlRowMapper<String> {
        private Map<String, Date> captureMap;
        
        public LastCaptureByChannelMapper(Map<String, Date> map) {
            captureMap = map;
        }
        
        public Map<String, Date> getCaptureMap() {
            return captureMap;
        }
        
        @Override
        public String mapRow(Row row) {
            captureMap.put(row.getString("CHANNEL_ID"), row.getDateTime("CREATE_TIME"));
            return null;
        }
    }
}
