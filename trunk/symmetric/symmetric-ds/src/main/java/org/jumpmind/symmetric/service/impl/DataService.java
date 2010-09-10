/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.Message;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ext.IHeartbeatListener;
import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEvent;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.DataRef;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.symmetric.util.CsvUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionCallback;


public class DataService extends AbstractService implements IDataService {

    private ITriggerRouterService triggerRouterService;

    private INodeService nodeService;

    private IPurgeService purgeService;

    private IConfigurationService configurationService;

    private IOutgoingBatchService outgoingBatchService;

    private List<IReloadListener> reloadListeners;

    private List<IHeartbeatListener> heartbeatListeners;

    protected Map<IHeartbeatListener, Long> lastHeartbeatTimestamps = new HashMap<IHeartbeatListener, Long>();

    @Transactional
    public void insertReloadEvent(final Node targetNode, final TriggerRouter triggerRouter) {
        insertReloadEvent(targetNode, triggerRouter, null);
    }

    @Transactional
    public void insertReloadEvent(final Node targetNode, final TriggerRouter triggerRouter,
            final String overrideInitialLoadSelect) {
        TriggerHistory history = lookupTriggerHistory(triggerRouter.getTrigger());
        // initial_load_select for table can be overridden by populating the
        // row_data
        Data data = new Data(history.getSourceTableName(), DataEventType.RELOAD,
                overrideInitialLoadSelect != null ? overrideInitialLoadSelect : triggerRouter
                        .getInitialLoadSelect(), null, history, triggerRouter.getTrigger().getChannelId(), null,
                null);
        insertDataAndDataEventAndOutgoingBatch(data, targetNode.getNodeId(), triggerRouter
                .getRouter().getRouterId(), true);
    }

    private TriggerHistory lookupTriggerHistory(Trigger trigger) {
        TriggerHistory history = triggerRouterService.getNewestTriggerHistoryForTrigger(trigger
                .getTriggerId());

        if (history == null) {
            throw new RuntimeException("Cannot find history for trigger " + trigger.getTriggerId()
                    + ", " + trigger.getSourceTableName());
        }
        return history;
    }

    public void insertPurgeEvent(final Node targetNode, final TriggerRouter triggerRouter, boolean isLoad) {
        String sql = dbDialect.createPurgeSqlFor(targetNode, triggerRouter);
        insertSqlEvent(targetNode, triggerRouter.getTrigger(), sql, isLoad);
    }

    public void insertSqlEvent(final Node targetNode, final Trigger trigger, String sql, boolean isLoad) {
        TriggerHistory history = triggerRouterService.getNewestTriggerHistoryForTrigger(trigger
                .getTriggerId());
        Data data = new Data(trigger.getSourceTableName(), DataEventType.SQL, CsvUtils
                .escapeCsvData(sql), null, history, trigger.getChannelId(), null, null);
        insertDataAndDataEventAndOutgoingBatch(data, targetNode.getNodeId(),
                Constants.UNKNOWN_ROUTER_ID, isLoad);
    }

    public void insertSqlEvent(final Node targetNode, String sql, boolean isLoad) {
        Data data = new Data(Constants.NA, DataEventType.SQL, CsvUtils.escapeCsvData(sql), null,
                null, Constants.CHANNEL_CONFIG, null, null);
        insertDataAndDataEventAndOutgoingBatch(data, targetNode.getNodeId(),
                Constants.UNKNOWN_ROUTER_ID, isLoad);
    }

    public int countDataInRange(long firstDataId, long secondDataId) {
        return jdbcTemplate.queryForInt(getSql("countDataInRangeSql"), firstDataId, secondDataId);
    }

    public void insertCreateEvent(final Node targetNode, final TriggerRouter triggerRouter,
            String xml, boolean isLoad) {
        TriggerHistory history = triggerRouterService
                .getNewestTriggerHistoryForTrigger(triggerRouter.getTrigger().getTriggerId());
        Data data = new Data(triggerRouter.getTrigger().getSourceTableName(), DataEventType.CREATE,
                CsvUtils.escapeCsvData(xml), null, history, 
                parameterService.is(ParameterConstants.INITIAL_LOAD_USE_RELOAD_CHANNEL) && isLoad ? Constants.CHANNEL_RELOAD : triggerRouter.getTrigger().getChannelId(), null, null);
        insertDataAndDataEventAndOutgoingBatch(data, targetNode.getNodeId(),
                Constants.UNKNOWN_ROUTER_ID, isLoad);
    }

    public long insertData(final Data data) {
        long id = dbDialect.insertWithGeneratedKey(getSql("insertIntoDataSql"),
                SequenceIdentifier.DATA, new PreparedStatementCallback<Object>() {
                    public Object doInPreparedStatement(PreparedStatement ps) throws SQLException,
                            DataAccessException {
                        ps.setString(1, data.getTableName());
                        ps.setString(2, data.getEventType().getCode());
                        ps.setString(3, data.getRowData());
                        ps.setString(4, data.getPkData());
                        ps.setString(5, data.getOldData());
                        ps.setLong(6, data.getTriggerHistory() != null ? data.getTriggerHistory()
                                .getTriggerHistoryId() : -1);
                        ps.setString(7, data.getChannelId());
                        return null;
                    }
                });
        data.setDataId(id);
        return id;
    }

    public void insertDataEvent(DataEvent dataEvent) {
        this.insertDataEvent(jdbcTemplate, dataEvent.getDataId(), dataEvent.getBatchId(), dataEvent
                .getRouterId());
    }

    public void insertDataEvent(long dataId, long batchId, String routerId) {
        this.insertDataEvent(jdbcTemplate, dataId, batchId, routerId);
    }

    public void insertDataEvent(JdbcTemplate template, long dataId, long batchId, String routerId) {
        try {
            template.update(getSql("insertIntoDataEventSql"), new Object[] { dataId, batchId,
                    StringUtils.isBlank(routerId) ? Constants.UNKNOWN_ROUTER_ID : routerId },
                    new int[] { Types.INTEGER, Types.INTEGER, Types.VARCHAR });
        } catch (RuntimeException ex) {
            log.error("DataEventInsertFailed", ex, dataId, batchId, routerId);
            throw ex;
        }
    }

    public void insertDataAndDataEventAndOutgoingBatch(Data data, String channelId,
            List<Node> nodes, String routerId, boolean isLoad) {
        long dataId = insertData(data);
        for (Node node : nodes) {
            insertDataEventAndOutgoingBatch(dataId, channelId, node.getNodeId(), data.getEventType(), routerId, isLoad);
        }
    }

    public void insertDataAndDataEventAndOutgoingBatch(Data data, String nodeId, String routerId, boolean isLoad) {
        long dataId = insertData(data);
        insertDataEventAndOutgoingBatch(dataId, data.getChannelId(), nodeId, data.getEventType(), routerId, isLoad);
    }

    public void insertDataEventAndOutgoingBatch(long dataId, String channelId, String nodeId, DataEventType eventType,
            String routerId, boolean isLoad) {
        OutgoingBatch outgoingBatch = new OutgoingBatch(nodeId, parameterService.is(ParameterConstants.INITIAL_LOAD_USE_RELOAD_CHANNEL) && isLoad ? Constants.CHANNEL_RELOAD : channelId);
        outgoingBatch.setLoadFlag(isLoad);
        outgoingBatch.incrementEventCount(eventType);
        outgoingBatchService.insertOutgoingBatch(outgoingBatch);
        insertDataEvent(new DataEvent(dataId, outgoingBatch.getBatchId(), routerId));
    }

    public String reloadNode(String nodeId) {
        Node targetNode = nodeService.findNode(nodeId);
        if (targetNode == null) {
            return Message.get("NodeUnknown", nodeId);
        }
        if (nodeService.setInitialLoadEnabled(nodeId, true)) {
            return Message.get("NodeInitialLoadOpened", nodeId);
        } else {
            return Message.get("NodeInitialLoadFailed", nodeId);
        }
    }

    public void insertReloadEvents(Node targetNode) {
        
        // outgoing data events are pointless because we are reloading all data
        outgoingBatchService.markAllAsSentForNode(targetNode);
        
        if (parameterService.is(ParameterConstants.DATA_RELOAD_IS_BATCH_INSERT_TRANSACTIONAL)) {
            newTransactionTemplate.execute(new TransactionalInsertReloadEventsDelegate(targetNode));
        } else {
            new TransactionalInsertReloadEventsDelegate(targetNode).doInTransaction(null);
        }
        
        // remove all incoming events from the node are starting a reload for.
        purgeService.purgeAllIncomingEventsForNode(targetNode.getNodeId());
        
    }
    
    class TransactionalInsertReloadEventsDelegate implements TransactionCallback<Object> {

        Node targetNode;

        public TransactionalInsertReloadEventsDelegate(Node targetNode) {
            this.targetNode = targetNode;
        }

        public Object doInTransaction(TransactionStatus status) {

            Node sourceNode = nodeService.findIdentity();
            
            if (reloadListeners != null) {
                for (IReloadListener listener : reloadListeners) {
                    listener.beforeReload(targetNode);
                }
            }

            // insert node security so the client doing the initial load knows
            // that an initial load is currently happening
            insertNodeSecurityUpdate(targetNode, true);

            List<TriggerRouter> triggerRouters = new ArrayList<TriggerRouter>(triggerRouterService
                    .getAllTriggerRoutersForReloadForCurrentNode(sourceNode.getNodeGroupId(),
                            targetNode.getNodeGroupId()));
            
            for (Iterator<TriggerRouter> iterator = triggerRouters.iterator(); iterator.hasNext();) {
                TriggerRouter triggerRouter = iterator.next();
                Trigger trigger = triggerRouter.getTrigger();
                Table table = dbDialect.getTable(trigger.getSourceCatalogName(), trigger
                        .getSourceSchemaName(), trigger.getSourceTableName(), false);
                if (table == null) {
                    log.warn("TriggerTableMissing",trigger.qualifiedSourceTableName());
                    iterator.remove();
                }
            }

            if (parameterService.is(ParameterConstants.INITIAL_LOAD_CREATE_SCHEMA_BEFORE_RELOAD)) {
                for (TriggerRouter triggerRouter : triggerRouters) {
                    String xml = dbDialect.getCreateTableXML(triggerRouter);
                    insertCreateEvent(targetNode, triggerRouter, xml, true);
                }
            }

            if (parameterService.is(ParameterConstants.INITIAL_LOAD_DELETE_BEFORE_RELOAD)) {
                for (ListIterator<TriggerRouter> iterator = triggerRouters
                        .listIterator(triggerRouters.size()); iterator.hasPrevious();) {
                    TriggerRouter triggerRouter = iterator.previous();
                    insertPurgeEvent(targetNode, triggerRouter, true);
                }
            }

            for (TriggerRouter trigger : triggerRouters) {
                insertReloadEvent(targetNode, trigger);
            }

            if (reloadListeners != null) {
                for (IReloadListener listener : reloadListeners) {
                    listener.afterReload(targetNode);
                }
            }

            nodeService.setInitialLoadEnabled(targetNode.getNodeId(), false);

            // don't mark this batch as a load batch so it is forced to go last
            insertNodeSecurityUpdate(targetNode,
                    parameterService.is(ParameterConstants.INITIAL_LOAD_USE_RELOAD_CHANNEL));

            return null;
        }
    }

    private void insertNodeSecurityUpdate(Node node, boolean isReload) {
        Data data = createData(null, null, tablePrefix + "_node_security", " t.node_id = '"
                + node.getNodeId() + "'");
        if (data != null) {
            insertDataAndDataEventAndOutgoingBatch(data, node.getNodeId(),
                    Constants.UNKNOWN_ROUTER_ID, isReload);
        }
    }

    @Transactional
    public void sendScript(String nodeId, String script, boolean isLoad) {
        Node targetNode = nodeService.findNode(nodeId);
        Data data = new Data(Constants.NA, DataEventType.BSH, CsvUtils.escapeCsvData(script), null,
                null, Constants.CHANNEL_CONFIG, null, null);
        insertDataAndDataEventAndOutgoingBatch(data, targetNode.getNodeId(),
                Constants.UNKNOWN_ROUTER_ID, isLoad);
    }

    @Transactional
    public String sendSQL(String nodeId, String catalogName, String schemaName, String tableName,
            String sql, boolean isLoad) {
        Node sourceNode = nodeService.findIdentity();
        Node targetNode = nodeService.findNode(nodeId);
        if (targetNode == null) {
            // TODO message bundle
            return "Unknown node " + nodeId;
        }

        TriggerRouter trigger = triggerRouterService.getTriggerRouterForTableForCurrentNode(
                catalogName, schemaName, tableName, true);
        if (trigger == null) {
            // TODO message bundle
            return "Trigger for table " + tableName + " does not exist from node "
                    + sourceNode.getNodeGroupId();
        }

        insertSqlEvent(targetNode, trigger.getTrigger(), sql, isLoad);
        // TODO message bundle
        return "Successfully create SQL event for node " + targetNode.getNodeId();
    }

    @Transactional
    public String reloadTable(String nodeId, String catalogName, String schemaName, String tableName) {
        return reloadTable(nodeId, catalogName, schemaName, tableName, null);
    }

    @Transactional
    public String reloadTable(String nodeId, String catalogName, String schemaName,
            String tableName, String overrideInitialLoadSelect) {
        Node sourceNode = nodeService.findIdentity();
        Node targetNode = nodeService.findNode(nodeId);
        if (targetNode == null) {
            // TODO message bundle
            return "Unknown node " + nodeId;
        }

        TriggerRouter triggerRouter = triggerRouterService.getTriggerRouterForTableForCurrentNode(
                catalogName, schemaName, tableName, true);
        if (triggerRouter == null) {
            // TODO message bundle
            return "Trigger for table " + tableName + " does not exist from node "
                    + sourceNode.getNodeGroupId();
        }

        if (parameterService.is(ParameterConstants.INITIAL_LOAD_CREATE_SCHEMA_BEFORE_RELOAD)) {
            String xml = dbDialect.getCreateTableXML(triggerRouter);
            insertCreateEvent(targetNode, triggerRouter, xml, true);
        } else if (parameterService.is(ParameterConstants.INITIAL_LOAD_DELETE_BEFORE_RELOAD)) {
            insertPurgeEvent(targetNode, triggerRouter, true);
        }

        insertReloadEvent(targetNode, triggerRouter, overrideInitialLoadSelect);

        // TODO message bundle
        return "Successfully created event to reload table " + tableName + " for node "
                + targetNode.getNodeId();
    }

    /**
     * Because we can't add a trigger on the _node table, we are artificially
     * generating heartbeat events.
     * 
     * @param node
     */
    public void insertHeartbeatEvent(Node node, boolean isReload) {
        String tableName = TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE);
        List<NodeGroupLink> links = configurationService.getGroupLinksFor(parameterService
                .getNodeGroupId());
        for (NodeGroupLink nodeGroupLink : links) {
            if (nodeGroupLink.getDataEventAction() == NodeGroupLinkAction.P) {
                TriggerRouter triggerRouter = triggerRouterService
                        .getTriggerRouterForTableForCurrentNode(nodeGroupLink, null, null, tableName, false);
                if (triggerRouter != null) {
                    Data data = createData(triggerRouter.getTrigger(), String.format(
                            " t.node_id = '%s'", node.getNodeId()));
                    if (data != null) {
                        insertData(data);
                    } else {
                        log.warn("TableGeneratingEventsFailure", tableName);
                    }
                } else {
                    log.warn("TableGeneratingEventsFailure", tableName);
                }
            }
        }
    }

    public Data createData(String catalogName, String schemaName, String tableName) {
        return createData(catalogName, schemaName, tableName, null);
    }

    public Data createData(String catalogName, String schemaName, String tableName,
            String whereClause) {
        Data data = null;
        TriggerRouter trigger = triggerRouterService.getTriggerRouterForTableForCurrentNode(
                catalogName, schemaName, tableName, false);
        if (trigger != null) {
            data = createData(trigger.getTrigger(), whereClause);
        }
        return data;
    }

    public Data createData(Trigger trigger, String whereClause) {
        Data data = null;
        if (trigger != null) {
            String rowData = null;
            String pkData = null;
            if (whereClause != null) {
                rowData = (String) jdbcTemplate.queryForObject(dbDialect.createCsvDataSql(trigger,
                        whereClause), String.class);
                pkData = (String) jdbcTemplate.queryForObject(dbDialect.createCsvPrimaryKeySql(
                        trigger, whereClause), String.class);
            }
            TriggerHistory history = triggerRouterService.getNewestTriggerHistoryForTrigger(trigger
                    .getTriggerId());
            if (history == null) {
                history = triggerRouterService.findTriggerHistory(trigger.getSourceTableName());
                if (history == null) {
                    history = triggerRouterService.findTriggerHistory(trigger.getSourceTableName()
                            .toUpperCase());
                }
            }
            if (history != null) {
                data = new Data(trigger.getSourceTableName(), DataEventType.UPDATE, rowData,
                        pkData, history, trigger
                                .getChannelId(), null, null);
            }
        }
        return data;
    }

    public DataRef getDataRef() {
        List<DataRef> refs = getSimpleTemplate().query(getSql("findDataRefSql"),
                new RowMapper<DataRef>() {
                    public DataRef mapRow(ResultSet rs, int rowNum) throws SQLException {
                        return new DataRef(rs.getLong(1), rs.getDate(2));
                    }
                });
        if (refs.size() > 0) {
            return refs.get(0);
        } else {
            return new DataRef(-1, new Date());
        }
    }
    
    public List<DataGap> findDataGapsByStatus(DataGap.STATUS status) {
        return getSimpleTemplate().query(getSql("findDataGapsByStatusSql"),
                new RowMapper<DataGap>() {
                    public DataGap mapRow(ResultSet rs, int rowNum) throws SQLException {
                        return new DataGap(rs.getLong(1), rs.getLong(2), rs.getTimestamp(3));
                    }
                }, status.name());
    }
    
    public List<DataGap> findDataGaps() {
        List<DataGap> gaps = findDataGapsByStatus(DataGap.STATUS.GP);
        if (gaps.size() == 0) {
            gaps = new ArrayList<DataGap>(1);
            gaps.add(new DataGap(0, DataGap.OPEN_END_ID));
        }
        return gaps;

    }

    public void insertDataGap(DataGap gap) {
        jdbcTemplate.update(getSql("insertDataGapSql"), new Object[] { DataGap.STATUS.GP.name(),
                AppUtils.getHostName(), gap.getStartId(), gap.getEndId() }, new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.INTEGER });
    }

    public void updateDataGap(DataGap gap, DataGap.STATUS status) {
        jdbcTemplate.update(
                getSql("updateDataGapSql"),
                new Object[] { status.name(), AppUtils.getHostName(), gap.getStartId(),
                        gap.getEndId() }, new int[] { Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
                        Types.INTEGER });
    }

    public void saveDataRef(DataRef dataRef) {
        if (0 >= jdbcTemplate.update(getSql("updateDataRefSql"), new Object[] {
                dataRef.getRefDataId(), dataRef.getRefTime() }, new int[] { Types.INTEGER,
                Types.TIMESTAMP })) {
            jdbcTemplate.update(getSql("insertDataRefSql"), new Object[] { dataRef.getRefDataId(),
                    dataRef.getRefTime() }, new int[] { Types.INTEGER, Types.TIMESTAMP });
        }
    }

    public Date findCreateTimeOfEvent(long dataId) {
        try {
            return (Date) jdbcTemplate.queryForObject(getSql("findDataEventCreateTimeSql"),
                    new Object[] { dataId }, new int[] { Types.INTEGER }, Date.class);
        } catch (EmptyResultDataAccessException ex) {
            return null;
        }
    }

    public Date findCreateTimeOfData(long dataId) {
        try {
            return (Date) jdbcTemplate.queryForObject(getSql("findDataCreateTimeSql"),
                    new Object[] { dataId }, new int[] { Types.INTEGER }, Date.class);
        } catch (EmptyResultDataAccessException ex) {
            return null;
        }
    }  

    public Map<String, String> getRowDataAsMap(Data data) {
        Map<String, String> map = new HashMap<String, String>();
        String[] columnNames = CsvUtils.tokenizeCsvData(data.getTriggerHistory().getColumnNames());
        String[] columnData = CsvUtils.tokenizeCsvData(data.getRowData());
        for (int i = 0; i < columnNames.length; i++) {
            map.put(columnNames[i].toLowerCase(), columnData[i]);
        }
        return map;
    }

    public void setRowDataFromMap(Data data, Map<String, String> map) {
        String[] columnNames = CsvUtils.tokenizeCsvData(data.getTriggerHistory().getColumnNames());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = new CsvWriter(new OutputStreamWriter(out), ',');
        writer.setEscapeMode(CsvWriter.ESCAPE_MODE_BACKSLASH);
        for (String columnName : columnNames) {
            try {
                writer.write(map.get(columnName.toLowerCase()), true);
            } catch (IOException e) {
            }
        }
        writer.close();
        data.setRowData(out.toString());
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
            return this.heartbeatListeners;
        } else {
            List<IHeartbeatListener> listeners = new ArrayList<IHeartbeatListener>();
            if (listeners != null) {
                long ts = System.currentTimeMillis();
                for (IHeartbeatListener iHeartbeatListener : this.heartbeatListeners) {
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
    @Transactional
    public void heartbeat(boolean force) {
        List<IHeartbeatListener> listeners = getHeartbeatListeners(force);
        if (listeners.size() > 0) {
            Node me = nodeService.findIdentity();
            if (me != null) {
                log.info("NodeVersionUpdating");
                Calendar now = Calendar.getInstance();
                now.set(Calendar.MILLISECOND, 0);
                me.setHeartbeatTime(now.getTime());
                me.setTimezoneOffset(AppUtils.getTimezoneOffset());
                me.setSymmetricVersion(Version.version());
                me.setDatabaseType(dbDialect.getName());
                me.setDatabaseVersion(dbDialect.getVersion());
                me.setBatchInErrorCount(outgoingBatchService
                        .countOutgoingBatchesInError());
                if (parameterService.is(ParameterConstants.AUTO_UPDATE_NODE_VALUES)) {
                    log.info("NodeConfigurationUpdating");
                    me.setSchemaVersion(parameterService
                            .getString(ParameterConstants.SCHEMA_VERSION));
                    me.setExternalId(parameterService.getExternalId());
                    me.setNodeGroupId(parameterService.getNodeGroupId());
                    if (!StringUtils.isBlank(parameterService.getSyncUrl())) {
                        me.setSyncUrl(parameterService.getSyncUrl());
                    }
                }

                nodeService.updateNode(me);
                nodeService.updateNodeHostForCurrentNode();
                log.info("NodeVersionUpdated");

                Set<Node> children = nodeService.findNodesThatOriginatedFromNodeId(me.getNodeId());
                for (IHeartbeatListener l : listeners) {
                    l.heartbeat(me, children);
                }

                updateLastHeartbeatTime(listeners);

            } else {
                log.info("HeartbeatUpdatingFailure");
            }
        }
    }

    public void setReloadListeners(List<IReloadListener> listeners) {
        this.reloadListeners = listeners;
    }

    public void addReloadListener(IReloadListener listener) {
        if (reloadListeners == null) {
            reloadListeners = new ArrayList<IReloadListener>();
        }
        reloadListeners.add(listener);
    }

    public boolean removeReloadListener(IReloadListener listener) {
        if (reloadListeners != null) {
            return reloadListeners.remove(listener);
        } else {
            return false;
        }
    }

    public void setHeartbeatListeners(List<IHeartbeatListener> listeners) {
        this.heartbeatListeners = listeners;
    }

    public void addHeartbeatListener(IHeartbeatListener listener) {
        if (heartbeatListeners == null) {
            heartbeatListeners = new ArrayList<IHeartbeatListener>();
        }
        heartbeatListeners.add(listener);
    }

    public boolean removeHeartbeatListener(IHeartbeatListener listener) {
        if (heartbeatListeners != null) {
            return heartbeatListeners.remove(listener);
        } else {
            return false;
        }
    }

    public Data readData(ResultSet results) throws SQLException {
        Data data = new Data();
        data.setDataId(results.getLong(1));
        data.setTableName(results.getString(2));
        data.setEventType(DataEventType.getEventType(results.getString(3)));
        data.setRowData(results.getString(4));
        data.setPkData(results.getString(5));
        data.setOldData(results.getString(6));
        data.setCreateTime(results.getDate(7));
        int histId = results.getInt(8);
        data.setTriggerHistory(triggerRouterService.getTriggerHistory(histId));
        data.setChannelId(results.getString(9));
        data.setTransactionId(results.getString(10));
        data.setSourceNodeId(results.getString(11));
        data.setExternalData(results.getString(12));
        // TODO Be careful add more columns. Callers might not be expecting
        // them.
        return data;
    }


    public void setTriggerRouterService(ITriggerRouterService triggerService) {
        this.triggerRouterService = triggerService;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setPurgeService(IPurgeService purgeService) {
        this.purgeService = purgeService;
    }

    public void setOutgoingBatchService(IOutgoingBatchService outgoingBatchService) {
        this.outgoingBatchService = outgoingBatchService;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }
}
