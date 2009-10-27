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
import java.util.Date;
import java.util.HashMap;
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
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.ext.IHeartbeatListener;
import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEvent;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.DataRef;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.service.LockActionConstants;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.symmetric.util.CsvUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.simple.ParameterizedRowMapper;

import com.csvreader.CsvWriter;

public class DataService extends AbstractService implements IDataService {

    private ITriggerRouterService triggerRouterService;

    private INodeService nodeService;

    private IPurgeService purgeService;

    private IClusterService clusterService;

    private IOutgoingBatchService outgoingBatchService;

    private List<IReloadListener> reloadListeners;

    private List<IHeartbeatListener> heartbeatListeners;

    protected Map<IHeartbeatListener, Long> lastHeartbeatTimestamps = new HashMap<IHeartbeatListener, Long>();

    public void insertReloadEvent(final Node targetNode, final TriggerRouter triggerRouter) {
        insertReloadEvent(targetNode, triggerRouter, null);
    }

    public void insertReloadEvent(final Node targetNode, final TriggerRouter triggerRouter,
            final String overrideInitialLoadSelect) {
        TriggerHistory history = lookupTriggerHistory(triggerRouter.getTrigger());
        // initial_load_select for table can be overridden by populating the
        // row_data
        Data data = new Data(history.getSourceTableName(), DataEventType.RELOAD,
                overrideInitialLoadSelect != null ? overrideInitialLoadSelect : triggerRouter.getRouter()
                        .getInitialLoadSelect(), null, history, Constants.CHANNEL_RELOAD, null, null);
        insertDataAndDataEvent(data, targetNode.getNodeId(), triggerRouter.getRouter().getRouterId());
    }

    public void insertResendConfigEvent(final Node targetNode) {
        Data data = new Data(Constants.NA, DataEventType.CONFIG, null, null, null, Constants.CHANNEL_CONFIG, null, null);
        insertDataAndDataEvent(data, targetNode.getNodeId(), Constants.UNKNOWN_ROUTER_ID);
    }

    private TriggerHistory lookupTriggerHistory(Trigger trigger) {
        TriggerHistory history = triggerRouterService.getNewestTriggerHistoryForTrigger(trigger.getTriggerId());

        if (history == null) {
            throw new RuntimeException("Cannot find history for trigger " + trigger.getTriggerId() + ", "
                    + trigger.getSourceTableName());
        }
        return history;
    }

    public void insertPurgeEvent(final Node targetNode, final TriggerRouter triggerRouter) {
        String sql = dbDialect.createPurgeSqlFor(targetNode, triggerRouter);
        insertSqlEvent(targetNode, triggerRouter.getTrigger(), sql);
    }

    public void insertSqlEvent(final Node targetNode, final Trigger trigger, String sql) {
        TriggerHistory history = triggerRouterService.getNewestTriggerHistoryForTrigger(trigger.getTriggerId());
        Data data = new Data(trigger.getSourceTableName(), DataEventType.SQL, CsvUtils.escapeCsvData(sql), null,
                history, Constants.CHANNEL_RELOAD, null, null);
        insertDataAndDataEvent(data, targetNode.getNodeId(), Constants.UNKNOWN_ROUTER_ID);
    }

    public void insertSqlEvent(final Node targetNode, String sql) {
        Data data = new Data(Constants.NA, DataEventType.SQL, CsvUtils.escapeCsvData(sql), null, null,
                Constants.CHANNEL_RELOAD, null, null);
        insertDataAndDataEvent(data, targetNode.getNodeId(), Constants.UNKNOWN_ROUTER_ID);
    }

    public void insertCreateEvent(final Node targetNode, final TriggerRouter triggerRouter, String xml) {
        TriggerHistory history = triggerRouterService.getNewestTriggerHistoryForTrigger(triggerRouter.getTrigger()
                .getTriggerId());
        Data data = new Data(triggerRouter.getTrigger().getSourceTableName(), DataEventType.CREATE, CsvUtils
                .escapeCsvData(xml), null, history, Constants.CHANNEL_RELOAD, null, null);
        insertDataAndDataEvent(data, targetNode.getNodeId(), Constants.UNKNOWN_ROUTER_ID);
    }

    public long insertData(final Data data) {
        long id = dbDialect.insertWithGeneratedKey(getSql("insertIntoDataSql"), SequenceIdentifier.DATA,
                new PreparedStatementCallback() {
                    public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                        ps.setString(1, data.getTableName());
                        ps.setString(2, data.getEventType().getCode());
                        ps.setString(3, data.getRowData());
                        ps.setString(4, data.getPkData());
                        ps.setString(5, data.getOldData());
                        ps.setLong(6, data.getTriggerHistory() != null ? data.getTriggerHistory().getTriggerHistoryId()
                                : -1);
                        ps.setString(7, data.getChannelId());
                        return null;
                    }
                });
        data.setDataId(id);
        return id;
    }

    public void insertDataEvent(DataEvent dataEvent) {
        this.insertDataEvent(jdbcTemplate, dataEvent.getDataId(), dataEvent.getBatchId(), dataEvent.getRouterId());
    }

    public void insertDataEvent(long dataId, long batchId, String routerId) {
        this.insertDataEvent(jdbcTemplate, dataId, batchId, routerId);
    }

    public void insertDataEvent(JdbcTemplate template, long dataId, long batchId, String routerId) {
        template.update(getSql("insertIntoDataEventSql"), new Object[] { dataId, batchId,
                StringUtils.isBlank(routerId) ? Constants.UNKNOWN_ROUTER_ID : routerId }, new int[] { Types.INTEGER,
                Types.INTEGER, Types.VARCHAR });
    }

    public void insertDataAndDataEvent(Data data, String channelId, List<Node> nodes, String routerId) {
        long dataId = insertData(data);
        for (Node node : nodes) {
            insertDataEvent(dataId, channelId, node.getNodeId(), routerId);
        }
    }

    public void insertDataAndDataEvent(Data data, String nodeId, String routerId) {
        long dataId = insertData(data);
        insertDataEvent(dataId, data.getChannelId(), nodeId, routerId);
    }

    public void insertDataEvent(long dataId, String channelId, String nodeId, String routerId) {
        OutgoingBatch outgoingBatch = new OutgoingBatch(nodeId, channelId);
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

    public void insertReloadEvent(Node targetNode) {
        Node sourceNode = nodeService.findIdentity();
        if (reloadListeners != null) {
            for (IReloadListener listener : reloadListeners) {
                listener.beforeReload(targetNode);
            }
        }

        // outgoing data events are pointless because we are reloading all data
        outgoingBatchService.markAllAsSentForNode(targetNode);

        // insert node security so the client doing the initial load knows that
        // an initial load is currently happening
        insertNodeSecurityUpdate(targetNode);

        List<TriggerRouter> triggerRouters = triggerRouterService.getActiveTriggerRoutersForReload(sourceNode
                .getNodeGroupId(), targetNode.getNodeGroupId());

        if (parameterService.is(ParameterConstants.AUTO_CREATE_SCHEMA_BEFORE_RELOAD)) {
            for (TriggerRouter triggerRouter : triggerRouters) {
                String xml = dbDialect.getCreateTableXML(triggerRouter);
                insertCreateEvent(targetNode, triggerRouter, xml);
            }
        }

        if (parameterService.is(ParameterConstants.AUTO_DELETE_BEFORE_RELOAD)) {
            for (ListIterator<TriggerRouter> iterator = triggerRouters.listIterator(triggerRouters.size()); iterator
                    .hasPrevious();) {
                TriggerRouter triggerRouter = iterator.previous();
                insertPurgeEvent(targetNode, triggerRouter);
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
        insertNodeSecurityUpdate(targetNode);

        // remove all incoming events from the node are starting a reload for.
        purgeService.purgeAllIncomingEventsForNode(targetNode.getNodeId());
    }

    private void insertNodeSecurityUpdate(Node node) {
        Data data = createData(tablePrefix + "_node_security", " t.node_id = '" + node.getNodeId() + "'");
        if (data != null) {
            insertDataAndDataEvent(data, node.getNodeId(), Constants.UNKNOWN_ROUTER_ID);
        }
    }

    public void sendScript(String nodeId, String script) {
        Node targetNode = nodeService.findNode(nodeId);
        Data data = new Data(Constants.NA, DataEventType.BSH, CsvUtils.escapeCsvData(script), null, null,
                Constants.CHANNEL_RELOAD, null, null);
        insertDataAndDataEvent(data, targetNode.getNodeId(), Constants.UNKNOWN_ROUTER_ID);
    }

    public String sendSQL(String nodeId, String tableName, String sql) {
        Node sourceNode = nodeService.findIdentity();
        Node targetNode = nodeService.findNode(nodeId);
        if (targetNode == null) {
            // TODO message bundle
            return "Unknown node " + nodeId;
        }

        TriggerRouter trigger = triggerRouterService.findTriggerRouter(tableName, sourceNode.getNodeGroupId());
        if (trigger == null) {
            // TODO message bundle
            return "Trigger for table " + tableName + " does not exist from node " + sourceNode.getNodeGroupId();
        }

        insertSqlEvent(targetNode, trigger.getTrigger(), sql);
        // TODO message bundle
        return "Successfully create SQL event for node " + targetNode.getNodeId();
    }

    public String reloadTable(String nodeId, String tableName) {
        return reloadTable(nodeId, tableName, null);
    }

    public String reloadTable(String nodeId, String tableName, String overrideInitialLoadSelect) {
        Node sourceNode = nodeService.findIdentity();
        Node targetNode = nodeService.findNode(nodeId);
        if (targetNode == null) {
            // TODO message bundle
            return "Unknown node " + nodeId;
        }

        TriggerRouter triggerRouter = triggerRouterService.findTriggerRouter(tableName, sourceNode.getNodeGroupId());
        if (triggerRouter == null) {
            // TODO message bundle
            return "Trigger for table " + tableName + " does not exist from node " + sourceNode.getNodeGroupId();
        }

        if (parameterService.is(ParameterConstants.AUTO_CREATE_SCHEMA_BEFORE_RELOAD)) {
            String xml = dbDialect.getCreateTableXML(triggerRouter);
            insertCreateEvent(targetNode, triggerRouter, xml);
        } else if (parameterService.is(ParameterConstants.AUTO_DELETE_BEFORE_RELOAD)) {
            insertPurgeEvent(targetNode, triggerRouter);
        }

        insertReloadEvent(targetNode, triggerRouter, overrideInitialLoadSelect);

        // TODO message bundle
        return "Successfully created event to reload table " + tableName + " for node " + targetNode.getNodeId();
    }

    /**
     * Because we can't add a trigger on the _node table, we are artificially
     * generating heartbeat events.
     * 
     * @param node
     */
    public void insertHeartbeatEvent(Node node) {
        String tableName = TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE);
        Trigger trigger = new Trigger(tableName);
        Data data = createData(trigger, String.format(" t.node_id = '%s'", node.getNodeId()));
        if (data != null) {
            insertDataAndDataEvent(data, Constants.CHANNEL_CONFIG, nodeService.findNodesToPushTo(),
                    Constants.UNKNOWN_ROUTER_ID);
        } else {
            log.info("TableGeneratingEventsFailure", tableName);
        }
    }

    public Data createData(String tableName) {
        return createData(tableName, null);
    }

    public Data createData(String tableName, String whereClause) {
        Data data = null;
        TriggerRouter trigger = triggerRouterService.findTriggerRouter(tableName, parameterService.getNodeGroupId());
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
                rowData = (String) jdbcTemplate.queryForObject(dbDialect.createCsvDataSql(trigger, whereClause),
                        String.class);
                pkData = (String) jdbcTemplate.queryForObject(dbDialect.createCsvPrimaryKeySql(trigger, whereClause),
                        String.class);
            }
            TriggerHistory history = triggerRouterService.getNewestTriggerHistoryForTrigger(trigger.getTriggerId());
            if (history == null) {
                history = triggerRouterService.findTriggerHistory(trigger.getSourceTableName());
                if (history == null) {
                    history = triggerRouterService.findTriggerHistory(trigger.getSourceTableName().toUpperCase());
                }
            }
            if (history != null) {
                data = new Data(trigger.getSourceTableName(), DataEventType.UPDATE, rowData, pkData, history,
                        Constants.CHANNEL_RELOAD, null, null);
            }
        }
        return data;
    }

    public DataRef getDataRef() {
        List<DataRef> refs = getSimpleTemplate().query(getSql("findDataRefSql"), new ParameterizedRowMapper<DataRef>() {
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

    public void saveDataRef(DataRef dataRef) {
        if (0 >= jdbcTemplate.update(getSql("updateDataRefSql"), new Object[] { dataRef.getRefDataId(),
                dataRef.getRefTime() }, new int[] { Types.INTEGER, Types.TIMESTAMP })) {
            jdbcTemplate.update(getSql("insertDataRefSql"),
                    new Object[] { dataRef.getRefDataId(), dataRef.getRefTime() }, new int[] { Types.INTEGER,
                            Types.TIMESTAMP });
        }
    }

    public Date findCreateTimeOfEvent(long dataId) {
        return (Date) jdbcTemplate.queryForObject(getSql("findDataEventCreateTimeSql"), new Object[] { dataId },
                new int[] { Types.INTEGER }, Date.class);
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
    public void heartbeat(boolean force) {
        List<IHeartbeatListener> listeners = getHeartbeatListeners(force);
        if (listeners.size() > 0) {
            if (clusterService.lock(LockActionConstants.HEARTBEAT)) {
                try {
                    Node me = nodeService.findIdentity();
                    if (me != null) {
                        log.info("NodeVersionUpdating");
                        me.setHeartbeatTime(new Date());
                        me.setTimezoneOffset(AppUtils.getTimezoneOffset());
                        me.setSymmetricVersion(Version.version());
                        me.setDatabaseType(dbDialect.getName());
                        me.setDatabaseVersion(dbDialect.getVersion());
                        OutgoingBatches batches = outgoingBatchService.getOutgoingBatches(me);
                        me.setBatchToSendCount(batches.countBatches(false));
                        me.setBatchInErrorCount(batches.countBatches(true));
                        if (parameterService.is(ParameterConstants.AUTO_UPDATE_NODE_VALUES)) {
                            log.info("NodeConfigurationUpdating");
                            me.setSchemaVersion(parameterService.getString(ParameterConstants.SCHEMA_VERSION));
                            me.setExternalId(parameterService.getExternalId());
                            me.setNodeGroupId(parameterService.getNodeGroupId());
                            if (!StringUtils.isBlank(parameterService.getMyUrl())) {
                                me.setSyncURL(parameterService.getMyUrl());
                            }
                        }

                        nodeService.updateNode(me);
                        log.info("NodeVersionUpdated");

                        Set<Node> children = nodeService.findNodesThatOriginatedFromNodeId(me.getNodeId());
                        for (IHeartbeatListener l : listeners) {
                            l.heartbeat(me, children);
                        }

                    }

                } finally {
                    updateLastHeartbeatTime(listeners);
                    clusterService.unlock(LockActionConstants.HEARTBEAT);
                }

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

    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
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
        return data;
    }

}
