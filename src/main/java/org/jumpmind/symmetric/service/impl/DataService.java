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
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.csv.CsvUtil;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEvent;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;

import com.csvreader.CsvWriter;

public class DataService extends AbstractService implements IDataService {

    static final Log logger = LogFactory.getLog(DataService.class);

    private IConfigurationService configurationService;

    private INodeService nodeService;

    private IPurgeService purgeService;

    private IOutgoingBatchService outgoingBatchService;

    private String tablePrefix;

    private IDbDialect dbDialect;

    private List<IReloadListener> listeners;

    public void insertReloadEvent(final Node targetNode, final Trigger trigger) {
        insertReloadEvent(targetNode, trigger, null);
    }

    public void insertReloadEvent(final Node targetNode, final Trigger trigger, final String overrideInitialLoadSelect) {
        TriggerHistory history = lookupTriggerHistory(trigger);
        // initial_load_select for table can be overridden by populating the
        // row_data
        Data data = new Data(history.getSourceTableName(), DataEventType.RELOAD, overrideInitialLoadSelect, null,
                history);
        insertDataEvent(data, Constants.CHANNEL_RELOAD, targetNode.getNodeId());
    }

    private TriggerHistory lookupTriggerHistory(Trigger trigger) {
        TriggerHistory history = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());

        if (history == null) {
            throw new RuntimeException("Cannot find history for trigger " + trigger.getTriggerId() + ", "
                    + trigger.getSourceTableName());
        }
        return history;
    }

    public void insertPurgeEvent(final Node targetNode, final Trigger trigger) {
        String sql = dbDialect.createPurgeSqlFor(targetNode, trigger, lookupTriggerHistory(trigger));
        insertSqlEvent(targetNode, trigger, sql);
    }

    public void insertSqlEvent(final Node targetNode, final Trigger trigger, String sql) {
        TriggerHistory history = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());
        Data data = new Data(trigger.getSourceTableName(), DataEventType.SQL, CsvUtil.escapeCsvData(sql), null, history);
        insertDataEvent(data, Constants.CHANNEL_RELOAD, targetNode.getNodeId());
    }

    public void insertCreateEvent(final Node targetNode, final Trigger trigger, String xml) {
        TriggerHistory history = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());
        Data data = new Data(trigger.getSourceTableName(), DataEventType.CREATE, CsvUtil.escapeCsvData(xml), null,
                history);
        insertDataEvent(data, Constants.CHANNEL_RELOAD, targetNode.getNodeId());
    }

    public long insertData(final Data data) {
        return dbDialect.insertWithGeneratedKey(getSql("insertIntoDataSql"), SequenceIdentifier.DATA,
                new PreparedStatementCallback() {
                    public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                        ps.setString(1, data.getTableName());
                        ps.setString(2, data.getEventType().getCode());
                        ps.setString(3, data.getRowData());
                        ps.setString(4, data.getPkData());
                        ps.setLong(5, data.getAudit().getTriggerHistoryId());
                        return null;
                    }
                });
    }

    public void insertDataEvent(DataEvent dataEvent) {
        jdbcTemplate.update(getSql("insertIntoDataEventSql"), new Object[] { dataEvent.getDataId(),
                dataEvent.getNodeId(), dataEvent.getChannelId(), dataEvent.getTransactionId(), dataEvent.getBatchId(),
                dataEvent.isBatched() ? 1 : 0 }, new int[] { Types.INTEGER, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.INTEGER, Types.INTEGER });
    }

    public void insertDataEvent(Data data, String channelId, List<Node> nodes) {
        insertDataEvent(data, channelId, null, nodes);
    }

    public void insertDataEvent(Data data, String channelId, String transactionId, List<Node> nodes) {
        long dataId = insertData(data);
        for (Node node : nodes) {
            insertDataEvent(new DataEvent(dataId, node.getNodeId(), channelId, transactionId));
        }
    }

    public void insertDataEvent(Data data, String channelId, String nodeId) {
        long dataId = insertData(data);
        insertDataEvent(new DataEvent(dataId, nodeId, channelId));
    }

    public String reloadNode(String nodeId) {
        Node targetNode = nodeService.findNode(nodeId);
        if (targetNode == null) {
            return "Unknown node " + nodeId;
        }
        if (nodeService.setInitialLoadEnabled(nodeId, true)) {
            return "Successfully opened initial load for node " + nodeId;
        } else {
            return "Could not open initial load for " + nodeId;
        }
    }

    public void insertReloadEvent(Node targetNode) {
        Node sourceNode = nodeService.findIdentity();
        if (listeners != null) {
            for (IReloadListener listener : listeners) {
                listener.beforeReload(targetNode);
            }
        }
        
        // outgoing data events are pointless because we are reloading all data
        purgeService.purgeAllOutgoingEventsForNode(targetNode.getNodeId());

        insertNodeSecurityUpdate(targetNode);
        List<Trigger> triggers = configurationService.getActiveTriggersForReload(sourceNode.getNodeGroupId(),
                targetNode.getNodeGroupId());

        if (parameterService.is(ParameterConstants.AUTO_CREATE_SCHEMA_BEFORE_RELOAD)) {
            for (Trigger trigger : triggers) {
                String xml = dbDialect.getCreateTableXML(trigger);
                insertCreateEvent(targetNode, trigger, xml);
                buildReloadBatches(targetNode.getNodeId());
            }
        }
        if (parameterService.is(ParameterConstants.AUTO_DELETE_BEFORE_RELOAD)) {
            for (ListIterator<Trigger> iterator = triggers.listIterator(triggers.size()); iterator.hasPrevious();) {
                Trigger trigger = iterator.previous();
                insertPurgeEvent(targetNode, trigger);
                buildReloadBatches(targetNode.getNodeId());
            }
        }

        for (Trigger trigger : triggers) {
            insertReloadEvent(targetNode, trigger);
            buildReloadBatches(targetNode.getNodeId());
        }

        if (listeners != null) {
            for (IReloadListener listener : listeners) {
                listener.afterReload(targetNode);
            }
        }
        nodeService.setInitialLoadEnabled(targetNode.getNodeId(), false);
        insertNodeSecurityUpdate(targetNode);

        // remove all incoming events from the node are starting a reload for.
        purgeService.purgeAllIncomingEventsForNode(targetNode.getNodeId());
    }

    /**
     * This should be called after a reload event is inserted so there is a one
     * to one between data events and reload batches.
     */
    private void buildReloadBatches(String nodeId) {
        NodeChannel channel = new NodeChannel();
        channel.setId(Constants.CHANNEL_RELOAD);
        channel.setEnabled(true);
        outgoingBatchService.buildOutgoingBatches(nodeId, channel);

    }

    private void insertNodeSecurityUpdate(Node node) {
        Data data = createData(tablePrefix + "_node_security", " t.node_id = '" + node.getNodeId() + "'");
        if (data != null) {
            insertDataEvent(data, Constants.CHANNEL_RELOAD, node.getNodeId());
        }
    }

    public String sendSQL(String nodeId, String tableName, String sql) {
        Node sourceNode = nodeService.findIdentity();
        Node targetNode = nodeService.findNode(nodeId);
        if (targetNode == null) {
            return "Unknown node " + nodeId;
        }

        Trigger trigger = configurationService.getTriggerFor(tableName, sourceNode.getNodeGroupId());
        if (trigger == null) {
            return "Trigger for table " + tableName + " does not exist from node " + sourceNode.getNodeGroupId();
        }

        insertSqlEvent(targetNode, trigger, sql);
        return "Successfully create SQL event for node " + targetNode.getNodeId();
    }

    public String reloadTable(String nodeId, String tableName) {
        return reloadTable(nodeId, tableName, null);
    }

    public String reloadTable(String nodeId, String tableName, String overrideInitialLoadSelect) {
        Node sourceNode = nodeService.findIdentity();
        Node targetNode = nodeService.findNode(nodeId);
        if (targetNode == null) {
            return "Unknown node " + nodeId;
        }

        Trigger trigger = configurationService.getTriggerFor(tableName, sourceNode.getNodeGroupId());
        if (trigger == null) {
            return "Trigger for table " + tableName + " does not exist from node " + sourceNode.getNodeGroupId();
        }

        if (parameterService.is(ParameterConstants.AUTO_CREATE_SCHEMA_BEFORE_RELOAD)) {
            String xml = dbDialect.getCreateTableXML(trigger);
            insertCreateEvent(targetNode, trigger, xml);
        } else if (parameterService.is(ParameterConstants.AUTO_DELETE_BEFORE_RELOAD)) {
            insertPurgeEvent(targetNode, trigger);
        }

        insertReloadEvent(targetNode, trigger, overrideInitialLoadSelect);

        return "Successfully created event to reload table " + tableName + " for node " + targetNode.getNodeId();
    }

    /**
     * Because we can't add a trigger on the _node table, we are artificially
     * generating heartbeat events.
     * 
     * @param node
     */
    public void insertHeartbeatEvent(Node node) {
        Data data = createData(tablePrefix + "_node", " t.node_id = '" + node.getNodeId() + "'");
        if (data != null && data.getAudit() != null) {
            insertDataEvent(data, Constants.CHANNEL_CONFIG, nodeService.findNodesToPushTo());
        } else {
            logger
                    .info("Not generating data/data events for node because a trigger is not created for that table yet.");
        }
    }

    public Data createData(String tableName) {
        return createData(tableName, null);
    }

    public Data createData(String tableName, String whereClause) {
        Data data = null;
        Trigger trigger = configurationService.getTriggerFor(tableName, parameterService.getNodeGroupId());
        if (trigger != null) {
            String rowData = null;
            String pkData = null;
            if (whereClause != null) {
                rowData = (String) jdbcTemplate.queryForObject(dbDialect.createCsvDataSql(trigger, whereClause),
                        String.class);
                pkData = (String) jdbcTemplate.queryForObject(dbDialect.createCsvPrimaryKeySql(trigger, whereClause),
                        String.class);
            }
            TriggerHistory history = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());
            data = new Data(trigger.getSourceTableName(), DataEventType.UPDATE, rowData, pkData, history);
        }
        return data;
    }

    public Map<String, String> getRowDataAsMap(Data data) {
        Map<String, String> map = new HashMap<String, String>();
        String[] columnNames = CsvUtil.tokenizeCsvData(data.getAudit().getColumnNames());
        String[] columnData = CsvUtil.tokenizeCsvData(data.getRowData());
        for (int i = 0; i < columnNames.length; i++) {
            map.put(columnNames[i].toLowerCase(), columnData[i]);
        }
        return map;
    }

    public void setRowDataFromMap(Data data, Map<String, String> map) {
        String[] columnNames = CsvUtil.tokenizeCsvData(data.getAudit().getColumnNames());
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

    @Deprecated
    public String[] tokenizeCsvData(String csvData) {
        return CsvUtil.tokenizeCsvData(csvData);
    }

    public void setReloadListeners(List<IReloadListener> listeners) {
        this.listeners = listeners;
    }

    public void addReloadListener(IReloadListener listener) {
        if (listeners == null) {
            listeners = new ArrayList<IReloadListener>();
        }
        listeners.add(listener);
    }

    public void removeReloadListener(IReloadListener listener) {
        listeners.remove(listener);
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public void setPurgeService(IPurgeService purgeService) {
        this.purgeService = purgeService;
    }

    public void setOutgoingBatchService(IOutgoingBatchService outgoingBatchService) {
        this.outgoingBatchService = outgoingBatchService;
    }

}
