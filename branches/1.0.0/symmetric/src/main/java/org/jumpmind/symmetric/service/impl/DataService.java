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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEvent;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.support.JdbcUtils;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class DataService extends AbstractService implements IDataService {

    static final Log logger = LogFactory.getLog(DataService.class);

    private IConfigurationService configurationService;

    private INodeService nodeService;

    private IOutgoingBatchService outgoingBatchService;

    private String tablePrefix;

    private IDbDialect dbDialect;

    private List<IReloadListener> listeners;

    private String insertIntoDataSql;

    private String insertIntoDataEventSql;

    private boolean deleteFirstForReload;

    public void insertReloadEvent(final Node targetNode, final Trigger trigger) {
        final TriggerHistory history = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());

        Data data = new Data(Constants.CHANNEL_RELOAD, trigger.getSourceTableName(), DataEventType.RELOAD, null, null,
                history);
        insertDataEvent(data, targetNode.getNodeId());
    }

    public void createPurgeEvent(final Node targetNode, final Trigger trigger) {
        final TriggerHistory history = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());
        final String sql = dbDialect.createPurgeSqlFor(targetNode, trigger);

        Data data = new Data(Constants.CHANNEL_RELOAD, trigger.getSourceTableName(), DataEventType.SQL, sql, null,
                history);
        insertDataEvent(data, targetNode.getNodeId());
    }

    public long insertData(final Data data) {
        return (Long) jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                PreparedStatement ps = c.prepareStatement(insertIntoDataSql, new int[] { 1 });
                ps.setString(1, data.getChannelId());
                ps.setString(2, data.getTableName());
                ps.setString(3, data.getEventType().getCode());
                ps.setString(4, data.getRowData());
                ps.setString(5, data.getPkData());
                ps.setLong(6, data.getAudit().getTriggerHistoryId());
                ps.execute();
                ResultSet rs = ps.getGeneratedKeys();
                rs.next();
                long dataId = rs.getLong(1);
                JdbcUtils.closeResultSet(rs);
                JdbcUtils.closeStatement(ps);
                return dataId;
            }
        });
    }

    public void insertDataEvent(DataEvent dataEvent) {
        jdbcTemplate.update(insertIntoDataEventSql, new Object[] { dataEvent.getDataId(), dataEvent.getNodeId() });
    }

    public void insertDataEvent(Data data, List<Node> nodes) {
        long dataId = insertData(data);
        for (Node node : nodes) {
            insertDataEvent(new DataEvent(dataId, node.getNodeId()));
        }
    }

    public void insertDataEvent(Data data, String nodeId) {
        long dataId = insertData(data);
        insertDataEvent(new DataEvent(dataId, nodeId));
    }

    public String reloadNode(String nodeId) {
        Node sourceNode = nodeService.findIdentity();
        Node targetNode = nodeService.findNode(nodeId);
        if (targetNode == null) {
            return "Unknown node " + nodeId;
        }
        if (listeners != null) {
            for (IReloadListener listener : listeners) {
                listener.beforeReload(targetNode);
            }
        }
        List<Trigger> triggers = configurationService.getActiveTriggersForReload(sourceNode.getNodeGroupId(),
                targetNode.getNodeGroupId());

        if (deleteFirstForReload) {
            for (ListIterator<Trigger> iterator = triggers.listIterator(triggers.size()); iterator.hasPrevious();) {
                Trigger trigger = iterator.previous();
                createPurgeEvent(targetNode, trigger);
            }

            outgoingBatchService.buildOutgoingBatches(nodeId);
        }

        for (Trigger trigger : triggers) {
            insertReloadEvent(targetNode, trigger);
            outgoingBatchService.buildOutgoingBatches(nodeId);
        }

        if (listeners != null) {
            for (IReloadListener listener : listeners) {
                listener.afterReload(targetNode);
            }
        }
        return "Successfully created events to reload node " + nodeId;
    }

    /**
     * Because we can't add a trigger on the _node table, we are artificially generating heartbeat events.
     * @param node
     */
    public void insertHeartbeatEvent(Node node) {
        Data data = createData(tablePrefix + "_node", " t.node_id = '" + node.getNodeId() + "'");
        if (data != null) {
            data.setChannelId(Constants.CHANNEL_CONFIG);
            insertDataEvent(data, nodeService.findNodesToPushTo());
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
        Trigger trigger = configurationService.getTriggerFor(tableName, runtimeConfiguration.getNodeGroupId());
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
            data = new Data(Constants.CHANNEL_RELOAD, trigger.getSourceTableName(), DataEventType.UPDATE, rowData,
                    pkData, history);
        }
        return data;
    }

    public Map<String, String> getRowDataAsMap(Data data) {
        Map<String, String> map = new HashMap<String, String>();
        String[] columnNames = tokenizeCsvData(data.getAudit().getColumnNames());
        String[] columnData = tokenizeCsvData(data.getRowData());
        for (int i = 0; i < columnNames.length; i++) {
            map.put(columnNames[i].toLowerCase(), columnData[i]);
        }
        return map;
    }

    public void setRowDataFromMap(Data data, Map<String, String> map) {
        String[] columnNames = tokenizeCsvData(data.getAudit().getColumnNames());
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

    public String[] tokenizeCsvData(String csvData) {
        String[] tokens = null;
        if (csvData != null) {
            InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(csvData.getBytes()));
            CsvReader csvReader = new CsvReader(reader);
            csvReader.setEscapeMode(CsvReader.ESCAPE_MODE_BACKSLASH);
            try {
                if (csvReader.readRecord()) {
                    tokens = csvReader.getValues();
                }
            } catch (IOException e) {
            }
        }
        return tokens;
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

    public void setInsertIntoDataEventSql(String insertIntoDataEventSql) {
        this.insertIntoDataEventSql = insertIntoDataEventSql;
    }

    public void setInsertIntoDataSql(String insertIntoDataSql) {
        this.insertIntoDataSql = insertIntoDataSql;
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

    public void setOutgoingBatchService(IOutgoingBatchService outgoingBatchService) {
        this.outgoingBatchService = outgoingBatchService;
    }

    public void setDeleteFirstForReload(boolean deleteFirstForReload) {
        this.deleteFirstForReload = deleteFirstForReload;
    }

}
