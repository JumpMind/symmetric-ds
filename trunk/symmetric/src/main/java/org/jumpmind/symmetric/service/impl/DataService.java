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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEvent;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.support.JdbcUtils;

public class DataService extends AbstractService implements IDataService {

    static final Log logger = LogFactory.getLog(DataService.class);
            
    private IConfigurationService configurationService;

    private INodeService nodeService;
    
    private String tablePrefix;
    
    private IDbDialect dbDialect;

    private String insertIntoDataSql;

    private String insertIntoDataEventSql;

    public void createReloadEvent(final Node targetNode, final Trigger trigger) {
        final TriggerHistory history = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());

        Data data = new Data(Constants.CHANNEL_RELOAD, trigger.getSourceTableName(), DataEventType.RELOAD,
                null, null, history);
        long dataId = createData(data);
        createDataEvent(new DataEvent(dataId, targetNode.getNodeId()));
    }

    public void createPurgeEvent(final Node targetNode, final Trigger trigger) {
        final TriggerHistory history = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());
        final String sql = dbDialect.createPurgeSqlFor(targetNode, trigger);
        
        Data data = new Data(Constants.CHANNEL_RELOAD, trigger.getSourceTableName(), DataEventType.SQL, sql,
                null, history);
        long dataId = createData(data);
        createDataEvent(new DataEvent(dataId, targetNode.getNodeId()));
    }

    public long createData(final Data data) {
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
    
    public void createDataEvent(DataEvent dataEvent) {
        jdbcTemplate.update(insertIntoDataEventSql, new Object[] { dataEvent.getDataId(),
                dataEvent.getNodeId() });
    }
    
    public String reloadNode(String nodeId) {
        Node sourceNode = nodeService.findIdentity();
        Node targetNode = nodeService.findNode(nodeId);
        if (targetNode == null) {
            return "Unknown node " + nodeId;
        }
        List<Trigger> triggers = configurationService.getActiveTriggersForReload(sourceNode.getNodeGroupId(),
                targetNode.getNodeGroupId());
        for (ListIterator<Trigger> iterator = triggers.listIterator(triggers.size()); iterator.hasPrevious();) {
            Trigger trigger = iterator.previous();
            createPurgeEvent(targetNode, trigger);
        }
        for (Trigger trigger : triggers) {
            createReloadEvent(targetNode, trigger);
        }
        return "Successfully created events to reload node " + nodeId;
    }

    /**
     * Because we can't add a trigger on the _node table, we are artificially generating heartbeat events.
     * @param node
     */
    public void createHeartbeatEvent(Node node) {
        String whereClause = " t.node_id = '" + node.getNodeId() + "'";
        Trigger trigger = configurationService.getTriggerFor(tablePrefix + "_node", runtimeConfiguration
                .getNodeGroupId());
        if (trigger != null) {
            String rowData = (String) jdbcTemplate.queryForObject(dbDialect.createCsvDataSql(trigger,
                    whereClause), String.class);
            String pkData = (String) jdbcTemplate.queryForObject(dbDialect.createCsvPrimaryKeySql(trigger,
                    whereClause), String.class);
            TriggerHistory history = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());
            Data data = new Data(Constants.CHANNEL_CONFIG, trigger.getSourceTableName(),
                    DataEventType.UPDATE, rowData, pkData, history);
            long dataId = createData(data);

            List<Node> nodes = nodeService.findNodesToPushTo();
            for (Node pushNode : nodes) {
                createDataEvent(new DataEvent(dataId, pushNode.getNodeId()));
            }
        } else {
            logger.info("Not generating data/data events for node because a trigger is not created for that table yet.");
        }
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

}
