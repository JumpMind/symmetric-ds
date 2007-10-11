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

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;

public class DataService implements IDataService {

    private JdbcTemplate jdbcTemplate;

    private IConfigurationService configurationService;

    private INodeService nodeService;

    private String insertIntoDataSql;

    private String insertIntoDataEventSql;

    public void createReloadEvent(final Node targetNode, final Trigger trigger) {
        final TriggerHistory history = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());

        int dataId = insertData(new Object[] { Constants.CHANNEL_RELOAD, trigger.getSourceTableName(),
                DataEventType.RELOAD.getCode(), null, null, history.getTriggerHistoryId() });
        jdbcTemplate.update(insertIntoDataEventSql, new Object[] { dataId, targetNode.getNodeId() });
    }

    public void createPurgeEvent(final Node targetNode, final Trigger trigger) {
        final TriggerHistory history = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());
        final String sql = "\"delete from " + trigger.getDefaultTargetTableName() + " where "
                + trigger.getInitialLoadSelect().replaceAll("\"", "\\\"") + "\"";

        int dataId = insertData(new Object[] { Constants.CHANNEL_RELOAD, trigger.getSourceTableName(),
                DataEventType.SQL.getCode(), sql, null, history.getTriggerHistoryId() });
        jdbcTemplate.update(insertIntoDataEventSql, new Object[] { dataId, targetNode.getNodeId() });
    }

    protected int insertData(final Object[] values) {
        return (Integer) jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                PreparedStatement ps = c.prepareStatement(insertIntoDataSql, new int[] { 1 });
                int columnNumber = 1;
                for (Object value : values) {
                    ps.setObject(columnNumber++, value);
                }
                ps.execute();
                ResultSet rs = ps.getGeneratedKeys();
                rs.next();
                int dataId = rs.getInt(1);
                JdbcUtils.closeResultSet(rs);
                JdbcUtils.closeStatement(ps);
                return dataId;
            }
        });
    }
    
    public void reloadNode(String nodeId) {
        Node sourceNode = nodeService.findIdentity();
        Node targetNode = nodeService.findNode(nodeId);
        List<Trigger> triggers = configurationService.getActiveTriggersForReload(sourceNode.getNodeGroupId(),
                targetNode.getNodeGroupId());
        for (ListIterator<Trigger> iterator = triggers.listIterator(triggers.size()); iterator.hasPrevious();) {
            Trigger trigger = iterator.previous();
            createPurgeEvent(targetNode, trigger);
        }
        for (Trigger trigger : triggers) {
            createReloadEvent(targetNode, trigger);
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

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

}
