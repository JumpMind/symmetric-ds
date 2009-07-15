/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
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
package org.jumpmind.symmetric.route;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;
import org.jumpmind.symmetric.model.Trigger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.support.JdbcUtils;

public class RoutingContext implements IRoutingContext {
    
    protected final Log logger = LogFactory.getLog(getClass());
    private NodeChannel channel;
    private Map<String, OutgoingBatch> batchesByNodes = new HashMap<String, OutgoingBatch>();
    private Map<String, OutgoingBatchHistory> batchHistoryByNodes = new HashMap<String, OutgoingBatchHistory>();
    private Map<Trigger, Set<Node>> availableNodes = new HashMap<Trigger, Set<Node>>();
    private Connection connection;
    private JdbcTemplate jdbcTemplate;

    public RoutingContext(NodeChannel channel, Connection connection) throws SQLException {
        this.channel = channel;
        this.connection = connection;
        this.connection.setAutoCommit(false);
        this.jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public NodeChannel getChannel() {
        return channel;
    }

    public Map<String, OutgoingBatch> getBatchesByNodes() {
        return batchesByNodes;
    }

    public Map<String, OutgoingBatchHistory> getBatchHistoryByNodes() {
        return batchHistoryByNodes;
    }

    public Map<Trigger, Set<Node>> getAvailableNodes() {
        return availableNodes;
    }

    public void commit() throws SQLException {
        connection.commit();
    }

    public void rollback() {
        try {
            connection.rollback();
        } catch (SQLException e) {
            logger.warn(e,e);
        }
    }

    public void cleanup() {
        JdbcUtils.closeConnection(this.connection);
    }

}
