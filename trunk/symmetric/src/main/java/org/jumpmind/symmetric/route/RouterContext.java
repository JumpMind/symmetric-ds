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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.Trigger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.support.JdbcUtils;

public class RouterContext extends SimpleRouterContext implements IRouterContext {

    private Map<String, OutgoingBatch> batchesByNodes = new HashMap<String, OutgoingBatch>();
    private Map<Trigger, Set<Node>> availableNodes = new HashMap<Trigger, Set<Node>>();
    private Set<IDataRouter> usedDataRouters = new HashSet<IDataRouter>();
    private Connection connection;
    private boolean needsCommitted = false;
    private boolean routed = false;

    public RouterContext(String nodeId, NodeChannel channel, DataSource dataSource) throws SQLException {
        this.connection = dataSource.getConnection();
        this.connection.setAutoCommit(false);
        this.init(new JdbcTemplate(new SingleConnectionDataSource(connection, true)), channel, nodeId);
    }

    public Map<String, OutgoingBatch> getBatchesByNodes() {
        return batchesByNodes;
    }

    public Map<Trigger, Set<Node>> getAvailableNodes() {
        return availableNodes;
    }

    public void commit() throws SQLException {
        connection.commit();
        this.usedDataRouters.clear();
        this.encountedTransactionBoundary = false;
        this.batchesByNodes.clear();
        this.availableNodes.clear();
    }

    public void rollback() {
        try {
            connection.rollback();
        } catch (SQLException e) {
            logger.warn(e, e);
        }
    }

    public void cleanup() {
        JdbcUtils.closeConnection(this.connection);
    }

    public void setNeedsCommitted(boolean b) {
        this.needsCommitted = b;
    }

    public void setRouted(boolean b) {
        this.routed = b;
    }

    public boolean isNeedsCommitted() {
        return needsCommitted;
    }

    public boolean isRouted() {
        return routed;
    }
    
    public Set<IDataRouter> getUsedDataRouters() {
        return usedDataRouters;
    }
    
    public void addUsedDataRouter(IDataRouter dataRouter) {
        this.usedDataRouters.add(dataRouter);
    }

    public void resetForNextData() {
        this.routed = false;
        this.needsCommitted = false;
    }
}
