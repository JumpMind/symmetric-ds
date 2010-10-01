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

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.support.JdbcUtils;

public class RouterContext extends SimpleRouterContext implements IRouterContext {

    static final ILog log = LogFactory.getLog(RouterContext.class);
    
    public static final String STAT_INSERT_DATA_EVENTS_MS = "insert.data.events.ms";
    public static final String STAT_DATA_ROUTER_MS = "data.router.ms";
    public static final String STAT_READ_DATA_MS = "read.data.ms";
    public static final String STAT_REREAD_DATA_MS = "already.read.data.ms";
    public static final String STAT_READ_RESULT_WAIT_TIME_MS = "read.result.wait.time.ms";
    public static final String STAT_ENQUEUE_DATA_MS = "enqueue.data.ms";
    public static final String STAT_ENQUEUE_EOD_MS = "enqueue.eod.data.ms";

    private Map<String, OutgoingBatch> batchesByNodes = new HashMap<String, OutgoingBatch>();
    private Map<TriggerRouter, Set<Node>> availableNodes = new HashMap<TriggerRouter, Set<Node>>();
    private Set<IDataRouter> usedDataRouters = new HashSet<IDataRouter>();
    private Connection connection;
    private boolean needsCommitted = false;
    private long createdTimeInMs = System.currentTimeMillis();
    private Map<String, Long> transactionIdDataIds = new HashMap<String, Long>();
    private boolean oldAutoCommitSetting = false;

    public RouterContext(String nodeId, NodeChannel channel, DataSource dataSource)
            throws SQLException {
        this.connection = dataSource.getConnection();
        this.oldAutoCommitSetting = this.connection.getAutoCommit();
        this.connection.setAutoCommit(false);
        this.init(new JdbcTemplate(new SingleConnectionDataSource(connection, true)), channel,
                nodeId);
    }

    public Map<String, OutgoingBatch> getBatchesByNodes() {
        return batchesByNodes;
    }

    public Map<TriggerRouter, Set<Node>> getAvailableNodes() {
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
            LogFactory.getLog(getClass()).warn(e);
        }
    }

    public void cleanup() {
        try {
            this.connection.commit();
            this.connection.setAutoCommit(oldAutoCommitSetting);
        } catch (Exception ex) {
            log.warn(ex);
        }
        JdbcUtils.closeConnection(this.connection);
    }

    public void setNeedsCommitted(boolean b) {
        this.needsCommitted = b;
    }

    public boolean isNeedsCommitted() {
        return needsCommitted;
    }

    public Set<IDataRouter> getUsedDataRouters() {
        return usedDataRouters;
    }

    public void addUsedDataRouter(IDataRouter dataRouter) {
        this.usedDataRouters.add(dataRouter);
    }

    public void resetForNextData() {
        this.needsCommitted = false;
    }

    public long getCreatedTimeInMs() {
        return createdTimeInMs;
    }

    public void setLastDataIdForTransactionId(Data data) {
        if (data.getTransactionId() != null) {
            this.transactionIdDataIds.put(data.getTransactionId(), data.getDataId());
        }
    }

    public void recordTransactionBoundaryEncountered(Data data) {
        Long dataId = transactionIdDataIds.get(data.getTransactionId());
        setEncountedTransactionBoundary(dataId == null ? true : dataId == data.getDataId());
    }

}
