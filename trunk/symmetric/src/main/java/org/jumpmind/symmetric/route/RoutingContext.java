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
