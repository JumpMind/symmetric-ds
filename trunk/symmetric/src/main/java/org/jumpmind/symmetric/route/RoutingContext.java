package org.jumpmind.symmetric.route;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;
import org.jumpmind.symmetric.model.Trigger;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.support.JdbcUtils;

public class RoutingContext implements IRoutingContext {
    
    private NodeChannel channel;
    private Map<String, OutgoingBatch> batchesByNodes = new HashMap<String, OutgoingBatch>();
    private Map<String, OutgoingBatchHistory> batchHistoryByNodes = new HashMap<String, OutgoingBatchHistory>();
    private Map<Trigger, Set<Node>> availableNodes = new HashMap<Trigger, Set<Node>>();
    private Map<String, IDataRouter> dataRouters = new HashMap<String, IDataRouter>();
    private Connection connection;
    private SingleConnectionDataSource dataSource;
    
    public RoutingContext(NodeChannel channel, Connection connection) throws SQLException {
        this.channel = channel;
        this.connection = connection;
        this.connection.setAutoCommit(false);
        this.dataSource = new SingleConnectionDataSource(connection, true);
    }
    
    public SingleConnectionDataSource getDataSource() {
        return dataSource;
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
    
    public Map<String, IDataRouter> getDataRouters() {
        return dataRouters;
    }
    
    public void commit() throws SQLException {
        connection.commit();
    }
    
    public void cleanup() {
     JdbcUtils.closeConnection(this.connection);        
    }
    
}
