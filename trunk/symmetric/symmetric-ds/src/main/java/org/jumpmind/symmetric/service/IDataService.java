package org.jumpmind.symmetric.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.ext.IHeartbeatListener;
import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.DataRef;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.springframework.jdbc.core.JdbcTemplate;

public interface IDataService {

    
    public String reloadNode(String nodeId);

    
    public String reloadTable(String nodeId, String catalogName, String schemaName, String tableName);

    public String reloadTable(String nodeId, String catalogName, String schemaName, String tableName, String overrideInitialLoadSelect);

    public String sendSQL(String nodeId, String catalogName, String schemaName, String tableName, String sql, boolean isLoad);

    public void insertReloadEvent(Node targetNode);

    public void insertReloadEvent(final Node targetNode, final TriggerRouter trigger);
    
    public void sendScript(String nodeId, String script, boolean isLoad);

    /**
     * Update {@link Node} information for this node and call {@link IHeartbeatListener}s.
     */
    public void heartbeat(boolean force);

    public void insertHeartbeatEvent(Node node, boolean isReload);

    public long insertData(final Data data);

    public void insertDataEvent(long dataId, long batchId, String routerId);

    public void insertDataEvent(JdbcTemplate template, long dataId, long batchId, String routerId);
    
    public void insertDataEventAndOutgoingBatch(long dataId, String channelId, String nodeId, DataEventType eventType, String routerId, boolean isLoad);

    public void insertDataAndDataEventAndOutgoingBatch(Data data, String channelId, List<Node> nodes, String routerId, boolean isLoad);

    public void insertDataAndDataEventAndOutgoingBatch(Data data, String nodeId, String routerId, boolean isLoad);

    public void insertPurgeEvent(Node targetNode, TriggerRouter triggerRouter, boolean isLoad);

    public void insertSqlEvent(Node targetNode, Trigger trigger, String sql, boolean isLoad);

    public void insertSqlEvent(final Node targetNode, String sql, boolean isLoad);

    public void insertCreateEvent(Node targetNode, TriggerRouter triggerRouter, String xml, boolean isLoad);
    
    /**
     * Count the number of data ids in a range
     */
    public int countDataInRange(long firstDataId, long secondDataId);

    public void saveDataRef(DataRef dataRef);

    public DataRef getDataRef();

    public Date findCreateTimeOfEvent(long dataId);

    public Data createData(String catalogName, String schemaName, String tableName, boolean isReload);

    public Data createData(String catalogName, String schemaName, String tableName, String whereClause, boolean isReload);

    public Map<String, String> getRowDataAsMap(Data data);

    public void setRowDataFromMap(Data data, Map<String, String> map);

    public void addReloadListener(IReloadListener listener);
    
    public void addHeartbeatListener(IHeartbeatListener listener);

    public void setReloadListeners(List<IReloadListener> listeners);

    public boolean removeReloadListener(IReloadListener listener);

    public Data readData(ResultSet results) throws SQLException;

}
