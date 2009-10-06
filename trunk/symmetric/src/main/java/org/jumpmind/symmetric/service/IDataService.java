package org.jumpmind.symmetric.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.ext.IHeartbeatListener;
import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataRef;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

public interface IDataService {

    @Transactional
    public String reloadNode(String nodeId);

    @Transactional
    public String reloadTable(String nodeId, String tableName);

    @Transactional
    public String reloadTable(String nodeId, String tableName, String overrideInitialLoadSelect);

    @Transactional
    public String sendSQL(String nodeId, String tableName, String sql);

    @Transactional
    public void insertReloadEvent(Node targetNode);

    @Transactional
    public void insertReloadEvent(final Node targetNode, final TriggerRouter trigger);

    @Transactional
    public void insertResendConfigEvent(final Node targetNode);

    /**
     * Update {@link Node} information for this node and call {@link IHeartbeatListener}s.
     */
    @Transactional
    public void heartbeat();

    public void insertHeartbeatEvent(Node node);

    public long insertData(final Data data);

    public void insertDataEvent(long dataId, long batchId, String routerId);

    public void insertDataEvent(JdbcTemplate template, long dataId, long batchId, String routerId);

    public void insertDataAndDataEvent(Data data, String channelId, List<Node> nodes, String routerId);

    public void insertDataAndDataEvent(Data data, String nodeId, String routerId);

    public void insertPurgeEvent(Node targetNode, TriggerRouter triggerRouter);

    public void insertSqlEvent(Node targetNode, Trigger trigger, String sql);

    public void insertSqlEvent(final Node targetNode, String sql);

    public void insertCreateEvent(Node targetNode, TriggerRouter triggerRouter, String xml);

    public void saveDataRef(DataRef dataRef);

    public DataRef getDataRef();

    public Date findCreateTimeOfEvent(long dataId);

    public Data createData(String tableName);

    public Data createData(String tableName, String whereClause);

    public Map<String, String> getRowDataAsMap(Data data);

    public void setRowDataFromMap(Data data, Map<String, String> map);

    public void addReloadListener(IReloadListener listener);
    
    public void addHeartbeatListener(IHeartbeatListener listener);

    public void setReloadListeners(List<IReloadListener> listeners);

    public boolean removeReloadListener(IReloadListener listener);

    public Data readData(ResultSet results) throws SQLException;

}
