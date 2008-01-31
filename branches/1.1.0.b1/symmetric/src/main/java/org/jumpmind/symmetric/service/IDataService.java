package org.jumpmind.symmetric.service;

import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEvent;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.springframework.transaction.annotation.Transactional;

public interface IDataService {

    @Transactional
    public String reloadNode(String nodeId);
    
    public void insertReloadEvent(final Node targetNode, final Trigger trigger);

    public void insertHeartbeatEvent(Node node);
    
    public long insertData(final Data data);
    
    public void insertDataEvent(DataEvent dataEvent);

    public void insertDataEvent(Data data, List<Node> nodes);       

    public void insertDataEvent(Data data, String nodeId);

    public Data createData(String tableName);
    
    public Data createData(String tableName, String whereClause);
    
    public String[] tokenizeCsvData(String csvData);

    public Map<String, String> getRowDataAsMap(Data data);
    
    public void setRowDataFromMap(Data data, Map<String, String> map);
    
    public void addReloadListener(IReloadListener listener);
    
    public void setReloadListeners(List<IReloadListener> listeners);
    
    public void removeReloadListener(IReloadListener listener);

}
