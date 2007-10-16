package org.jumpmind.symmetric.service;

import java.util.List;

import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEvent;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;

public interface IDataService {

    public String reloadNode(String nodeId);
    
    public void createReloadEvent(final Node targetNode, final Trigger trigger);

    public void createHeartbeatEvent(Node node);
    
    public long createData(final Data data);
    
    public void createDataEvent(DataEvent dataEvent);

    public void addReloadListener(IReloadListener listener);
    
    public void setReloadListeners(List<IReloadListener> listeners);
    
    public void removeReloadListener(IReloadListener listener);

}
