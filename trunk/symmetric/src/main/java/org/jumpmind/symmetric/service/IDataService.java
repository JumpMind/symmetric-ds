package org.jumpmind.symmetric.service;

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

}
