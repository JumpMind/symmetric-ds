package org.jumpmind.symmetric.service;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;

public interface IDataService {

    public void reloadNode(String nodeId);
    
    public void createReloadEvent(final Node targetNode, final Trigger trigger);

}
