package org.jumpmind.symmetric.service;

import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;


public interface IRoutingService {

    public void routeData();
    
    public boolean routeInitialLoadData(Data data, Trigger trigger, Node node);
    
}
