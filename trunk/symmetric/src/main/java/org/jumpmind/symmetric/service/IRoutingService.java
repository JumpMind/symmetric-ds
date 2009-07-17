package org.jumpmind.symmetric.service;

import java.util.Set;

import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;


public interface IRoutingService {

    public void routeData();
    
    public boolean shouldDataBeRouted(DataMetaData dataMetaData, Set<Node> nodes, boolean initialLoad);
    
}
