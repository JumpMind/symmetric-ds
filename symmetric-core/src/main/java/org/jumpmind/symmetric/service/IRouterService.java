package org.jumpmind.symmetric.service;

import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.route.IBatchAlgorithm;
import org.jumpmind.symmetric.route.IDataRouter;
import org.jumpmind.symmetric.route.SimpleRouterContext;


/**
 * This service is responsible for routing data to specific nodes and managing
 * the batching of data to be delivered to each node.
 * 
 * @since 2.0
 */
public interface IRouterService extends IService {

    public long routeData(boolean force);
 
    public long getUnroutedDataCount();
    
    public boolean shouldDataBeRouted(SimpleRouterContext context, DataMetaData dataMetaData,
            Node node, boolean initialLoad);
 
    public void addDataRouter(String name, IDataRouter dataRouter);
    
    public void addBatchAlgorithm(String name, IBatchAlgorithm algorithm);
    
    /**
     * Get a list of available batch algorithms that can be used for the different channels
     */
    public List<String> getAvailableBatchAlgorithms();
    
    public Map<String, IDataRouter> getRouters();
    
    public void stop ();

}