package org.jumpmind.symmetric.service;

import java.util.Date;
import java.util.Map;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;

/**
 * Service API that is responsible for pushing data to the list of configured
 * {@link Node}s that are configured to {@link NodeGroupLinkAction#P}
 */
public interface IPushService extends IOfflineDetectorService {

    /**
     * Attempt to push data, if any has been captured, to nodes that the
     * captured data is targeted for.
     * @param force TODO
     * 
     * @return RemoteNodeStatuses the status of the push attempt(s)
     */
    public RemoteNodeStatuses pushData(boolean force);
    
    public Map<String, Date> getStartTimesOfNodesBeingPushedTo();

}