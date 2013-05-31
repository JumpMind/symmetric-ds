package org.jumpmind.symmetric.service;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;

/**
 * Service API that is responsible for pulling data from the list of configured
 * {@link Node}s that are configured to {@link NodeGroupLinkAction#W}
 */
public interface IPullService extends IOfflineDetectorService {

    public RemoteNodeStatuses pullData(boolean force);

}