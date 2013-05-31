
package org.jumpmind.symmetric.job;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ext.IOfflineServerListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * The default implementation of the {@link IOfflineServerListener}.  
 */
public class DefaultOfflineServerListener implements IOfflineServerListener,
 IBuiltInExtensionPoint {

    protected final static Logger log = LoggerFactory.getLogger(DefaultOfflineServerListener.class);

    protected IStatisticManager statisticManager;
    protected INodeService nodeService;
    protected IOutgoingBatchService outgoingBatchService;
    
    public DefaultOfflineServerListener(IStatisticManager statisticManager,
            INodeService nodeService, IOutgoingBatchService outgoingBatchService) {        
        this.statisticManager = statisticManager;
        this.nodeService = nodeService;
        this.outgoingBatchService = outgoingBatchService;
    }

    /*
     * Handle a client node that was determined to be offline.
     * Syncing is disabled for the node, node security is deleted, and cleanup processing is done for
     * outgoing batches.
     */
    public void clientNodeOffline(Node node) {
        log.warn("The '{}' node is offline.  Syncing will be disabled and node security deleted", new Object[] {node.getNodeId()});
        statisticManager.incrementNodesDisabled(1);
        node.setSyncEnabled(false);
        nodeService.save(node);
        outgoingBatchService.markAllAsSentForNode(node.getNodeId());
        nodeService.deleteNodeSecurity(node.getNodeId());
    }
    
}
