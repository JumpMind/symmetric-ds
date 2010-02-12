/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.IOfflineServerListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.impl.OutgoingBatchService;

/**
 * A default implementation of the Offline Server Listener.  
 * 
 * @author Jeff Bailey
 *
 */
public class DefaultOfflineServerListener implements IOfflineServerListener {

    protected INodeService nodeService;
    protected OutgoingBatchService outgoingBatchService;
    protected static final ILog log = LogFactory.getLog(DefaultOfflineServerListener.class);
    
    /**
     * Handle a client node that was determined to be offline.
     * Syncing is disabled for the node, node security is deleted, and cleanup processing is done for
     * outgoing batches.
     */
    public void clientNodeOffline(Node node) {
        log.warn("NodeOffline", node.getNodeId(), node.getHeartbeatTime(), node.getTimezoneOffset());
        node.setSyncEnabled(false);
        nodeService.updateNode(node);
        outgoingBatchService.markAllAsSentForNode(node);
        nodeService.deleteNodeSecurity(node.getNodeId());
    }

    public boolean isAutoRegister() {
        return true;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }
    
    public void setOutgoingBatchService(OutgoingBatchService outgoingBatchService) {
        this.outgoingBatchService = outgoingBatchService;
    }
}
