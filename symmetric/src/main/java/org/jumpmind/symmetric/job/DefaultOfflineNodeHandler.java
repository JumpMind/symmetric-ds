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

import java.util.List;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.IOfflineNodeHandler;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.impl.OutgoingBatchService;

/**
 * A default implementation of the Offline Node Handler.  Syncing
 * is disabled for the node and cleanup processing is done for
 * outgoing batches.
 * 
 * @author Jeff Bailey
 *
 */
public class DefaultOfflineNodeHandler implements IOfflineNodeHandler {

    protected INodeService nodeService;
    protected OutgoingBatchService outgoingBatchService;
    protected static final ILog log = LogFactory.getLog(DefaultOfflineNodeHandler.class);
    
    public void handleOfflineNodes(List<Node> offlineNodeList) {
        for (Node node : offlineNodeList) {
            log.info("NodeOffline", node.getNodeId(), node.getHeartbeatTime());
            node.setSyncEnabled(false);
            nodeService.updateNode(node);
            outgoingBatchService.markAllAsSentForNode(node);
        }
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
