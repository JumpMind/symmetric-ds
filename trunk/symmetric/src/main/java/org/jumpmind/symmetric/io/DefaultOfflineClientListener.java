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
package org.jumpmind.symmetric.io;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.impl.NodeService;

public class DefaultOfflineClientListener implements IOfflineClientListener {
    
    protected final ILog log = LogFactory.getLog(getClass());
    protected IParameterService parameterService;
    protected INodeService nodeService;


    public void busy(Node remoteNode) {
        log.warn("TransportFailedConnectionBusy");
    }

    public void notAuthenticated(Node remoteNode) {
        log.warn("AuthenticationFailed");
    }

    public void offline(Node remoteNode) {
        log.warn("TransportFailedConnectionUnavailable",
                (remoteNode.getSyncUrl() == null ? parameterService.getRegistrationUrl() : remoteNode
                        .getSyncUrl()));
    }

    public void syncDisabled(Node remoteNode) {
        log.warn("SyncDisabled");
        nodeService.deleteIdentity();
    }
    
    public void registrationRequired(Node remoteNode) {
        log.warn("RegistrationRequired");
        nodeService.deleteIdentity();
    }

    public boolean isAutoRegister() {
        return true;
    }
    
    public void setNodeService(NodeService nodeService) {
        this.nodeService = nodeService;
    }
    
    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }
}
