/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Keith Naas <knaas@users.sourceforge.net>
 *               
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
package org.jumpmind.symmetric.transport.handler;

import org.jumpmind.symmetric.service.INodeService;

public class AuthenticationResourceHandler extends AbstractTransportResourceHandler {

    public enum AuthenticationStatus {
        REGISTRATION_REQUIRED, FORBIDDEN, ACCEPTED;
    };

    private INodeService nodeService;

    public AuthenticationStatus status(String nodeId, String securityToken) {
        AuthenticationStatus retVal = AuthenticationStatus.ACCEPTED;

        if (!nodeService.isNodeAuthorized(nodeId, securityToken)) {
            if (nodeService.findNode(nodeId) == null) {
                retVal = AuthenticationStatus.REGISTRATION_REQUIRED;
            } else {
                retVal = AuthenticationStatus.FORBIDDEN;
            }
        }
        return retVal;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

}
