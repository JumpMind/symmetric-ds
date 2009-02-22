/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
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

package org.jumpmind.symmetric.service;

import java.io.IOException;
import java.io.OutputStream;

import org.jumpmind.symmetric.model.Node;

public interface IRegistrationService {

    public boolean registerNode(Node node, OutputStream out, boolean isRequestedRegistration) throws IOException;

    public void openRegistration(String nodeGroupId, String externalId);

    public void reOpenRegistration(String nodeId);

    /**
     * Mark the passed in node as registered in node_security
     * @param nodeId is the node that has just finished 'successfully' registering
     */
    public void markNodeAsRegistered(String nodeId);
    
    public boolean isAutoRegistration();

    public void registerWithServer();

    public boolean isRegisteredWithServer();
    
    /**
     * Add an entry to the registation_redirect table so that if a node tries to register here.  It will be redirected to the correct node.
     */
    public void saveRegistrationRedirect(String externalIdToRedirect, String nodeIdToRedirectTo);

}
