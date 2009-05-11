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

    /**
     * Open registration for a single new node given a node group (f.e.,
     * "STORE") and external ID (f.e., "00001"). The unique node ID and password
     * are generated and stored in the node and node_security tables with the
     * registration_enabled flag turned on. The next node to try registering for
     * this node group and external ID will be given this information.
     */
    public void openRegistration(String nodeGroupId, String externalId);

    /**
     * Re-open registration for a single node that already exists in the
     * database. A new password is generated and the registration_enabled flag
     * is turned on. The next node to try registering for this node group and
     * external ID will be given this information.
     */
    public void reOpenRegistration(String nodeId);

    /**
     * Mark the passed in node as registered in node_security
     * @param nodeId is the node that has just finished 'successfully' registering
     */
    public void markNodeAsRegistered(String nodeId);
    
    public boolean isAutoRegistration();

    /**
     * Client method which attempts to register with the registration.url to
     * pull configuration if the node has not already been registered. If the
     * registration server cannot be reach this method will continue to try with
     * random sleep periods up to one minute up until the registration succeeds
     * or the maximum number of attempts has been reached.
     */
    public void registerWithServer();

    public boolean isRegisteredWithServer();
    
    /**
     * Add an entry to the registation_redirect table so that if a node tries to register here.  It will be redirected to the correct node.
     */
    public void saveRegistrationRedirect(String externalIdToRedirect, String nodeIdToRedirectTo);

}
