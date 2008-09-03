/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Andrew Wilcox <andrewbwilcox@users.sourceforge.net>,
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

import java.util.List;

import org.jumpmind.symmetric.model.DataEventAction;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;

public interface INodeService {

    public Node findNode(String nodeId);

    public Node findNodeByExternalId(String nodeGroupId, String externalId);

    public NodeSecurity findNodeSecurity(String nodeId);

    public void ignoreNodeChannelForExternalId(boolean ignore, String channelId, String nodeGroupId, String externalId);

    public boolean isNodeAuthorized(String nodeId, String password);
    
    public void flushNodeAuthorizedCache();

    public boolean isRegistrationEnabled(String nodeId);

    public Node findIdentity();

    public Node findIdentity(boolean useCache);

    public List<Node> findNodesToPull();

    public List<Node> findNodesToPushTo();

    public List<Node> findSourceNodesFor(DataEventAction eventAction);

    public List<Node> findTargetNodesFor(DataEventAction eventAction);

    public boolean isExternalIdRegistered(String nodeGroupId, String externalId);

    public boolean updateNode(Node node);

    public boolean updateNodeSecurity(NodeSecurity security);

    public boolean setInitialLoadEnabled(String nodeId, boolean initialLoadEnabled);
    
    public String generatePassword();

    /**
     * Generate the next node ID that is available. Try to use the domain ID as
     * the node ID.
     */
    public String generateNodeId(String nodeGroupId, String externalId);

}
