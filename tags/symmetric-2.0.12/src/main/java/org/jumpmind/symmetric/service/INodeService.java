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

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.jumpmind.symmetric.config.INodeIdGenerator;
import org.jumpmind.symmetric.ext.IOfflineServerListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.NodeStatus;
import org.jumpmind.symmetric.security.INodePasswordFilter;

/**
 * This service provides an API to access {@link Node}s and Node related
 * information.
 */
public interface INodeService {

    public Node findNode(String nodeId);
    
    public boolean isRegistrationServer();

    public Node findNodeByExternalId(String nodeGroupId, String externalId);

    /**
     * Find a list of {@link Node}s that were create at the passed in node or
     * were created at a node that was created by the passed in node
     * (recursively).
     */
    public Set<Node> findNodesThatOriginatedFromNodeId(String originalNodeId);
    
    public Collection<Node> findEnabledNodesFromNodeGroup(String nodeGroupId);

    public NodeSecurity findNodeSecurity(String nodeId);
    
    public NodeSecurity findNodeSecurity(String nodeId, boolean createIfNotFound);
    
    public void deleteNodeSecurity(String nodeId);

    public String findSymmetricVersion();

    public String findIdentityNodeId();

    public void ignoreNodeChannelForExternalId(boolean ignore, String channelId, String nodeGroupId, String externalId);

    public boolean isNodeAuthorized(String nodeId, String password);

    public void flushNodeAuthorizedCache();

    public boolean isRegistrationEnabled(String nodeId);

    public Node findIdentity();

    public Node findIdentity(boolean useCache);
    
    public void deleteIdentity();

    public List<Node> findNodesToPull();

    public List<Node> findNodesToPushTo();

    public List<Node> findSourceNodesFor(NodeGroupLinkAction eventAction);

    public List<Node> findTargetNodesFor(NodeGroupLinkAction eventAction);

    public boolean isExternalIdRegistered(String nodeGroupId, String externalId);

    public void insertNode(String nodeId, String nodeGroupdId, String externalId, String createdAtNodeId);
    
    public boolean updateNode(Node node);
    
    public void updateNodeHostForCurrentNode();
    
    public void insertNodeIdentity(String nodeId);
    
    public void insertNodeGroup(String groupId, String description);

    public boolean updateNodeSecurity(NodeSecurity security);

    public boolean setInitialLoadEnabled(String nodeId, boolean initialLoadEnabled);

    public INodeIdGenerator getNodeIdGenerator();

    public void setNodeIdGenerator(INodeIdGenerator nodeIdGenerator);

    public void setNodePasswordFilter(INodePasswordFilter nodePasswordFilter);
    
    /**
     * @return true if a data load has occurred and has been completed.
     */
    public boolean isDataLoadCompleted();

    /**
     * @return true if a data load has started but not yet completed.
     */
    public boolean isDataLoadStarted();
    
    /**
     * Get the current status of this node.
     * 
     * @return {@link NodeStatus}
     */
    public NodeStatus getNodeStatus();
    
    /**
     * Check to see if any nodes are offline and
     * process any nodes found using the configured IOfflineNodeHandler.
     */
    public void checkForOfflineNodes();
    
    /**
     * Find offline nodes.
     * 
     * @return list of offline nodes
     */
    public List<Node> findOfflineNodes();
    
    public void addOfflineServerListener(IOfflineServerListener listener);

    public boolean removeOfflineServerListener(IOfflineServerListener listener);
}