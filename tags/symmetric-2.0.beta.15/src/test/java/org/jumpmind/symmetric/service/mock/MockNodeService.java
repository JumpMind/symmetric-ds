/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Keith Naas <knaas@users.sourceforge.net>
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

package org.jumpmind.symmetric.service.mock;

import java.util.List;
import java.util.Set;

import org.jumpmind.symmetric.config.INodeIdGenerator;
import org.jumpmind.symmetric.ext.IOfflineServerListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.NodeStatus;
import org.jumpmind.symmetric.security.INodePasswordFilter;
import org.jumpmind.symmetric.service.INodeService;

public class MockNodeService implements INodeService {

    public boolean isRegistrationServer() {
        return false;
    }

    public Set<Node> findNodesThatOriginatedFromNodeId(String originalNodeId) {
        return null;
    }

    public Node findIdentity() {
        return null;
    }

    public String findSymmetricVersion() {
        return null;
    }

    public NodeSecurity findNodeSecurity(String nodeId, boolean createIfNotFound) {
        return null;
    }

    public void insertNode(String nodeId, String nodeGroupdId, String externalId, String createdAtNodeId) {
    }

    public void updateNodeHostForCurrentNode() {
    }
    
    public void insertNodeGroup(String groupId, String description) {
    }

    public void insertNodeIdentity(String nodeId) {
    }

    public String findIdentityNodeId() {
        return null;
    }

    public Node findNode(String nodeId) {
        return null;
    }

    public Node findNodeByExternalId(String nodeGroupId, String externalId) {
        return null;
    }

    public NodeSecurity findNodeSecurity(String nodeId) {
        return null;
    }

    public List<Node> findNodesToPull() {
        return null;
    }

    public List<Node> findNodesToPushTo() {
        return null;
    }

    public List<Node> findSourceNodesFor(NodeGroupLinkAction eventAction) {
        return null;
    }

    public List<Node> findTargetNodesFor(NodeGroupLinkAction eventAction) {
        return null;
    }

    public void ignoreNodeChannelForExternalId(boolean ignore, String channelId, String nodeGroupId, String externalId) {

    }

    public boolean isExternalIdRegistered(String nodeGroupId, String externalId) {
        return false;
    }

    public boolean isNodeAuthorized(String nodeId, String password) {
        return false;
    }

    public boolean isRegistrationEnabled(String nodeId) {
        return false;
    }

    public boolean setInitialLoadEnabled(String nodeId, boolean initialLoadEnabled) {
        return false;
    }

    public boolean updateNode(Node node) {
        return false;
    }

    public boolean updateNodeSecurity(NodeSecurity security) {
        return false;
    }

    public Node findIdentity(boolean useCache) {
        return null;
    }

    public String generateNodeId(String nodeGroupId, String externalId) {
        return null;
    }

    public String generatePassword() {
        return null;
    }

    public void flushNodeAuthorizedCache() {
    }

    public INodeIdGenerator getNodeIdGenerator() {
        return null;
    }

    public void setNodeIdGenerator(INodeIdGenerator nodeIdGenerator) {
    }

    public boolean isDataLoadCompleted() {
        return false;
    }

    public boolean isDataLoadStarted() {
        return false;
    }

    public NodeStatus getNodeStatus() {
        return null;
    }

    public void setNodePasswordFilter(INodePasswordFilter nodePasswordFilter) {
    }

    public void checkForOfflineNodes() {        
    }

    public List<Node> findOfflineNodes() {
        return null;
    }

    public void deleteIdentity() {        
    }

    public void deleteNodeSecurity(String nodeId) {        
    }

    public void addOfflineServerListener(IOfflineServerListener listener) {        
    }

    public boolean removeOfflineServerListener(IOfflineServerListener listener) {
        return false;
    }
}