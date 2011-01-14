/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */


package org.jumpmind.symmetric.service.mock;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.config.INodeIdGenerator;
import org.jumpmind.symmetric.ext.IOfflineServerListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.NodeHost;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.NodeStatus;
import org.jumpmind.symmetric.security.INodePasswordFilter;
import org.jumpmind.symmetric.service.INodeService;

public class MockNodeService implements INodeService {
    
    public Node getCachedIdentity() {
        return null;
    }
    
    public List<Node> findAllNodes() {
        return null;
    }
    
    public boolean isRegistrationServer() {
        return false;
    }

    public Set<Node> findNodesThatOriginatedFromNodeId(String originalNodeId) {
        return null;
    }
    
    public Set<Node> findNodesThatOriginatedFromNodeId(String originalNodeId, boolean recursive) {
        return null;
    }
    
    public Collection<Node> findNodesWithOpenRegistration() {
        return null;
    }
    
    public List<NodeHost> findNodeHosts(String nodeId) {
        return null;
    }

    public Node findIdentity() {
        return null;
    }

    public Collection<Node> findEnabledNodesFromNodeGroup(String nodeGroupId) {
        return null;
    }
    
    public Map<String, NodeSecurity> findAllNodeSecurity(boolean useCache) {
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
    
    public List<Node> findOfflineNodes(long minutesOffline) {
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