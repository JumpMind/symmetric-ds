/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.service;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.config.INodeIdCreator;
import org.jumpmind.symmetric.ext.IOfflineServerListener;
import org.jumpmind.symmetric.io.IOfflineClientListener;
import org.jumpmind.symmetric.model.NetworkedNode;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.NodeHost;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.NodeStatus;
import org.jumpmind.symmetric.security.INodePasswordFilter;

/**
 * This service provides an API to access {@link Node}s and Node related
 * information.
 */
public interface INodeService {

    public Node findNode(String nodeId);

    public List<NodeHost> findNodeHosts(String nodeId);

    public boolean isRegistrationServer();

    public Node findNodeByExternalId(String nodeGroupId, String externalId);

    /**
     * Find a list of {@link Node}s that were create at the passed in node or
     * were created at a node that was created by the passed in node
     * (recursively).
     */
    public Set<Node> findNodesThatOriginatedFromNodeId(String originalNodeId);

    public Set<Node> findNodesThatOriginatedFromNodeId(String originalNodeId, boolean recursive);

    public Collection<Node> findEnabledNodesFromNodeGroup(String nodeGroupId);

    public Collection<Node> findNodesWithOpenRegistration();

    public Map<String, NodeSecurity> findAllNodeSecurity(boolean useCache);

    public List<NodeSecurity> findNodeSecurityWithLoadEnabled();

    public List<String> findAllExternalIds();

    public NodeSecurity findNodeSecurity(String nodeId);

    public NodeSecurity findNodeSecurity(String nodeId, boolean createIfNotFound);
    
    public void deleteNodeHost(String nodeId);

    public void deleteNodeSecurity(String nodeId);

    public void deleteNode(String nodeId, boolean syncChange);

    public String findSymmetricVersion();

    public String findIdentityNodeId();

    public void ignoreNodeChannelForExternalId(boolean ignore, String channelId,
            String nodeGroupId, String externalId);

    public boolean isNodeAuthorized(String nodeId, String password);

    public void flushNodeAuthorizedCache();

    public boolean isRegistrationEnabled(String nodeId);

    public Node findIdentity();

    public Node findIdentity(boolean useCache);

    public Node findIdentity(boolean useCache, boolean logSqlError);

    public Node getCachedIdentity();

    public boolean deleteIdentity();

    public List<Node> findAllNodes();

    public List<Node> findNodesToPull();

    public List<Node> findNodesToPushTo();

    public List<Node> findSourceNodesFor(NodeGroupLinkAction eventAction);

    public List<Node> findTargetNodesFor(NodeGroupLinkAction eventAction);

    public boolean isExternalIdRegistered(String nodeGroupId, String externalId);

    public void save(Node node);

    public void updateNodeHost(NodeHost nodeHost);

    public void updateNodeHostForCurrentNode();

    public void insertNodeIdentity(String nodeId);

    public void insertNodeGroup(String groupId, String description);

    public boolean updateNodeSecurity(NodeSecurity security);

    public boolean updateNodeSecurity(ISqlTransaction transaction, NodeSecurity security);

    public boolean setInitialLoadEnabled(String nodeId, boolean initialLoadEnabled, boolean syncChange, long loadId, String createBy);

    public boolean setInitialLoadEnabled(ISqlTransaction transaction, String nodeId, boolean initialLoadEnabled, boolean syncChange, long loadId, String createBy);

    public boolean setReverseInitialLoadEnabled(ISqlTransaction transaction, String nodeId, boolean initialLoadEnabled, boolean syncChange, long loadId, String createBy);

    public boolean setReverseInitialLoadEnabled(String nodeId, boolean initialLoadEnabled, boolean syncChange, long loadId, String createBy);

    public INodeIdCreator getNodeIdCreator();

    public void setNodeIdCreator(INodeIdCreator nodeIdGenerator);

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
     * Check to see if any nodes are offline and process any nodes found using
     * the configured IOfflineNodeHandler.
     */
    public void checkForOfflineNodes();

    /**
     * Find nodes that have been offline for the configured timeframe before {@link IOfflineClientListener}
     * and {@link IOfflineServerListener} will be called
     *
     * @return list of offline nodes
     */
    public List<Node> findOfflineNodes();

    /**
     * Find nodes that have been offline for a number of minutes
     *
     * @return list of offline nodes
     * @param minutesOffline
     *            the number of minutes that have passed that a node has not
     *            checked in for until it is considered offline
     */
    public List<Node> findOfflineNodes(long minutesOffline);

    public Map<String, Date> findLastHeartbeats();

    public List<String> findOfflineNodeIds(long minutesOffline);

    public void addOfflineServerListener(IOfflineServerListener listener);

    public boolean removeOfflineServerListener(IOfflineServerListener listener);

    public NetworkedNode getRootNetworkedNode();
}