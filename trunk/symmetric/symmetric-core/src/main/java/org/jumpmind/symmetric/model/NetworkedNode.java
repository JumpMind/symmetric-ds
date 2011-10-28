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
 * under the License. 
 */
package org.jumpmind.symmetric.model;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class NetworkedNode implements Comparable<NetworkedNode> {

    private Node node;

    private NetworkedNode parent;

    private Set<NetworkedNode> children;

    public NetworkedNode(Node node) {
        this.node = node;
    }

    public void addChild(NetworkedNode node) {
        if (children == null) {
            children = new TreeSet<NetworkedNode>();
        }
        node.parent = this;
        children.add(node);
    }

    public Node getNode() {
        return node;
    }

    public NetworkedNode getParent() {
        return parent;
    }

    public NetworkedNode findNetworkedNode(String nodeId) {
        if (this.node.getNodeId().equals(nodeId)) {
            return this;
        } else {
            if (children != null) {
                for (NetworkedNode child : children) {
                    if (child.getNode().getNodeId().equals(nodeId)) {
                        return child;
                    } else {
                        NetworkedNode foundIt = child.findNetworkedNode(nodeId);
                        if (foundIt != null) {
                            return foundIt;
                        }
                    }
                }
            }
        }
        return null;
    }

    public boolean isInParentHierarchy(String nodeId) {
        if (parent != null) {
            if (parent.getNode().getNodeId().equals(nodeId)) {
                return true;
            } else {
                return parent.isInParentHierarchy(nodeId);
            }
        } else {
            return false;
        }
    }

    public boolean hasChildrenThatBelongToGroups(Set<String> groupIds) {
        if (children != null) {
            for (NetworkedNode child : children) {
                if (groupIds.contains(child.getNode().getNodeGroupId())) {
                    return true;
                } else {
                    if (child.hasChildrenThatBelongToGroups(groupIds)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public boolean isInChildHierarchy(String nodeId) {
        if (children != null) {
            for (NetworkedNode child : children) {
                if (child.getNode().getNodeId().equals(nodeId)) {
                    return true;
                } else {
                    if (child.isInChildHierarchy(nodeId)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public void addParents(Map<String, Node> nodes, Map<String, NetworkedNode> leaves) {
        String parentNodeId = node.getCreatedAtNodeId();
        NetworkedNode parentNetworkedNode = leaves.get(parentNodeId);
        if (parentNetworkedNode == null) {
            Node parentNode = nodes.get(parentNodeId);
            if (parentNode != null) {
                parentNetworkedNode = new NetworkedNode(parentNode);
                parentNetworkedNode.addParents(nodes, leaves);
                leaves.put(parentNodeId, parentNetworkedNode);
            }
        }

        if (parentNetworkedNode != null) {
            parentNetworkedNode.addChild(this);
        }
        this.parent = parentNetworkedNode;
    }

    public NetworkedNode getRoot() {
        if (parent != null) {
            return parent.getRoot();
        } else {
            return this;
        }
    }

    public int compareTo(NetworkedNode o) {
        return node.getNodeId().compareTo(o.getNode().getNodeId());
    }

}
