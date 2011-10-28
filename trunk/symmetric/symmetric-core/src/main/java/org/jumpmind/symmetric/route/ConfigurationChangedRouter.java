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
package org.jumpmind.symmetric.route;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.NetworkedNode;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;

public class ConfigurationChangedRouter extends AbstractDataRouter implements IDataRouter {

    public final static String KEY = "symconfig";

    protected String tablePrefix;

    private IConfigurationService configurationService;

    private INodeService nodeService;

    public Collection<String> routeToNodes(IRouterContext routingContext,
            DataMetaData dataMetaData, Set<Node> possibleTargetNodes, boolean initialLoad) {

        // the list of nodeIds that we will return
        Set<String> nodeIds = null;

        // the inbound data
        Map<String, String> columnValues = getDataMap(dataMetaData);

        // if this is sym_node or sym_node_security determine which nodes it
        // goes to.
        if (tableMatches(dataMetaData, TableConstants.SYM_NODE)
                || tableMatches(dataMetaData, TableConstants.SYM_NODE_SECURITY)) {

            String nodeIdInQuestion = columnValues.get("NODE_ID");
            Node me = findIdentity();
            NetworkedNode rootNetworkedNode = getRootNetworkNodeFromContext(routingContext);
            List<NodeGroupLink> nodeGroupLinks = getNodeGroupLinksFromContext(routingContext);
            for (Node nodeThatMayBeRoutedTo : possibleTargetNodes) {
                if (isLinked(nodeIdInQuestion, nodeThatMayBeRoutedTo, rootNetworkedNode, me,
                        nodeGroupLinks)) {
                    if (nodeIds == null) {
                        nodeIds = new HashSet<String>();
                    }
                    nodeIds.add(nodeThatMayBeRoutedTo.getNodeId());
                }
            }
        } else {
            nodeIds = toNodeIds(possibleTargetNodes, nodeIds);
        }

        return nodeIds;
    }
    
    protected Node findIdentity() {
        return nodeService.findIdentity();
    }

    @SuppressWarnings("unchecked")
    protected List<NodeGroupLink> getNodeGroupLinksFromContext(IRouterContext routingContext) {
        List<NodeGroupLink> list = (List<NodeGroupLink>) routingContext.getContextCache().get(
                NodeGroupLink.class.getName());
        if (list == null) {
            list = configurationService.getNodeGroupLinks();
            routingContext.getContextCache().put(NodeGroupLink.class.getName(), list);
        }
        return list;
    }

    protected NetworkedNode getRootNetworkNodeFromContext(IRouterContext routingContext) {
        NetworkedNode root = (NetworkedNode) routingContext.getContextCache().get(
                NetworkedNode.class.getName());
        if (root == null) {
            root = nodeService.getRootNetworkedNode();
            routingContext.getContextCache().put(NetworkedNode.class.getName(), root);
        }
        return root;
    }

    private boolean isLinked(String nodeIdInQuestion, Node nodeThatCouldBeRoutedTo,
            NetworkedNode root, Node me, List<NodeGroupLink> allLinks) {
        if (!nodeIdInQuestion.equals(nodeThatCouldBeRoutedTo.getNodeId())) {
            NetworkedNode networkedNodeInQuestion = root.findNetworkedNode(nodeIdInQuestion);
            NetworkedNode networkedNodeThatCouldBeRoutedTo = root
                    .findNetworkedNode(nodeThatCouldBeRoutedTo.getNodeId());
            if (networkedNodeInQuestion != null) {
                if (networkedNodeInQuestion
                        .isInParentHierarchy(nodeThatCouldBeRoutedTo.getNodeId())) {
                    // always route changes to parent nodes
                    return true;
                }
                String createdAtNodeId = networkedNodeInQuestion.getNode().getCreatedAtNodeId();
                if (createdAtNodeId != null && !createdAtNodeId.equals(me.getNodeId())) {
                    if (createdAtNodeId.equals(nodeThatCouldBeRoutedTo.getNodeId())) {
                        return true;
                    } else {
                        // the node was created at some other node. lets attempt
                        // to get that update back to that node
                        return networkedNodeThatCouldBeRoutedTo.isInChildHierarchy(createdAtNodeId);
                    }
                }

                // if we haven't found a place to route by now, then we need to
                // send the row to all nodes that have links to the node's group
                String groupId = networkedNodeInQuestion.getNode().getNodeGroupId();
                Set<String> groupsThatWillBeInterested = new HashSet<String>();
                for (NodeGroupLink nodeGroupLink : allLinks) {
                    if (nodeGroupLink.getTargetNodeGroupId().equals(groupId)) {
                        groupsThatWillBeInterested.add(nodeGroupLink.getSourceNodeGroupId());
                    } else if (nodeGroupLink.getSourceNodeGroupId().equals(groupId)) {
                        groupsThatWillBeInterested.add(nodeGroupLink.getTargetNodeGroupId());
                    }
                }

                if (groupsThatWillBeInterested.contains(nodeThatCouldBeRoutedTo.getNodeGroupId())) {
                    return true;
                } else {
                    return networkedNodeThatCouldBeRoutedTo
                            .hasChildrenThatBelongToGroups(groupsThatWillBeInterested);
                }
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    public void completeBatch(IRouterContext context, OutgoingBatch batch) {
        // TODO resync triggers if sym_trigger, sym_trigger_router or sym_router
        // has changed we could do a synch triggers call here when we know these are
        // changing
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    private boolean tableMatches(DataMetaData dataMetaData, String tableName) {
        boolean matches = false;
        if (dataMetaData.getTable().getName()
                .equalsIgnoreCase(TableConstants.getTableName(tablePrefix, tableName))) {
            matches = true;
        }
        return matches;
    }

    // TODO: This guy is going to replace the initial load selects for sym_node
    // and sym_node_security found in
    // triggerrouter-service.xml. The nodes variable has all eligible nodes that
    // can be sync'd to.
    // Go through them all and figure out if the sym_node or sym_node_security
    // rows should be synced. If so,
    // return the nodeid in the returned collection. - same criteira as the
    // initial load sql should be implemented in code here

    // DONE - if the configuration table is something other than node or
    // security, then return all node ids (configuration
    // goes everywhere.
    //
    // STILL USED IN TRIGGER ROUTER SERVICE
    // - this router is configured in symmetric-routers.xml. it will be used in
    // TriggerRouterService.buildRegistrationTriggerRouter()
    // we can get rid of rootConfigChannelInitialLoadSelect in
    // triggerrouter-service.xml

    // TODO: side note: if the external id of a node exists in
    // registration_redirect, then we should sync that node only
    // to the registration_node_id.

    // TODO: another other side node: we should put some indicator into the
    // context if sym_trigger, sym_trigger_router, or sym_router
    // changes so we can run syncTriggers when the batch is completed.

}
