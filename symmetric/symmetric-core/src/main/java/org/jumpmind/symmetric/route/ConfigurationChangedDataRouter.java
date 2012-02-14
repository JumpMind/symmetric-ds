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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.NetworkedNode;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITransformService;
import org.jumpmind.symmetric.service.ITriggerRouterService;

public class ConfigurationChangedDataRouter extends AbstractDataRouter implements IDataRouter {

    final String CTX_KEY_RESYNC_NEEDED = "Resync."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_CHANNELS_NEEDED = "FlushChannels."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_TRANSFORMS_NEEDED = "FlushTransforms."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    public final static String KEY = "symconfig";

    protected IConfigurationService configurationService;

    protected INodeService nodeService;

    protected ITriggerRouterService triggerRouterService;

    protected IParameterService parameterService;

    protected ITransformService transformService;

    protected String tablePrefix;

    public ConfigurationChangedDataRouter() {
    }

    public ConfigurationChangedDataRouter(IConfigurationService configurationService,
            INodeService nodeService, ITriggerRouterService triggerRouterService,
            IParameterService parameterService, ITransformService transformService) {
        this.configurationService = configurationService;
        this.nodeService = nodeService;
        this.triggerRouterService = triggerRouterService;
        this.parameterService = parameterService;
        this.tablePrefix = parameterService.getTablePrefix();
        this.transformService = transformService;
    }

    public Set<String> routeToNodes(SimpleRouterContext routingContext, DataMetaData dataMetaData,
            Set<Node> possibleTargetNodes, boolean initialLoad) {

        // the list of nodeIds that we will return
        Set<String> nodeIds = null;

        // the inbound data
        Map<String, String> columnValues = getDataMap(dataMetaData);

        Node me = findIdentity();
        NetworkedNode rootNetworkedNode = getRootNetworkNodeFromContext(routingContext);

        // if this is sym_node or sym_node_security determine which nodes it
        // goes to.
        if (tableMatches(dataMetaData, TableConstants.SYM_NODE)
                || tableMatches(dataMetaData, TableConstants.SYM_NODE_SECURITY) 
                || tableMatches(dataMetaData, TableConstants.SYM_NODE_HOST)) {

            if (didNodeSecurityChangeForNodeInitialization(dataMetaData)) {
                return null;
            }

            String nodeIdInQuestion = columnValues.get("NODE_ID");
            List<NodeGroupLink> nodeGroupLinks = getNodeGroupLinksFromContext(routingContext);
            for (Node nodeThatMayBeRoutedTo : possibleTargetNodes) {
                if (isLinked(nodeIdInQuestion, nodeThatMayBeRoutedTo, rootNetworkedNode, me,
                        nodeGroupLinks)
                        && !isSameNumberOfLinksAwayFromRoot(nodeThatMayBeRoutedTo,
                                rootNetworkedNode, me)) {
                    if (nodeIds == null) {
                        nodeIds = new HashSet<String>();
                    }
                    nodeIds.add(nodeThatMayBeRoutedTo.getNodeId());
                }
            }
        } else {
            for (Node nodeThatMayBeRoutedTo : possibleTargetNodes) {
                if (!isSameNumberOfLinksAwayFromRoot(nodeThatMayBeRoutedTo, rootNetworkedNode, me)) {
                    if (nodeIds == null) {
                        nodeIds = new HashSet<String>();
                    }
                    nodeIds.add(nodeThatMayBeRoutedTo.getNodeId());
                }
            }

            if (StringUtils.isBlank(dataMetaData.getData().getSourceNodeId())
                    && (tableMatches(dataMetaData, TableConstants.SYM_TRIGGER)
                            || tableMatches(dataMetaData, TableConstants.SYM_TRIGGER_ROUTER)
                            || tableMatches(dataMetaData, TableConstants.SYM_ROUTER) || tableMatches(
                            dataMetaData, TableConstants.SYM_NODE_GROUP_LINK))) {
                routingContext.getContextCache().put(CTX_KEY_RESYNC_NEEDED, Boolean.TRUE);
            }

            if (tableMatches(dataMetaData, TableConstants.SYM_CHANNEL)) {
                routingContext.getContextCache().put(CTX_KEY_FLUSH_CHANNELS_NEEDED, Boolean.TRUE);
            }

            if (tableMatches(dataMetaData, TableConstants.SYM_TRANSFORM_COLUMN)
                    || tableMatches(dataMetaData, TableConstants.SYM_TRANSFORM_TABLE)) {
                routingContext.getContextCache().put(CTX_KEY_FLUSH_TRANSFORMS_NEEDED, Boolean.TRUE);
            }
        }

        return nodeIds;
    }

    /**
     * Check to see if the change was due to registration or initial load is
     * being enabled or disabled. If so, then don't propagate the change.
     */
    protected boolean didNodeSecurityChangeForNodeInitialization(DataMetaData dataMetaData) {
        if (tableMatches(dataMetaData, TableConstants.SYM_NODE_SECURITY)
                && dataMetaData.getData().getDataEventType() == DataEventType.UPDATE) {
            Map<String, String> oldData = getOldDataAsString("", dataMetaData);
            Map<String, String> newData = getNewDataAsString("", dataMetaData);
            if (newData.get("REGISTRATION_ENABLED") != null
                    && !newData.get("REGISTRATION_ENABLED").equals(
                            oldData.get("REGISTRATION_ENABLED"))) {
                return true;
            } else if (newData.get("INITIAL_LOAD_ENABLED") != null
                    && !newData.get("INITIAL_LOAD_ENABLED").equals(
                            oldData.get("INITIAL_LOAD_ENABLED"))) {
                return true;
            }
        }
        return false;
    }

    protected Node findIdentity() {
        return nodeService.findIdentity();
    }

    @SuppressWarnings("unchecked")
    protected List<NodeGroupLink> getNodeGroupLinksFromContext(SimpleRouterContext routingContext) {
        List<NodeGroupLink> list = (List<NodeGroupLink>) routingContext.getContextCache().get(
                NodeGroupLink.class.getName());
        if (list == null) {
            list = configurationService.getNodeGroupLinks();
            routingContext.getContextCache().put(NodeGroupLink.class.getName(), list);
        }
        return list;
    }

    protected NetworkedNode getRootNetworkNodeFromContext(SimpleRouterContext routingContext) {
        NetworkedNode root = (NetworkedNode) routingContext.getContextCache().get(
                NetworkedNode.class.getName());
        if (root == null) {
            root = nodeService.getRootNetworkedNode();
            routingContext.getContextCache().put(NetworkedNode.class.getName(), root);
        }
        return root;
    }

    private boolean isSameNumberOfLinksAwayFromRoot(Node nodeThatCouldBeRoutedTo,
            NetworkedNode root, Node me) {
        return root.getNumberOfLinksAwayFromRoot(nodeThatCouldBeRoutedTo.getNodeId()) == root
                .getNumberOfLinksAwayFromRoot(me.getNodeId());
    }

    private boolean isLinked(String nodeIdInQuestion, Node nodeThatCouldBeRoutedTo,
            NetworkedNode root, Node me, List<NodeGroupLink> allLinks) {
        if (nodeIdInQuestion != null && nodeThatCouldBeRoutedTo != null
                && !nodeIdInQuestion.equals(nodeThatCouldBeRoutedTo.getNodeId())) {
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
                if (createdAtNodeId != null && !createdAtNodeId.equals(me.getNodeId())
                        && !networkedNodeInQuestion.getNode().getNodeId().equals(me.getNodeId())) {
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

    @Override
    public void contextCommitted(SimpleRouterContext routingContext) {
        if (routingContext.getContextCache().get(CTX_KEY_FLUSH_CHANNELS_NEEDED) != null) {
            log.info("Channels flushed because new channels came through the data router");
            configurationService.reloadChannels();
        }
        if (routingContext.getContextCache().get(CTX_KEY_RESYNC_NEEDED) != null
                && parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            log.info("About to syncTriggers because new configuration came through the data router");
            triggerRouterService.syncTriggers();
        }
        if (routingContext.getContextCache().get(CTX_KEY_FLUSH_TRANSFORMS_NEEDED) != null
                && parameterService.is(ParameterConstants.AUTO_SYNC_CONFIGURATION)) {
            log.info("About to refresh the cache of transformation because new configuration come through the data router");
            transformService.resetCache();
        }
    }

    private boolean tableMatches(DataMetaData dataMetaData, String tableName) {
        boolean matches = false;
        if (dataMetaData.getTable().getName()
                .equalsIgnoreCase(TableConstants.getTableName(tablePrefix, tableName))) {
            matches = true;
        }
        return matches;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

}
