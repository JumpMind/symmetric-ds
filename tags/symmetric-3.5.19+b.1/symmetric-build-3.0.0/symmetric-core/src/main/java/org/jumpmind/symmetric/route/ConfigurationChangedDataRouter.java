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
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.NetworkedNode;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;

public class ConfigurationChangedDataRouter extends AbstractDataRouter implements IDataRouter {

    final String CTX_KEY_RESYNC_NEEDED = "Resync."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_CHANNELS_NEEDED = "FlushChannels."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_TRANSFORMS_NEEDED = "FlushTransforms."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_PARAMETERS_NEEDED = "FlushParameters."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_CONFLICTS_NEEDED = "FlushConflicts."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_RESTART_JOBMANAGER_NEEDED = "RestartJobManager."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_RESTART_NODE_COMMUNICATOR_NEEDED = "RestartNodeCommunicatorThreadPool."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    public final static String KEY = "symconfig";

    protected ISymmetricEngine engine;

    public ConfigurationChangedDataRouter() {
    }

    public ConfigurationChangedDataRouter(ISymmetricEngine engine) {
        this.engine = engine;
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

            String nodeIdInQuestion = columnValues.get("NODE_ID");
            List<NodeGroupLink> nodeGroupLinks = getNodeGroupLinksFromContext(routingContext);
            for (Node nodeThatMayBeRoutedTo : possibleTargetNodes) {
                if (isLinked(nodeIdInQuestion, nodeThatMayBeRoutedTo, rootNetworkedNode, me,
                        nodeGroupLinks)
                        && !isSameNumberOfLinksAwayFromRoot(nodeThatMayBeRoutedTo,
                                rootNetworkedNode, me)
                        || (nodeThatMayBeRoutedTo.getNodeId().equals(me.getNodeId()) && initialLoad)) {
                    if (nodeIds == null) {
                        nodeIds = new HashSet<String>();
                    }
                    nodeIds.add(nodeThatMayBeRoutedTo.getNodeId());
                }
            }

            if (!initialLoad && nodeIds != null) {
                /*
                 * don't route node security to it's own node. that node will
                 * get node security via registration and it will be updated by
                 * initial load
                 */
                if (tableMatches(dataMetaData, TableConstants.SYM_NODE_SECURITY)) {

                    nodeIds.remove(nodeIdInQuestion);
                }
                /*
                 * don't route insert events for a node to itself. they will be
                 * loaded during
                 */
                if (dataMetaData.getData().getDataEventType() == DataEventType.INSERT) {
                    nodeIds.remove(nodeIdInQuestion);
                }
            }
        } else {
            for (Node nodeThatMayBeRoutedTo : possibleTargetNodes) {
                if (!isSameNumberOfLinksAwayFromRoot(nodeThatMayBeRoutedTo, rootNetworkedNode, me)
                        || (nodeThatMayBeRoutedTo.getNodeId().equals(me.getNodeId()) && initialLoad)) {
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

            if (tableMatches(dataMetaData, TableConstants.SYM_CONFLICT)) {
                routingContext.getContextCache().put(CTX_KEY_FLUSH_CONFLICTS_NEEDED, Boolean.TRUE);
            }

            if (tableMatches(dataMetaData, TableConstants.SYM_PARAMETER)) {
                routingContext.getContextCache().put(CTX_KEY_FLUSH_PARAMETERS_NEEDED, Boolean.TRUE);

                if (dataMetaData.getData().getRowData() != null
                        && dataMetaData.getData().getRowData().contains("job.")) {
                    routingContext.getContextCache().put(CTX_KEY_RESTART_JOBMANAGER_NEEDED,
                            Boolean.TRUE);
                }

                if (dataMetaData.getData().getRowData() != null
                        && (dataMetaData.getData().getRowData()
                                .contains(ParameterConstants.PULL_THREAD_COUNT_PER_SERVER)
                        || dataMetaData.getData().getRowData()
                                .contains(ParameterConstants.PUSH_THREAD_COUNT_PER_SERVER))) {
                    routingContext.getContextCache().put(CTX_KEY_RESTART_NODE_COMMUNICATOR_NEEDED,
                            Boolean.TRUE);
                }
            }

            if (tableMatches(dataMetaData, TableConstants.SYM_TRANSFORM_COLUMN)
                    || tableMatches(dataMetaData, TableConstants.SYM_TRANSFORM_TABLE)) {
                routingContext.getContextCache().put(CTX_KEY_FLUSH_TRANSFORMS_NEEDED, Boolean.TRUE);
            }
        }

        return nodeIds;
    }

    protected Node findIdentity() {
        return engine.getNodeService().findIdentity();
    }

    @SuppressWarnings("unchecked")
    protected List<NodeGroupLink> getNodeGroupLinksFromContext(SimpleRouterContext routingContext) {
        List<NodeGroupLink> list = (List<NodeGroupLink>) routingContext.getContextCache().get(
                NodeGroupLink.class.getName());
        if (list == null) {
            list = engine.getConfigurationService().getNodeGroupLinks();
            routingContext.getContextCache().put(NodeGroupLink.class.getName(), list);
        }
        return list;
    }

    protected NetworkedNode getRootNetworkNodeFromContext(SimpleRouterContext routingContext) {
        NetworkedNode root = (NetworkedNode) routingContext.getContextCache().get(
                NetworkedNode.class.getName());
        if (root == null) {
            root = engine.getNodeService().getRootNetworkedNode();
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
                    return false;
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

        if (engine.getParameterService().is(ParameterConstants.AUTO_REFRESH_AFTER_CONFIG_CHANGED,
                true)) {
            if (routingContext.getContextCache().get(CTX_KEY_FLUSH_PARAMETERS_NEEDED) != null
                    && engine.getParameterService().is(ParameterConstants.AUTO_SYNC_CONFIGURATION)) {
                log.info("About to refresh the cache of parameters because new configuration came through the data router");
                engine.getParameterService().rereadParameters();
            }

            if (routingContext.getContextCache().get(CTX_KEY_FLUSH_CHANNELS_NEEDED) != null) {
                log.info("Channels flushed because new channels came through the data router");
                engine.getConfigurationService().reloadChannels();
            }

            if (routingContext.getContextCache().get(CTX_KEY_RESYNC_NEEDED) != null
                    && engine.getParameterService().is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
                log.info("About to syncTriggers because new configuration came through the data router");
                engine.getTriggerRouterService().syncTriggers();
            }

            if (routingContext.getContextCache().get(CTX_KEY_FLUSH_TRANSFORMS_NEEDED) != null) {
                log.info("About to refresh the cache of transformation because new configuration came through the data router");
                engine.getTransformService().resetCache();
            }

            if (routingContext.getContextCache().get(CTX_KEY_FLUSH_CONFLICTS_NEEDED) != null) {
                log.info("About to refresh the cache of conflict settings because new configuration came through the data router");
                engine.getDataLoaderService().reloadConflictNodeGroupLinks();
            }

            if (routingContext.getContextCache().get(CTX_KEY_RESTART_NODE_COMMUNICATOR_NEEDED) != null) {
                log.info("About to reset the thread pools used to communicate with nodes because the thread pool definition changed");
                engine.getNodeCommunicationService().stop();
            }

            if (routingContext.getContextCache().get(CTX_KEY_RESTART_JOBMANAGER_NEEDED) != null) {
                IJobManager jobManager = engine.getJobManager();
                if (jobManager != null) {
                    log.info("About to restart jobs because new configuration come through the data router");
                    jobManager.stopJobs();
                    jobManager.startJobs();
                }

            }
        }

    }

    private boolean tableMatches(DataMetaData dataMetaData, String tableName) {
        boolean matches = false;
        if (dataMetaData
                .getTable()
                .getName()
                .equalsIgnoreCase(
                        TableConstants.getTableName(engine != null ? engine.getTablePrefix()
                                : "sym", tableName))) {
            matches = true;
        }
        return matches;
    }

}
