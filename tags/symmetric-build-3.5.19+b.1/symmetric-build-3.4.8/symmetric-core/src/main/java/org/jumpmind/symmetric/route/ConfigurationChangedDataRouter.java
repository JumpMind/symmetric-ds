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

import java.util.ArrayList;
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
import org.jumpmind.symmetric.load.ConfigurationChangedFilter;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.NetworkedNode;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.TableReloadRequest;
import org.jumpmind.symmetric.model.TableReloadRequestKey;

public class ConfigurationChangedDataRouter extends AbstractDataRouter implements IDataRouter {
    
    public static final String ROUTER_TYPE = "configurationChanged";

    final String CTX_KEY_TABLE_RELOAD_NEEDED = "Reload.Table."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();
    
    final String CTX_KEY_RESYNC_NEEDED = "Resync."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_CHANNELS_NEEDED = "FlushChannels."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();
    
    final String CTX_KEY_FLUSH_LOADFILTERS_NEEDED = "FlushLoadFilters."
            + ConfigurationChangedFilter.class.getSimpleName() + hashCode();    

    final String CTX_KEY_FLUSH_TRANSFORMS_NEEDED = "FlushTransforms."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_PARAMETERS_NEEDED = "FlushParameters."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_CONFLICTS_NEEDED = "FlushConflicts."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_RESTART_JOBMANAGER_NEEDED = "RestartJobManager."
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
        Map<String, String> columnValues = getDataMap(dataMetaData, engine != null ? engine.getSymmetricDialect() : null);

        Node me = findIdentity();

        if (me != null) {
            NetworkedNode rootNetworkedNode = getRootNetworkNodeFromContext(routingContext);

            // if this is sym_node or sym_node_security determine which nodes it
            // goes to.
            if (tableMatches(dataMetaData, TableConstants.SYM_NODE)
                    || tableMatches(dataMetaData, TableConstants.SYM_NODE_SECURITY)
                    || tableMatches(dataMetaData, TableConstants.SYM_NODE_HOST)) {

                String nodeIdInQuestion = columnValues.get("NODE_ID");
                List<NodeGroupLink> nodeGroupLinks = getNodeGroupLinksFromContext(routingContext);
                for (Node nodeThatMayBeRoutedTo : possibleTargetNodes) {
                    if (!nodeThatMayBeRoutedTo.requires13Compatiblity() && 
                            isLinked(nodeIdInQuestion, nodeThatMayBeRoutedTo, rootNetworkedNode, me,
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
                    DataEventType eventType = dataMetaData.getData().getDataEventType();
                    /*
                     * Don't route node security to it's own node. That node
                     * will get node security via registration and it will be
                     * updated by initial load.  Otherwise, updates can be 
                     * unpredictable in the order they will be applied at the
                     * node because updates are on a different channel than reloads
                     */
                    if (tableMatches(dataMetaData, TableConstants.SYM_NODE_SECURITY)) {
                        if (nodeIds.contains(nodeIdInQuestion)) {
                            boolean remove = true;
                            if (eventType == DataEventType.UPDATE) {                               
                                if ("1".equals(columnValues.get("REV_INITIAL_LOAD_ENABLED"))) {
                                    boolean reverseLoadQueued = engine.getParameterService().is(
                                            ParameterConstants.INTITAL_LOAD_REVERSE_FIRST)
                                            || "0".equals(columnValues.get("INITIAL_LOAD_ENABLED"));
                                    /*
                                     * Only send the update if the client is going to be expected 
                                     * to queue up a reverse load.  The trigger to do this is the arrival
                                     * of sym_node_security with REV_INITIAL_LOAD_ENABLED set to 1.
                                     */
                                    if (reverseLoadQueued) {
                                        remove = false;
                                    }
                                }
                            }
                            if (remove) {
                                nodeIds.remove(nodeIdInQuestion);
                            }
                        }
                        
                        /*
                         * The parent node never needs node_security updates.
                         */
                        nodeIds.remove(columnValues.get("CREATED_AT_NODE_ID"));
                    }
                    
                    /*
                     * Don't route insert events for a node to itself. they will
                     * be loaded during registration.  If we route them, then an old
                     * state can override the correct state
                     * 
                     * Don't send deletes to a node.  A node should be responsible for deleting
                     * itself.
                     */
                    if (dataMetaData.getData().getDataEventType() == DataEventType.INSERT ||
                            dataMetaData.getData().getDataEventType() == DataEventType.DELETE) {
                        nodeIds.remove(nodeIdInQuestion);
                    }
                }
            } else if (tableMatches(dataMetaData, TableConstants.SYM_TABLE_RELOAD_REQUEST)) {
                String sourceNodeId = columnValues.get("SOURCE_NODE_ID");
                String reloadEnabled = columnValues.get("RELOAD_ENABLED");
                if (me.getNodeId().equals(sourceNodeId)) {
                    if ("1".equals(reloadEnabled)) {
                        @SuppressWarnings("unchecked")
                        List<TableReloadRequestKey> list = (List<TableReloadRequestKey>) routingContext
                                .get(CTX_KEY_TABLE_RELOAD_NEEDED);
                        if (list == null) {
                            list = new ArrayList<TableReloadRequestKey>();
                            routingContext.put(CTX_KEY_TABLE_RELOAD_NEEDED, list);
                        }

                        String targetNodeId = columnValues.get("TARGET_NODE_ID");
                        String routerId = columnValues.get("ROUTER_ID");
                        String triggerId = columnValues.get("TRIGGER_ID");

                        list.add(new TableReloadRequestKey(targetNodeId, sourceNodeId, triggerId,
                                routerId, dataMetaData.getData().getSourceNodeId()));
                    }
                } else {
                    for (Node nodeThatMayBeRoutedTo : possibleTargetNodes) {
                        if (!nodeThatMayBeRoutedTo.requires13Compatiblity() &&
                                nodeThatMayBeRoutedTo.getNodeId().equals(sourceNodeId)) {                            
                            if (nodeIds == null) {
                                nodeIds = new HashSet<String>();
                            }
                            nodeIds.add(sourceNodeId);
                        }
                    }
                }
                
            } else {
                for (Node nodeThatMayBeRoutedTo : possibleTargetNodes) {
                    if (!nodeThatMayBeRoutedTo.requires13Compatiblity() && (
                            !isSameNumberOfLinksAwayFromRoot(nodeThatMayBeRoutedTo, rootNetworkedNode,
                            me)
                            || (nodeThatMayBeRoutedTo.getNodeId().equals(me.getNodeId()) && initialLoad))) {
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
                    routingContext.put(CTX_KEY_RESYNC_NEEDED, Boolean.TRUE);
                }

                if (tableMatches(dataMetaData, TableConstants.SYM_CHANNEL)) {
                    routingContext.put(CTX_KEY_FLUSH_CHANNELS_NEEDED,
                            Boolean.TRUE);
                }

                if (tableMatches(dataMetaData, TableConstants.SYM_CONFLICT)) {
                    routingContext.put(CTX_KEY_FLUSH_CONFLICTS_NEEDED,
                            Boolean.TRUE);
                }
                
                if (tableMatches(dataMetaData, TableConstants.SYM_LOAD_FILTER)) {
                    routingContext.put(CTX_KEY_FLUSH_LOADFILTERS_NEEDED,
                            Boolean.TRUE);
                }                

                if (tableMatches(dataMetaData, TableConstants.SYM_PARAMETER)) {
                    routingContext.put(CTX_KEY_FLUSH_PARAMETERS_NEEDED,
                            Boolean.TRUE);

                    if (StringUtils.isBlank(dataMetaData.getData().getSourceNodeId()) &&
                            (dataMetaData.getData().getRowData() != null
                            && dataMetaData.getData().getRowData().contains("job."))) {
                        routingContext.put(CTX_KEY_RESTART_JOBMANAGER_NEEDED,
                                Boolean.TRUE);
                    }

                }

                if (tableMatches(dataMetaData, TableConstants.SYM_TRANSFORM_COLUMN)
                        || tableMatches(dataMetaData, TableConstants.SYM_TRANSFORM_TABLE)) {
                    routingContext.put(CTX_KEY_FLUSH_TRANSFORMS_NEEDED,
                            Boolean.TRUE);
                }
            }
        }

        return nodeIds;
    }

    protected Node findIdentity() {
        return engine.getNodeService().findIdentity();
    }

    @SuppressWarnings("unchecked")
    protected List<NodeGroupLink> getNodeGroupLinksFromContext(SimpleRouterContext routingContext) {
        List<NodeGroupLink> list = (List<NodeGroupLink>) routingContext.get(
                NodeGroupLink.class.getName());
        if (list == null) {
            list = engine.getConfigurationService().getNodeGroupLinks();
            routingContext.put(NodeGroupLink.class.getName(), list);
        }
        return list;
    }

    protected NetworkedNode getRootNetworkNodeFromContext(SimpleRouterContext routingContext) {
        NetworkedNode root = (NetworkedNode) routingContext.get(
                NetworkedNode.class.getName());
        if (root == null) {
            root = engine.getNodeService().getRootNetworkedNode();
            routingContext.put(NetworkedNode.class.getName(), root);
        }
        return root;
    }

    private boolean isSameNumberOfLinksAwayFromRoot(Node nodeThatCouldBeRoutedTo,
            NetworkedNode root, Node me) {
        return me != null && root != null && root.getNumberOfLinksAwayFromRoot(nodeThatCouldBeRoutedTo.getNodeId()) == root
                .getNumberOfLinksAwayFromRoot(me.getNodeId());
    }

    private boolean isLinked(String nodeIdInQuestion, Node nodeThatCouldBeRoutedTo,
            NetworkedNode root, Node me, List<NodeGroupLink> allLinks) {
        if (root != null) {
            if (nodeIdInQuestion != null && nodeThatCouldBeRoutedTo != null
                    && !nodeIdInQuestion.equals(nodeThatCouldBeRoutedTo.getNodeId())) {
                NetworkedNode networkedNodeInQuestion = root.findNetworkedNode(nodeIdInQuestion);
                NetworkedNode networkedNodeThatCouldBeRoutedTo = root
                        .findNetworkedNode(nodeThatCouldBeRoutedTo.getNodeId());
                if (networkedNodeInQuestion != null) {
                    if (networkedNodeInQuestion.isInParentHierarchy(nodeThatCouldBeRoutedTo
                            .getNodeId())) {
                        // always route changes to parent nodes
                        return true;
                    }

                    String createdAtNodeId = networkedNodeInQuestion.getNode().getCreatedAtNodeId();
                    if (createdAtNodeId != null
                            && !createdAtNodeId.equals(me.getNodeId())
                            && !networkedNodeInQuestion.getNode().getNodeId()
                                    .equals(me.getNodeId())) {
                        if (createdAtNodeId.equals(nodeThatCouldBeRoutedTo.getNodeId())) {
                            return true;
                        } else if (networkedNodeThatCouldBeRoutedTo != null) {
                            // the node was created at some other node. lets
                            // attempt
                            // to get that update back to that node
                            return networkedNodeThatCouldBeRoutedTo
                                    .isInChildHierarchy(createdAtNodeId);
                        }
                    }

                    // if we haven't found a place to route by now, then we need
                    // to
                    // send the row to all nodes that have links to the node's
                    // group
                    String groupId = networkedNodeInQuestion.getNode().getNodeGroupId();
                    Set<String> groupsThatWillBeInterested = new HashSet<String>();
                    for (NodeGroupLink nodeGroupLink : allLinks) {
                        if (nodeGroupLink.getTargetNodeGroupId().equals(groupId)) {
                            groupsThatWillBeInterested.add(nodeGroupLink.getSourceNodeGroupId());
                        } else if (nodeGroupLink.getSourceNodeGroupId().equals(groupId)) {
                            groupsThatWillBeInterested.add(nodeGroupLink.getTargetNodeGroupId());
                        }
                    }

                    if (groupsThatWillBeInterested.contains(nodeThatCouldBeRoutedTo
                            .getNodeGroupId())) {
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
        } else {
            return false;
        }
    }

    @Override
    public void contextCommitted(SimpleRouterContext routingContext) {
        if (engine.getParameterService().is(ParameterConstants.AUTO_REFRESH_AFTER_CONFIG_CHANGED,
                true)) {
            if (routingContext.get(CTX_KEY_FLUSH_PARAMETERS_NEEDED) != null
                    && engine.getParameterService().is(ParameterConstants.AUTO_SYNC_CONFIGURATION)) {
                log.info("About to refresh the cache of parameters because new configuration came through the data router");
                engine.getParameterService().rereadParameters();
            }

            if (routingContext.get(CTX_KEY_FLUSH_CHANNELS_NEEDED) != null) {
                log.info("Channels flushed because new channels came through the data router");
                engine.getConfigurationService().clearCache();
            }

            if (routingContext.get(CTX_KEY_RESYNC_NEEDED) != null
                    && engine.getParameterService().is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
                log.info("About to syncTriggers because new configuration came through the data router");
                engine.getTriggerRouterService().syncTriggers();
            }

            if (routingContext.get(CTX_KEY_FLUSH_TRANSFORMS_NEEDED) != null) {
                log.info("About to refresh the cache of transformation because new configuration came through the data router");
                engine.getTransformService().clearCache();
            }

            if (routingContext.get(CTX_KEY_FLUSH_CONFLICTS_NEEDED) != null) {
                log.info("About to refresh the cache of conflict settings because new configuration came through the data router");
                engine.getDataLoaderService().clearCache();
            }
            
            if (routingContext.get(CTX_KEY_FLUSH_LOADFILTERS_NEEDED) != null) {
                log.info("About to refresh the cache of load filters because new configuration came through the data router");
                engine.getLoadFilterService().clearCache();
            }
            
            insertReloadEvents(routingContext);

            if (routingContext.get(CTX_KEY_RESTART_JOBMANAGER_NEEDED) != null) {
                IJobManager jobManager = engine.getJobManager();
                if (jobManager != null) {
                    log.info("About to restart jobs because new configuration come through the data router");
                    jobManager.stopJobs();
                    jobManager.startJobs();
                }

            }
        }

    }
    
    protected void insertReloadEvents(SimpleRouterContext routingContext) {        
        @SuppressWarnings("unchecked")
        List<TableReloadRequestKey> reloadRequestKeys = (List<TableReloadRequestKey>) routingContext
                .get(CTX_KEY_TABLE_RELOAD_NEEDED);
        if (reloadRequestKeys != null) {
            for (TableReloadRequestKey reloadRequestKey : reloadRequestKeys) {
                TableReloadRequest request = engine.getDataService()
                        .getTableReloadRequest(reloadRequestKey);
                if (engine.getDataService().insertReloadEvent(request, reloadRequestKey.getReceivedFromNodeId() != null)) {
                    log.info(
                            "Inserted table reload request from config data router for node {} and trigger {}",
                            reloadRequestKey.getTargetNodeId(), reloadRequestKey.getTriggerId());                    
                } 
            }
            routingContext.setRequestGapDetection(true);
        }
    }
    
    private String tableName(String tableName) {
        return TableConstants.getTableName(engine != null ? engine.getTablePrefix()
                : "sym", tableName);
    }

    private boolean tableMatches(DataMetaData dataMetaData, String tableName) {
        boolean matches = false;
        if (dataMetaData
                .getTable()
                .getName()
                .equalsIgnoreCase(tableName(tableName))) {
            matches = true;
        }
        return matches;
    }

}
