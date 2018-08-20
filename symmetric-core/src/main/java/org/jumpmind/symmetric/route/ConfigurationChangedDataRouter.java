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
package org.jumpmind.symmetric.route;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.load.ConfigurationChangedDatabaseWriterFilter;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.NetworkedNode;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.TableReloadRequestKey;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.ITriggerRouterService;

public class ConfigurationChangedDataRouter extends AbstractDataRouter implements IDataRouter, IBuiltInExtensionPoint {

    public static final String ROUTER_TYPE = "configurationChanged";

    final String CTX_KEY_TABLE_RELOAD_NEEDED = "Reload.Table."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_RESYNC_NEEDED = "Resync."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_CHANNELS_NEEDED = "FlushChannels."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_LOADFILTERS_NEEDED = "FlushLoadFilters."
            + ConfigurationChangedDatabaseWriterFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_TRANSFORMS_NEEDED = "FlushTransforms."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_PARAMETERS_NEEDED = "FlushParameters."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_CONFLICTS_NEEDED = "FlushConflicts."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_MONITORS_NEEDED = "FlushMonitors."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_NOTIFICATIONS_NEEDED = "FlushNotifcations."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_NODES_NEEDED = "FlushNodes."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_NODE_SECURITYS_NEEDED = "FlushNodeSecuritys."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_RESTART_JOBMANAGER_NEEDED = "RestartJobManager."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    final String CTX_KEY_REFRESH_EXTENSIONS_NEEDED = "RefreshExtensions."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();
    
    final String CTX_KEY_FLUSHED_TRIGGER_ROUTERS = "FlushedTriggerRouters."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();
    
    final String CTX_KEY_FLUSH_JOBS_NEEDED = "FlushJobs."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();    
    
    final String CTX_KEY_FLUSH_NODE_GROUP_LINK_NEEDED = "FlushNodeGroupLink."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();
    
    final String CTX_KEY_FILE_SYNC_TRIGGERS_NEEDED = "FileSyncTriggers."
            + ConfigurationChangedDataRouter.class.getSimpleName() + hashCode();

    public final static String KEY = "symconfig";

    protected ISymmetricEngine engine;

    public ConfigurationChangedDataRouter() {
    }

    public ConfigurationChangedDataRouter(ISymmetricEngine engine) {
        this.engine = engine;
    }

    @SuppressWarnings("unchecked")
    public Set<String> routeToNodes(SimpleRouterContext routingContext, DataMetaData dataMetaData,
            Set<Node> possibleTargetNodes, boolean initialLoad, boolean initialLoadSelectUsed,
            TriggerRouter triggerRouter) {
        
        possibleTargetNodes = filterOutOlderNodes(dataMetaData, possibleTargetNodes);
        possibleTargetNodes = filterOutNodesByDeploymentType(dataMetaData, possibleTargetNodes);

        // the list of nodeIds that we will return
        Set<String> nodeIds = new HashSet<String>();

        // the inbound data
        Map<String, String> columnValues = getDataMap(dataMetaData,
                engine != null ? engine.getSymmetricDialect() : null);

        Node me = findIdentity();

        if (me != null) {
            NetworkedNode rootNetworkedNode = getRootNetworkNodeFromContext(routingContext);

            if (tableMatches(dataMetaData, TableConstants.SYM_NODE) 
                    && dataMetaData.getData().getDataEventType().equals(DataEventType.SQL)
                    && dataMetaData.getData().getParsedData(CsvData.ROW_DATA).length > 1
                    && dataMetaData.getData().getParsedData(CsvData.ROW_DATA)[0].toUpperCase().contains("TABLE")) {
                routingContext.put(CTX_KEY_RESYNC_NEEDED, Boolean.TRUE);
                routeNodeTables(nodeIds, columnValues, rootNetworkedNode, me, routingContext,
                        dataMetaData, possibleTargetNodes, initialLoad);
            } else if (tableMatches(dataMetaData, TableConstants.SYM_NODE)
                    || tableMatches(dataMetaData, TableConstants.SYM_NODE_SECURITY)
                    || tableMatches(dataMetaData, TableConstants.SYM_NODE_HOST)
                    || tableMatches(dataMetaData, TableConstants.SYM_MONITOR_EVENT)) {
                
                if (tableMatches(dataMetaData, TableConstants.SYM_NODE)) {
                    routingContext.put(CTX_KEY_FLUSH_NODES_NEEDED, Boolean.TRUE);
                } else if (tableMatches(dataMetaData, TableConstants.SYM_NODE_SECURITY)) {
                    routingContext.put(CTX_KEY_FLUSH_NODE_SECURITYS_NEEDED, Boolean.TRUE);
                }

                /*
                 * If this is sym_node or sym_node_security determine which
                 * nodes it goes to.
                 */
                routeNodeTables(nodeIds, columnValues, rootNetworkedNode, me, routingContext,
                        dataMetaData, possibleTargetNodes, initialLoad);
            } else if (tableMatches(dataMetaData, TableConstants.SYM_TABLE_RELOAD_REQUEST)) {
                String sourceNodeId = columnValues.get("SOURCE_NODE_ID");
                String reloadEnabled = columnValues.get("RELOAD_ENABLED");
                if (me.getNodeId().equals(sourceNodeId)) {
                    if ("1".equals(reloadEnabled)) {
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
                        if (!Constants.DEPLOYMENT_TYPE_REST.equals(nodeThatMayBeRoutedTo
                                .getDeploymentType())
                                && !nodeThatMayBeRoutedTo.requires13Compatiblity()
                                && nodeThatMayBeRoutedTo.getNodeId().equals(sourceNodeId)) {
                            nodeIds.add(sourceNodeId);
                        }
                    }
                }
            } else {
                IConfigurationService configurationService = engine.getConfigurationService();
                for (Node nodeThatMayBeRoutedTo : possibleTargetNodes) {
                    if (!Constants.DEPLOYMENT_TYPE_REST.equals(nodeThatMayBeRoutedTo
                            .getDeploymentType())
                            && !nodeThatMayBeRoutedTo.requires13Compatiblity()
                            && (initialLoad || !isSameNumberOfLinksAwayFromRoot(nodeThatMayBeRoutedTo,
                                    rootNetworkedNode, me))) {
                        NodeGroupLink link = configurationService.getNodeGroupLinkFor(
                                me.getNodeGroupId(), nodeThatMayBeRoutedTo.getNodeGroupId(), false);
                        if (initialLoad || (link != null && link.isSyncConfigEnabled())) {
                            nodeIds.add(nodeThatMayBeRoutedTo.getNodeId());
                        }
                    }
                }

                if (StringUtils.isBlank(dataMetaData.getData().getSourceNodeId())) {
                    queueSyncTriggers(routingContext, dataMetaData, columnValues);
                }

                if (tableMatches(dataMetaData, TableConstants.SYM_CHANNEL)) {
                    routingContext.put(CTX_KEY_FLUSH_CHANNELS_NEEDED, Boolean.TRUE);
                }

                if (tableMatches(dataMetaData, TableConstants.SYM_CONFLICT)) {
                    routingContext.put(CTX_KEY_FLUSH_CONFLICTS_NEEDED, Boolean.TRUE);
                }

                if (tableMatches(dataMetaData, TableConstants.SYM_LOAD_FILTER)) {
                    routingContext.put(CTX_KEY_FLUSH_LOADFILTERS_NEEDED, Boolean.TRUE);
                }

                if (tableMatches(dataMetaData, TableConstants.SYM_PARAMETER)) {
                    routingContext.put(CTX_KEY_FLUSH_PARAMETERS_NEEDED, Boolean.TRUE);

                    if (StringUtils.isBlank(dataMetaData.getData().getSourceNodeId())
                            && (dataMetaData.getData().getRowData() != null && dataMetaData
                                    .getData().getRowData().contains("job."))) {
                        routingContext.put(CTX_KEY_RESTART_JOBMANAGER_NEEDED, Boolean.TRUE);
                    }

                }

                if (tableMatches(dataMetaData, TableConstants.SYM_TRANSFORM_COLUMN)
                        || tableMatches(dataMetaData, TableConstants.SYM_TRANSFORM_TABLE)) {
                    routingContext.put(CTX_KEY_FLUSH_TRANSFORMS_NEEDED, Boolean.TRUE);
                }
                
                if (tableMatches(dataMetaData, TableConstants.SYM_EXTENSION)) {
                    routingContext.put(CTX_KEY_REFRESH_EXTENSIONS_NEEDED, Boolean.TRUE);
                }

                if (tableMatches(dataMetaData, TableConstants.SYM_MONITOR)) {
                    routingContext.put(CTX_KEY_FLUSH_MONITORS_NEEDED, Boolean.TRUE);
                }
                
                if (tableMatches(dataMetaData, TableConstants.SYM_NOTIFICATION)) {
                    routingContext.put(CTX_KEY_FLUSH_NOTIFICATIONS_NEEDED, Boolean.TRUE);
                }
                
                if (tableMatches(dataMetaData, TableConstants.SYM_NODE_GROUP_LINK)) {
                    routingContext.put(CTX_KEY_FLUSH_NODE_GROUP_LINK_NEEDED, Boolean.TRUE);
                }
                
                if (tableMatches(dataMetaData, TableConstants.SYM_JOB)
                        || routingContext.get(CTX_KEY_FILE_SYNC_TRIGGERS_NEEDED) != null) {
                    routingContext.put(CTX_KEY_FLUSH_JOBS_NEEDED, Boolean.TRUE);
                }
            }
        }

        return nodeIds;
    }
    
    protected Set<Node> filterOutNodesByDeploymentType(DataMetaData dataMetaData, Set<Node> possibleTargetNodes) {
        if (tableMatches(dataMetaData, TableConstants.SYM_CONSOLE_USER)
                || tableMatches(dataMetaData, TableConstants.SYM_CONSOLE_USER_HIST)) {
            Set<Node> targetNodes = new HashSet<Node>(possibleTargetNodes.size());
            for (Node nodeThatMayBeRoutedTo : possibleTargetNodes) {
                boolean isTargetProfessional = StringUtils.equals(nodeThatMayBeRoutedTo.getDeploymentType(), 
                        Constants.DEPLOYMENT_TYPE_PROFESSIONAL);
                if (isTargetProfessional) {                    
                    targetNodes.add(nodeThatMayBeRoutedTo);
                }                
            }
            return targetNodes;
        } else {
            return possibleTargetNodes;
        }        
    }
    
    protected Set<Node> filterOutOlderNodes(DataMetaData dataMetaData, Set<Node> possibleTargetNodes) {
        if (tableMatches(dataMetaData, TableConstants.SYM_MONITOR)
                || tableMatches(dataMetaData, TableConstants.SYM_MONITOR_EVENT) 
                || tableMatches(dataMetaData, TableConstants.SYM_NOTIFICATION)
                || tableMatches(dataMetaData, TableConstants.SYM_JOB)) {
            Set<Node> targetNodes = new HashSet<Node>(possibleTargetNodes.size());
            for (Node nodeThatMayBeRoutedTo : possibleTargetNodes) {
                if (tableMatches(dataMetaData, TableConstants.SYM_JOB)) {
                    if (nodeThatMayBeRoutedTo.isVersionGreaterThanOrEqualTo(3, 9, 0)) {
                        targetNodes.add(nodeThatMayBeRoutedTo);
                    }
                }
                else if (nodeThatMayBeRoutedTo.isVersionGreaterThanOrEqualTo(3, 8, 0)) {
                    targetNodes.add(nodeThatMayBeRoutedTo);
                }
            }
            return targetNodes;
        } else {
            return possibleTargetNodes;
        }
    }

    protected void routeNodeTables(Set<String> nodeIds, Map<String, String> columnValues,
            NetworkedNode rootNetworkedNode, Node me, SimpleRouterContext routingContext,
            DataMetaData dataMetaData, Set<Node> possibleTargetNodes, boolean initialLoad) {
        String nodeIdForRecordBeingRouted = columnValues.get("NODE_ID");
        if (dataMetaData.getData().getDataEventType() == DataEventType.DELETE) {
            String createAtNodeId = columnValues.get("CREATED_AT_NODE_ID");
            for (Node nodeThatMayBeRoutedTo : possibleTargetNodes) {
                if (!Constants.DEPLOYMENT_TYPE_REST.equals(nodeThatMayBeRoutedTo
                        .getDeploymentType())
                        && !nodeIdForRecordBeingRouted.equals(nodeThatMayBeRoutedTo.getNodeId())
                        && !nodeThatMayBeRoutedTo.getNodeId().equals(createAtNodeId)
                        && !nodeIdForRecordBeingRouted.equals(me.getNodeId())                                
                        && (nodeThatMayBeRoutedTo.getCreatedAtNodeId() == null || !nodeThatMayBeRoutedTo
                                .getCreatedAtNodeId().equals(nodeIdForRecordBeingRouted))) {
                    nodeIds.add(nodeThatMayBeRoutedTo.getNodeId());
                }
            }
        } else {
            IConfigurationService configurationService = engine.getConfigurationService();
            List<NodeGroupLink> nodeGroupLinks = getNodeGroupLinksFromContext(routingContext);
            for (Node nodeThatMayBeRoutedTo : possibleTargetNodes) {
                if (!Constants.DEPLOYMENT_TYPE_REST.equals(nodeThatMayBeRoutedTo.getDeploymentType())
                        && !nodeThatMayBeRoutedTo.requires13Compatiblity()
                        && isLinked(nodeIdForRecordBeingRouted, nodeThatMayBeRoutedTo, rootNetworkedNode, me, nodeGroupLinks)
                        && (!isSameNumberOfLinksAwayFromRoot(nodeThatMayBeRoutedTo, rootNetworkedNode, me) 
                                || configurationService.isMasterToMaster())
                        || (nodeThatMayBeRoutedTo.getNodeId().equals(me.getNodeId()) && initialLoad)) {
                    nodeIds.add(nodeThatMayBeRoutedTo.getNodeId());
                }
            }

            if (!initialLoad && nodeIds != null) {

                if (tableMatches(dataMetaData, TableConstants.SYM_NODE_SECURITY)) {
                    routeSymNodeSecurity(me, nodeIdForRecordBeingRouted, dataMetaData, nodeIds, columnValues);
                }

                /*
                 * Don't route insert events for a node to itself. They will be
                 * loaded during registration. If we route them, then an old
                 * state can override the correct state
                 * 
                 * Don't send deletes to a node. A node should be responsible
                 * for deleting itself.
                 */
                if (dataMetaData.getData().getDataEventType() == DataEventType.INSERT) {
                    nodeIds.remove(nodeIdForRecordBeingRouted);
                }
            }
        }
    }
    
    protected void routeSymNodeSecurity (Node me, 
            String nodeIdForRecordBeingRouted, DataMetaData dataMetaData, Set<String> nodeIds, Map<String, String> columnValues) {
        DataEventType eventType = dataMetaData.getData().getDataEventType();
        boolean fromAnotherNode = isNotBlank(dataMetaData.getData().getSourceNodeId());

        if (nodeIds.contains(nodeIdForRecordBeingRouted)) {
            /*
             * Don't route node security to it's own node. That node will
             * get node security via registration and it will be updated by
             * initial load. Otherwise, updates can be unpredictable in the
             * order they will be applied at the node because updates are on
             * a different channel than reloads
             */
            boolean remove = true;
            if (eventType == DataEventType.UPDATE) {
                if ("1".equals(columnValues.get("REV_INITIAL_LOAD_ENABLED"))) {
                    boolean reverseLoadQueued = engine.getParameterService().is(
                            ParameterConstants.INITIAL_LOAD_REVERSE_FIRST)
                            || "0".equals(columnValues.get("INITIAL_LOAD_ENABLED"));
                    /*
                     * Only send the update if the client is going
                     * to be expected to queue up a reverse load.
                     * The trigger to do this is the arrival of
                     * sym_node_security with
                     * REV_INITIAL_LOAD_ENABLED set to 1.
                     */
                    if (reverseLoadQueued) {
                        remove = false;
                    }
                }                            
            }
            if (remove) {
                nodeIds.remove(nodeIdForRecordBeingRouted);
            }
        }
        
        boolean removeParentNode = true;
        if (eventType == DataEventType.UPDATE) {
            if ("1".equals(columnValues.get("INITIAL_LOAD_ENABLED")) &&
                    me.getNodeId().equals(nodeIdForRecordBeingRouted) ) {
                removeParentNode = false;
            }                            
        }
        if (removeParentNode) {
            nodeIds.remove(columnValues.get("CREATED_AT_NODE_ID"));
        }
        
        if (engine.getConfigurationService().isMasterToMaster() || fromAnotherNode) {
            /*
             * Don't send updates where the initial load flags are enabled to other 
             * nodes in the cluster 
             */
            if ("1".equals(columnValues.get("INITIAL_LOAD_ENABLED"))) {
                nodeIds.clear();
            }
        }

    }

    @SuppressWarnings("unchecked")
    protected void queueSyncTriggers(SimpleRouterContext routingContext, DataMetaData dataMetaData,
            Map<String, String> columnValues) {
        if ((tableMatches(dataMetaData, TableConstants.SYM_TRIGGER) || tableMatches(dataMetaData,
                TableConstants.SYM_TRIGGER_ROUTER))) {
            Object needResync = routingContext.get(CTX_KEY_RESYNC_NEEDED);
            if (needResync == null || needResync instanceof Set) {
                if (needResync == null) {
                    needResync = new HashSet<Trigger>();
                    routingContext.put(CTX_KEY_RESYNC_NEEDED, needResync);
                }

                ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
                
                boolean refreshCache = false;
                if (routingContext.get(CTX_KEY_FLUSHED_TRIGGER_ROUTERS) == null) {
            	    triggerRouterService.clearCache();
            	    refreshCache = true;
            	    routingContext.put(CTX_KEY_FLUSHED_TRIGGER_ROUTERS, true);
                }
                
                Trigger trigger = null;
                Date lastUpdateTime = null;
                String triggerId = columnValues.get("TRIGGER_ID");
                if (tableMatches(dataMetaData, TableConstants.SYM_TRIGGER_ROUTER)) {
                    String routerId = columnValues.get("ROUTER_ID");
                    TriggerRouter tr = triggerRouterService.findTriggerRouterById(triggerId,
                            routerId, refreshCache);
                    if (tr != null) {
                        trigger = tr.getTrigger();
                        lastUpdateTime = tr.getLastUpdateTime();
                    }
                } else {
                    trigger = triggerRouterService.getTriggerById(triggerId, refreshCache);
                    if (trigger != null) {
                        lastUpdateTime = trigger.getLastUpdateTime();
                    }
                }
                if (trigger != null) {
                    List<TriggerHistory> histories = triggerRouterService
                            .getActiveTriggerHistories(trigger);
                    boolean sync = false;
                    if (histories != null && histories.size() > 0) {
                        for (TriggerHistory triggerHistory : histories) {
                            if (triggerHistory.getCreateTime().before(lastUpdateTime)) {
                                sync = true;
                            }
                        }
                    } else {
                        sync = true;
                    }

                    if (sync) {
                        ((Set<Trigger>) needResync).add(trigger);
                    }
                }
            }
        } else if (tableMatches(dataMetaData, TableConstants.SYM_ROUTER)
                || tableMatches(dataMetaData, TableConstants.SYM_NODE_GROUP_LINK)) {
            routingContext.put(CTX_KEY_RESYNC_NEEDED, Boolean.TRUE);
        } else if (tableMatches(dataMetaData, TableConstants.SYM_PARAMETER)) {
            if (dataMetaData.getData().getCsvData(CsvData.ROW_DATA) != null
                    && dataMetaData.getData().getCsvData(CsvData.ROW_DATA).contains(ParameterConstants.FILE_SYNC_ENABLE)) {
                routingContext.put(CTX_KEY_FILE_SYNC_TRIGGERS_NEEDED, Boolean.TRUE);
            }
        }

    }

    protected Node findIdentity() {
        return engine.getNodeService().findIdentity();
    }

    @SuppressWarnings("unchecked")
    protected List<NodeGroupLink> getNodeGroupLinksFromContext(SimpleRouterContext routingContext) {
        List<NodeGroupLink> list = (List<NodeGroupLink>) routingContext.get(NodeGroupLink.class
                .getName());
        if (list == null) {
            list = engine.getConfigurationService().getNodeGroupLinks(false);
            routingContext.put(NodeGroupLink.class.getName(), list);
        }
        return list;
    }

    protected NetworkedNode getRootNetworkNodeFromContext(SimpleRouterContext routingContext) {
        NetworkedNode root = (NetworkedNode) routingContext.get(NetworkedNode.class.getName());
        if (root == null) {
            root = engine.getNodeService().getRootNetworkedNode();
            routingContext.put(NetworkedNode.class.getName(), root);
        }
        return root;
    }

    private boolean isSameNumberOfLinksAwayFromRoot(Node nodeThatCouldBeRoutedTo,
            NetworkedNode root, Node me) {
        return me != null
                && root != null
                && root.getNumberOfLinksAwayFromRoot(nodeThatCouldBeRoutedTo.getNodeId()) == root
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

            Object needsSynced = routingContext.get(CTX_KEY_RESYNC_NEEDED);
            if (needsSynced != null
                    && engine.getParameterService().is(ParameterConstants.AUTO_SYNC_TRIGGERS)
                    && engine.getParameterService().is(
                            ParameterConstants.AUTO_SYNC_TRIGGERS_AFTER_CONFIG_CHANGED)) {
                if (Boolean.TRUE.equals(needsSynced)) {
                    log.info("About to syncTriggers because new configuration came through the data router");
                    engine.getTriggerRouterService().syncTriggers();
                } else if (needsSynced instanceof Set) {
                    @SuppressWarnings("unchecked")
                    Set<Trigger> triggers = (Set<Trigger>) needsSynced;
                    for (Trigger trigger : triggers) {
                        log.info("About to sync the "
                                + trigger.getTriggerId()
                                + " trigger because a change was detected by the config data router");
                        engine.getTriggerRouterService().syncTrigger(trigger, null, false);
                    }
                }
            }

            if (routingContext.get(CTX_KEY_FLUSH_TRANSFORMS_NEEDED) != null) {
                log.info("About to refresh the cache of transformation because new configuration came through the data router");
                engine.getTransformService().clearCache();
                log.info("About to clear the staging area because new transform configuration came through the data router");
                engine.getStagingManager().clean(0);
            }

            if (routingContext.get(CTX_KEY_FLUSH_CONFLICTS_NEEDED) != null) {
                log.info("About to refresh the cache of conflict settings because new configuration came through the data router");
                engine.getDataLoaderService().clearCache();
            }

            if (routingContext.get(CTX_KEY_FLUSH_LOADFILTERS_NEEDED) != null) {
                log.info("About to refresh the cache of load filters because new configuration came through the data router");
                engine.getLoadFilterService().clearCache();
            }

            if (routingContext.get(CTX_KEY_RESTART_JOBMANAGER_NEEDED) != null) {
                IJobManager jobManager = engine.getJobManager();
                if (jobManager != null) {
                    log.info("About to restart jobs because new configuration come through the data router");
                    jobManager.restartJobs();
                }
            }
            
            if (routingContext.get(CTX_KEY_REFRESH_EXTENSIONS_NEEDED) != null) {
                log.info("About to refresh the cache of extensions because new configuration came through the data router");
                engine.getExtensionService().refresh();
            }
            
            if (routingContext.get(CTX_KEY_FLUSH_MONITORS_NEEDED) != null) {
                log.info("About to refresh the cache of monitors because new configuration came through the data router");
                engine.getMonitorService().flushMonitorCache();
            }

            if (routingContext.get(CTX_KEY_FLUSH_NOTIFICATIONS_NEEDED) != null) {
                log.info("About to refresh the cache of notifications because new configuration came through the data router");
                engine.getMonitorService().flushNotificationCache();
            }
            
            if (routingContext.get(CTX_KEY_FLUSH_JOBS_NEEDED) != null) {
                log.info("About to reset the job manager because new configuration came through the data router");
                engine.getJobManager().init();
                engine.getJobManager().startJobs();
            }
            
            if (routingContext.get(CTX_KEY_FLUSH_NODES_NEEDED) != null) {
                log.info("About to refresh the cache of nodes because new configuration came through the data router");
                engine.getNodeService().flushNodeCache();
                engine.getNodeService().flushNodeGroupCache();
            }

            if (routingContext.get(CTX_KEY_FLUSH_NODE_SECURITYS_NEEDED) != null) {
                log.info("About to refresh the cache of node security because new configuration came through the data router");
                engine.getNodeService().flushNodeAuthorizedCache();
            }
            
            if (routingContext.get(CTX_KEY_FLUSH_NODE_GROUP_LINK_NEEDED) != null) {
                log.info("About to refresh the cache of node group link because new configuration came through the data router");
                engine.getConfigurationService().clearCache();
                engine.getNodeService().flushNodeGroupCache();
            }
            
            if (routingContext.get(CTX_KEY_FILE_SYNC_TRIGGERS_NEEDED) != null
                    && engine.getParameterService().is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
                log.info("About to syncTriggers for file snapshot because the file sync parameter has changed");
                Table fileSnapshotTable = engine.getDatabasePlatform()
                        .getTableFromCache(TableConstants.getTableName(engine.getTablePrefix(), TableConstants.SYM_FILE_SNAPSHOT), false);
                engine.getTriggerRouterService().syncTriggers(fileSnapshotTable, false);
                engine.getFileSyncService().clearCache();
            }

        }
    }
    
    private String tableName(String tableName) {
        return TableConstants.getTableName(engine != null ? engine.getTablePrefix() : "sym",
                tableName);
    }

    private boolean tableMatches(DataMetaData dataMetaData, String tableName) {
        boolean matches = false;
        if (dataMetaData.getTable().getName().equalsIgnoreCase(tableName(tableName))) {
            matches = true;
        }
        return matches;
    }

    @Override
    public boolean isConfigurable() {
        return false;
    }
}
