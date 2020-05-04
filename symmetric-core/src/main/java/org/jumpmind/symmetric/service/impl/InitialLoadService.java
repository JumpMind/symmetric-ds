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
package org.jumpmind.symmetric.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.StringUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.load.IReloadGenerator;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.ExtractRequest;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessType;
import org.jumpmind.symmetric.model.TableReloadRequest;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.IInitialLoadService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitialLoadService extends AbstractService implements IInitialLoadService {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected ISymmetricEngine engine;
    
    protected IExtensionService extensionService;

    protected boolean syncTriggersBeforeInitialLoadAttempted = false;
    
    protected int lastLoadCountToProcess;
    
    public InitialLoadService(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect());

        this.engine = engine;
        this.extensionService = engine.getExtensionService();
    }

    @Override
    public synchronized void queueLoads(boolean force) {
        Node identity = engine.getNodeService().findIdentity();
        if (identity != null) {
            if (force || engine.getClusterService().lock(ClusterConstants.INITIAL_LOAD_QUEUE)) {
                ProcessInfo processInfo = null;
                try {
                    processInfo = engine.getStatisticManager().newProcessInfo(
                            new ProcessInfoKey(identity.getNodeId(), null, ProcessType.INSERT_LOAD_EVENTS));
                    processInfo.setStatus(ProcessInfo.ProcessStatus.PROCESSING);                    
                    boolean isRegistered = false;

                    if (engine.getParameterService().isRegistrationServer()) {
                        isRegistered = true;
                    } else {
                        boolean isClusteringEnabled = parameterService.is(ParameterConstants.CLUSTER_LOCKING_ENABLED);
                        NodeSecurity identitySecurity = engine.getNodeService().findNodeSecurity(identity.getNodeId(), !isClusteringEnabled);
                        isRegistered = identitySecurity != null && !identitySecurity.isRegistrationEnabled()
                                && identitySecurity.getRegistrationTime() != null;
                    }

                    if (isRegistered) {
                        processInitialLoadEnabledFlag(identity, processInfo);
                        processTableRequestLoads(identity, processInfo);
                    }
                    
                    processInfo.setStatus(ProcessInfo.ProcessStatus.OK);
                } catch (Exception e) {
                    processInfo.setStatus(ProcessInfo.ProcessStatus.ERROR);
                    log.error("Error while queuing initial loads", e);
                } finally {
                    if (!force) {
                        engine.getClusterService().unlock(ClusterConstants.INITIAL_LOAD_QUEUE);
                    }
                }
            }
        }
    }

    /**
     * If a load has been queued up by setting the initial load enabled or reverse
     * initial load enabled flags, then the router service will insert the reload
     * events. This process will not run at the same time sync triggers is running.
     */
    public void processInitialLoadEnabledFlag(Node identity, ProcessInfo processInfo) {
        try {
            List<NodeSecurity> nodeSecurities = findNodesThatAreReadyForInitialLoad();
            if (nodeSecurities != null && nodeSecurities.size() > 0) {
                boolean reverseLoadFirst = parameterService.is(ParameterConstants.INITIAL_LOAD_REVERSE_FIRST);
                List<TriggerHistory> activeHistories = null;
                IReloadGenerator reloadGenerator = extensionService.getExtensionPoint(IReloadGenerator.class);
                if (reloadGenerator == null) {
                    activeHistories = engine.getTriggerRouterService().getActiveTriggerHistories();
                }
                
                for (NodeSecurity security : nodeSecurities) {
                    if (reloadGenerator != null) {
                        Node targetNode = engine.getNodeService().findNode(security.getNodeId());
                        activeHistories = reloadGenerator.getActiveTriggerHistories(targetNode);
                    }

                    if (activeHistories.size() > 0) {
                        boolean thisMySecurityRecord = security.getNodeId().equals(identity.getNodeId());
                        boolean reverseLoadEnabled = security.isRevInitialLoadEnabled();
                        boolean initialLoadEnabled = security.isInitialLoadEnabled();
                        boolean registered = security.getRegistrationTime() != null;
                        if (! thisMySecurityRecord && registered && reverseLoadEnabled && (reverseLoadFirst || !initialLoadEnabled)) {
                            // If node is created by me then set up reverse initial load
                            if(StringUtils.equals(security.getCreatedAtNodeId(), identity.getNodeId())) {
                                TableReloadRequest request = new TableReloadRequest();
                                
                                request.setTriggerId(ParameterConstants.ALL);
                                request.setRouterId(ParameterConstants.ALL);
                                request.setSourceNodeId(security.getNodeId());
                                request.setTargetNodeId(identity.getNodeId());
                                request.setCreateTime(new Date());
                                
                                log.info("Creating load request from node " + security.getNodeId() + " to node " + identity.getNodeId());
                                engine.getDataService().insertTableReloadRequest(request);
                                processInfo.incrementCurrentDataCount();
                                
                                // Reset reverse initial load flag to off
                                engine.getNodeService().setReverseInitialLoadEnabled(security.getNodeId(), false, true, 0l, "initialLoadService");
                                
                            }
                            
                        } else if (!thisMySecurityRecord && registered && initialLoadEnabled && (!reverseLoadFirst || !reverseLoadEnabled)) {
                            // If node is created by me then set up initial load
                            if(StringUtils.equals(security.getCreatedAtNodeId(), identity.getNodeId())) {
                                TableReloadRequest reloadRequest = new TableReloadRequest();
                                reloadRequest.setTriggerId(ParameterConstants.ALL);
                                reloadRequest.setRouterId(ParameterConstants.ALL);
                                reloadRequest.setSourceNodeId(identity.getNodeId());
                                reloadRequest.setTargetNodeId(security.getNodeId());
                                reloadRequest.setCreateTable(parameterService.is(ParameterConstants.INITIAL_LOAD_CREATE_SCHEMA_BEFORE_RELOAD));
                                reloadRequest.setDeleteFirst(parameterService.is(ParameterConstants.INITIAL_LOAD_DELETE_BEFORE_RELOAD));
                                reloadRequest.setCreateTime(new Date());
                                log.info("Creating load request from node " + identity.getNodeId() + " to node " + security.getNodeId());
                                engine.getDataService().insertTableReloadRequest(reloadRequest);
                                processInfo.incrementCurrentDataCount();
                                
                                // Reset initial load flag to off
                                engine.getNodeService().setInitialLoadEnabled(security.getNodeId(), false, true, 0l, "initialLoadService");
                            }
                        }
                    } else {
                        List<NodeGroupLink> links = engine.getConfigurationService().getNodeGroupLinksFor(parameterService.getNodeGroupId(),
                                false);
                        if (links == null || links.size() == 0) {
                            log.warn(
                                    "Could not queue up a load for {} because a node group link is NOT configured over which a load could be delivered",
                                    security.getNodeId());
                        } else {
                            log.warn("Could not queue up a load for {} because sync triggers has not yet run", security.getNodeId());
                            if (!syncTriggersBeforeInitialLoadAttempted) {
                                syncTriggersBeforeInitialLoadAttempted = true;
                                engine.getTriggerRouterService().syncTriggers();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error while processing initial loads using node security", e);
        }
    }

    protected void processTableRequestLoads(Node source, ProcessInfo processInfo) {
        List<TableReloadRequest> loadsToProcess = engine.getDataService().getTableReloadRequestToProcess(source.getNodeId());
        if (loadsToProcess.size() > 0) {
            processInfo.setStatus(ProcessInfo.ProcessStatus.CREATING);

            int maxLoadCount = parameterService.getInt(ParameterConstants.INITIAL_LOAD_EXTRACT_THREAD_COUNT_PER_SERVER, 20);            
            int activeLoadCount = engine.getDataService().getActiveTableReloadStatus().size();
            int loadCountToProcess = loadsToProcess.size(); 

            if (activeLoadCount >= maxLoadCount) {
                logActiveLoadCount(activeLoadCount, loadCountToProcess);
                return;
            }

            boolean useExtractJob = parameterService.is(ParameterConstants.INITIAL_LOAD_USE_EXTRACT_JOB, true);

            if (useExtractJob) {
                Map<String, Channel> channels = engine.getConfigurationService().getChannels(false);
                boolean isError = false;
                for (Channel channel : channels.values()) {
                    if (channel.isReloadFlag() && channel.getMaxBatchSize() == 1) {
                        log.error("Max batch size must be greater than 1 for '{}' channel", channel.getChannelId());
                        isError = true;
                    }
                }
                if (isError) {
                    log.error("Initial loads are disabled until max batch size is corrected or {} is set to false",
                            ParameterConstants.INITIAL_LOAD_USE_EXTRACT_JOB);
                    return;
                }
            }

            log.info("Found {} table reload requests to process.", loadCountToProcess);

            boolean streamToFile = parameterService.is(ParameterConstants.STREAM_TO_FILE_ENABLED, false);
            Map<String, List<TableReloadRequest>> requestsSplitByLoad = new HashMap<String, List<TableReloadRequest>>();
            Map<String, List<TriggerRouter>> triggerRoutersByNodeGroup = new HashMap<String, List<TriggerRouter>>();
            Map<Integer, ExtractRequest> extractRequests = null;
            List<TriggerHistory> activeHistories = null;

            IReloadGenerator reloadGenerator = extensionService.getExtensionPoint(IReloadGenerator.class);
            if (reloadGenerator == null) {
                activeHistories = engine.getTriggerRouterService().getActiveTriggerHistories();
            }

            for (TableReloadRequest load : loadsToProcess) {
                Node targetNode = engine.getNodeService().findNode(load.getTargetNodeId(), true);
                if (!useExtractJob || streamToFile) {
                    if (load.isFullLoadRequest() && isValidLoadTarget(load.getTargetNodeId())) {
                        List<TableReloadRequest> fullLoad = new ArrayList<TableReloadRequest>();
                        fullLoad.add(load);
                        List<TriggerRouter> triggerRouters = getTriggerRoutersForNodeGroup(triggerRoutersByNodeGroup, targetNode.getNodeGroupId());

                        if (reloadGenerator != null) {
                            activeHistories = reloadGenerator.getActiveTriggerHistories(targetNode);
                        }

                        extractRequests = engine.getDataService().insertReloadEvents(targetNode, false, fullLoad, processInfo, activeHistories,
                                triggerRouters, extractRequests);

                        loadCountToProcess--;
                        if (++activeLoadCount >= maxLoadCount) {
                            logActiveLoadCount(activeLoadCount, loadCountToProcess);
                            return;
                        }
                    } else {
                        NodeSecurity targetNodeSecurity = engine.getNodeService().findNodeSecurity(load.getTargetNodeId());

                        boolean registered = targetNodeSecurity != null && (targetNodeSecurity.getRegistrationTime() != null
                                || targetNodeSecurity.getNodeId().equals(targetNodeSecurity.getCreatedAtNodeId()));
                        if (registered) {
                            // Make loads unique to the target and create time
                            String key = load.getTargetNodeId() + "::" + load.getCreateTime().toString();
                            if (!requestsSplitByLoad.containsKey(key)) {
                                requestsSplitByLoad.put(key, new ArrayList<TableReloadRequest>());
                            }
                            requestsSplitByLoad.get(key).add(load);
                        } else {
                            log.warn("There was a load queued up for '{}', but the node is not registered.  It is being ignored",
                                    load.getTargetNodeId());
                        }
                    }
                } else {
                    throw new SymmetricException(
                            String.format("Node '%s' can't process load for '%s' because of conflicting parameters: %s=%s and %s=%s",
                                    source.getNodeId(), load.getTargetNodeId(), ParameterConstants.INITIAL_LOAD_USE_EXTRACT_JOB, useExtractJob,
                                    ParameterConstants.STREAM_TO_FILE_ENABLED, streamToFile));
                }
            }

            Map<String, List<TriggerRouter>> triggerRoutersByTargetNodeGroupId = new HashMap<String, List<TriggerRouter>>();

            for (Map.Entry<String, List<TableReloadRequest>> entry : requestsSplitByLoad.entrySet()) {
                Node targetNode = engine.getNodeService().findNode(entry.getKey().split("::")[0], true);
                if (targetNode == null) {
                    targetNode = engine.getNodeService().findNode(entry.getKey().split("::")[0], false);
                }
                ITriggerRouterService triggerRouterService = engine.getTriggerRouterService();
                List<TriggerRouter> triggerRouters = triggerRoutersByTargetNodeGroupId.get(targetNode.getNodeGroupId());
                if (triggerRouters == null) {
                    triggerRouters = triggerRouterService.getAllTriggerRoutersForReloadForCurrentNode(parameterService.getNodeGroupId(),
                            targetNode.getNodeGroupId());
                    triggerRoutersByTargetNodeGroupId.put(targetNode.getNodeGroupId(), triggerRouters);
                }
                if (reloadGenerator != null) {
                    activeHistories = reloadGenerator.getActiveTriggerHistories(targetNode);
                }

                extractRequests = engine.getDataService().insertReloadEvents(targetNode, false, entry.getValue(), processInfo, activeHistories,
                        triggerRouters, extractRequests);

                loadCountToProcess--;
                if (++activeLoadCount >= maxLoadCount) {
                    logActiveLoadCount(activeLoadCount, loadCountToProcess);
                    return;
                }
            }
        }
    }

    protected void logActiveLoadCount(int activeLoadCount, int loadCountToProcess) {
        String message = "Max outgoing loads of {} are active, while {} outgoing loads are pending";
        if (loadCountToProcess != lastLoadCountToProcess) {
            log.warn(message, activeLoadCount, loadCountToProcess);
        } else {
            log.debug(message, activeLoadCount, loadCountToProcess);
        }
        lastLoadCountToProcess = loadCountToProcess;
    }

    protected List<TriggerRouter> getTriggerRoutersForNodeGroup(Map<String, List<TriggerRouter>> triggerRoutersByNodeGroup, String nodeGroupId) {
        List<TriggerRouter> list = triggerRoutersByNodeGroup.get(nodeGroupId);
        if (list == null) {
            list = engine.getTriggerRouterService().getAllTriggerRoutersForReloadForCurrentNode(parameterService.getNodeGroupId(), nodeGroupId);
            triggerRoutersByNodeGroup.put(nodeGroupId, list);
        }
        return list;
    }

    protected List<NodeSecurity> findNodesThatAreReadyForInitialLoad() {
        INodeService nodeService = engine.getNodeService();
        IConfigurationService configurationService = engine.getConfigurationService();
        String me = nodeService.findIdentityNodeId();
        List<NodeSecurity> toReturn = new ArrayList<NodeSecurity>();
        List<NodeSecurity> securities = nodeService.findNodeSecurityWithLoadEnabled();
        for (NodeSecurity nodeSecurity : securities) {
            if (((!nodeSecurity.getNodeId().equals(me) && nodeSecurity.isInitialLoadEnabled())
                    || (!nodeSecurity.getNodeId().equals(me) && configurationService.isMasterToMaster())
                    || (nodeSecurity.getNodeId().equals(me) && nodeSecurity.isRevInitialLoadEnabled()))) {
                toReturn.add(nodeSecurity);
            }
        }
        return toReturn;
    }

    protected void sendReverseInitialLoad(ProcessInfo processInfo, List<TriggerHistory> activeHistories,
            Map<String, List<TriggerRouter>> triggerRoutersByNodeGroup) {        
        INodeService nodeService = engine.getNodeService();
        boolean queuedLoad = false;
        List<Node> nodes = new ArrayList<Node>();
        nodes.addAll(nodeService.findTargetNodesFor(NodeGroupLinkAction.P));
        nodes.addAll(nodeService.findTargetNodesFor(NodeGroupLinkAction.W));
        for (Node node : nodes) {
            List<TriggerRouter> triggerRouters = getTriggerRoutersForNodeGroup(triggerRoutersByNodeGroup, node.getNodeGroupId()); 
            engine.getDataService().insertReloadEvents(node, true, null, processInfo, activeHistories, triggerRouters, null);
            queuedLoad = true;
        }

        if (!queuedLoad) {
            log.info("{} was enabled but no nodes were linked to load", ParameterConstants.AUTO_RELOAD_REVERSE_ENABLED);
        }
    }

    protected boolean isValidLoadTarget(String targetNodeId) {
        boolean result = false;
        NodeSecurity targetNodeSecurity = engine.getNodeService().findNodeSecurity(targetNodeId);

        boolean reverseLoadFirst = parameterService.is(ParameterConstants.INITIAL_LOAD_REVERSE_FIRST);
        boolean registered = targetNodeSecurity.getRegistrationTime() != null;
        boolean reverseLoadQueued = targetNodeSecurity.isRevInitialLoadEnabled();

        if (registered && (!reverseLoadFirst || !reverseLoadQueued)) {
            result = true;
        } else {
            log.info("Unable to process load for target node id " + targetNodeId + " [registered: " + registered + ", reverse load first: "
                    + reverseLoadFirst + ", reverse load queued: " + reverseLoadQueued + "]");
        }
        return result;
    }

}
