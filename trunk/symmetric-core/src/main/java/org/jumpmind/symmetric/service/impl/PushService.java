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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.BatchAck;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeCommunication;
import org.jumpmind.symmetric.model.NodeCommunication.CommunicationType;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfo.Status;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessInfoKey.ProcessType;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.INodeCommunicationService;
import org.jumpmind.symmetric.service.INodeCommunicationService.INodeCommunicationExecutor;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPushService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.ITransportManager;

/**
 * @see IPushService
 */
public class PushService extends AbstractOfflineDetectorService implements IPushService,
        INodeCommunicationExecutor {

    private IDataExtractorService dataExtractorService;

    private IAcknowledgeService acknowledgeService;

    private ITransportManager transportManager;

    private INodeService nodeService;

    private IClusterService clusterService;

    private INodeCommunicationService nodeCommunicationService;
    
    private IStatisticManager statisticManager;
    
    private IConfigurationService configurationService;

    private Map<String, Date> startTimesOfNodesBeingPushedTo = new HashMap<String, Date>();

    public PushService(IParameterService parameterService, ISymmetricDialect symmetricDialect,
            IDataExtractorService dataExtractorService, IAcknowledgeService acknowledgeService,
            ITransportManager transportManager, INodeService nodeService,
            IClusterService clusterService, INodeCommunicationService nodeCommunicationService, IStatisticManager statisticManager, IConfigurationService configrationService) {
        super(parameterService, symmetricDialect);
        this.dataExtractorService = dataExtractorService;
        this.acknowledgeService = acknowledgeService;
        this.transportManager = transportManager;
        this.nodeService = nodeService;
        this.clusterService = clusterService;
        this.nodeCommunicationService = nodeCommunicationService;
        this.statisticManager = statisticManager;
        this.configurationService = configrationService;
    }

    public Map<String, Date> getStartTimesOfNodesBeingPushedTo() {
        return new HashMap<String, Date>(startTimesOfNodesBeingPushedTo);
    }

    synchronized public RemoteNodeStatuses pushData(boolean force) {
        RemoteNodeStatuses statuses = new RemoteNodeStatuses(configurationService.getChannels(false));
        
        Node identity = nodeService.findIdentity(false);
        if (identity != null && identity.isSyncEnabled()) {
            long minimumPeriodMs = parameterService.getLong(ParameterConstants.PUSH_MINIMUM_PERIOD_MS, -1);
            if (force || !clusterService.isInfiniteLocked(ClusterConstants.PUSH)) {
                    List<NodeCommunication> nodes = nodeCommunicationService
                            .list(CommunicationType.PUSH);
                    if (nodes.size() > 0) {
                        NodeSecurity identitySecurity = nodeService.findNodeSecurity(identity
                                .getNodeId());
                        if (identitySecurity != null) {
                            int availableThreads = nodeCommunicationService
                                    .getAvailableThreads(CommunicationType.PUSH);
                            for (NodeCommunication nodeCommunication : nodes) {
                                boolean meetsMinimumTime = true;
                                if (minimumPeriodMs > 0 && nodeCommunication.getLastLockTime() != null &&
                                   (System.currentTimeMillis() - nodeCommunication.getLastLockTime().getTime()) < minimumPeriodMs) {
                                   meetsMinimumTime = false; 
                                }
                                if (availableThreads > 0 && meetsMinimumTime) {
                                    if (nodeCommunicationService.execute(nodeCommunication, statuses,
                                            this)) {
                                        availableThreads--;
                                    }
                                }
                            }
                        } else {
                            log.error(
                                    "Could not find a node security row for '{}'.  A node needs a matching security row in both the local and remote nodes if it is going to authenticate to push data",
                                    identity.getNodeId());
                        }
                    }
            } else {
                log.debug("Did not run the push process because it has been stopped");
            }
        }
        return statuses;
    }

    public void execute(NodeCommunication nodeCommunication, RemoteNodeStatus status) {
        Node node = nodeCommunication.getNode();
        if (StringUtils.isNotBlank(node.getSyncUrl()) || 
                !parameterService.isRegistrationServer()) {
            try {
                startTimesOfNodesBeingPushedTo.put(node.getNodeId(), new Date());
                long reloadBatchesProcessed = 0;
                long lastBatchCount = 0;
                do {
                    if (lastBatchCount > 0) {
                        log.info(
                                "Pushing to {} again because the last push contained reload batches",
                                node);
                    }
                    reloadBatchesProcessed = status.getReloadBatchesProcessed();
                    log.debug("Push requested for {}", node);
                    pushToNode(node, status);
                    if (!status.failed() && status.getBatchesProcessed() > 0 
                            && status.getBatchesProcessed() != lastBatchCount) {
                        log.info(
                                "Pushed data to {}. {} data and {} batches were processed",
                                new Object[] { node, status.getDataProcessed(),
                                        status.getBatchesProcessed()});
                    } else if (status.failed()) {
                        log.warn(
                                "There was a failure while pushing data to {}. {} data and {} batches were processed",
                                new Object[] { node, status.getDataProcessed(),
                                        status.getBatchesProcessed()});                        
                    }
                    log.debug("Push completed for {}", node);
                    lastBatchCount = status.getBatchesProcessed();
                } while (status.getReloadBatchesProcessed() > reloadBatchesProcessed && !status.failed());
            } finally {
                startTimesOfNodesBeingPushedTo.remove(node.getNodeId());
            }
        } else {
            log.warn("Cannot push to node '{}' in the group '{}'.  The sync url is blank",
                    node.getNodeId(), node.getNodeGroupId());
        }

    }

    private void pushToNode(Node remote, RemoteNodeStatus status) {
        Node identity = nodeService.findIdentity(false);
        NodeSecurity identitySecurity = nodeService.findNodeSecurity(identity.getNodeId());
        IOutgoingWithResponseTransport transport = null;
        ProcessInfo processInfo = statisticManager.newProcessInfo(new ProcessInfoKey(identity
                .getNodeId(), remote.getNodeId(), ProcessType.PUSH_JOB));
        try {
            transport = transportManager.getPushTransport(remote, identity,
                    identitySecurity.getNodePassword(), parameterService.getRegistrationUrl());

            List<OutgoingBatch> extractedBatches = dataExtractorService.extract(processInfo, remote, transport);
            if (extractedBatches.size() > 0) {
                
                log.info("Push data sent to {}", remote);
                
                List<BatchAck> batchAcks = readAcks(extractedBatches, transport, transportManager, acknowledgeService);
                status.updateOutgoingStatus(extractedBatches, batchAcks);
            }
            
            processInfo.setStatus(Status.DONE);
        } catch (Exception ex) {
            processInfo.setStatus(Status.ERROR);
            fireOffline(ex, remote, status);
        } finally {
            try {
                transport.close();
            } catch (Exception e) {
            }
        }
    }

}
