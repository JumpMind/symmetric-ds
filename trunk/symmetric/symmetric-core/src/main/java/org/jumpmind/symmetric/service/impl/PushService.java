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

package org.jumpmind.symmetric.service.impl;

import java.io.BufferedReader;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.BatchAck;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPushService;
import org.jumpmind.symmetric.service.RegistrationRequiredException;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.transport.SyncDisabledException;
import org.jumpmind.symmetric.transport.TransportException;

/**
 * @see IPushService
 */
public class PushService extends AbstractOfflineDetectorService implements IPushService {

    private IDataExtractorService dataExtractorService;

    private IAcknowledgeService acknowledgeService;

    private ITransportManager transportManager;

    private INodeService nodeService;

    private IClusterService clusterService;

    private Map<String, Date> startTimesOfNodesBeingPushedTo = new HashMap<String, Date>();

    public PushService(IParameterService parameterService, ISymmetricDialect symmetricDialect,
            IDataExtractorService dataExtractorService, IAcknowledgeService acknowledgeService,
            ITransportManager transportManager, INodeService nodeService,
            IClusterService clusterService) {
        super(parameterService, symmetricDialect);
        this.dataExtractorService = dataExtractorService;
        this.acknowledgeService = acknowledgeService;
        this.transportManager = transportManager;
        this.nodeService = nodeService;
        this.clusterService = clusterService;
    }

    public Map<String, Date> getStartTimesOfNodesBeingPushedTo() {
        return new HashMap<String, Date>(startTimesOfNodesBeingPushedTo);
    }

    synchronized public RemoteNodeStatuses pushData() {
        RemoteNodeStatuses statuses = new RemoteNodeStatuses();

        Node identity = nodeService.findIdentity(false);
        if (identity != null && identity.isSyncEnabled()) {
            if (clusterService.lock(ClusterConstants.PUSH)) {
                try {
                    NodeSecurity identitySecurity = nodeService.findNodeSecurity(identity
                            .getNodeId());
                    List<Node> nodes = nodeService.findNodesToPushTo();
                    if (nodes != null && nodes.size() > 0) {
                        if (identitySecurity != null) {
                            for (Node node : nodes) {
                                if (StringUtils.isNotBlank(node.getSyncUrl()) 
                                        && !node.getNodeId().equals(identity.getNodeId())) {
                                    try {
                                        startTimesOfNodesBeingPushedTo.put(node.getNodeId(),
                                                new Date());
                                        log.debug("Push requested for {}", node);
                                        RemoteNodeStatus status = pushToNode(node, identity,
                                                identitySecurity);
                                        statuses.add(status);
                                        if (status.getBatchesProcessed() > 0) {
                                            log.info(
                                                    "Pushed data to {}. {} data and {} batches were processed",
                                                    new Object[] { node, status.getDataProcessed(),
                                                            status.getBatchesProcessed() });
                                        } else if (status.failed()) {
                                            log.warn("There was an error while pushing data to the server");
                                        }
                                        log.debug("Push completed for {}", node);
                                    } finally {
                                        startTimesOfNodesBeingPushedTo.remove(node.getNodeId());
                                    }
                                } else {
                                    log.warn(
                                            "Cannot push to node '{}' in the group '{}'.  The sync url is blank",
                                            node.getNodeId(), node.getNodeGroupId());
                                }
                            }
                        } else {
                            log.error(
                                    "Could not find a node security row for {}.  A node needs a matching security row in both the local and remote nodes if it is going to authenticate to push data",
                                    identity.getNodeId());
                        }
                    }
                } finally {
                    clusterService.unlock(ClusterConstants.PUSH);
                }
            } else {
                log.info("Did not run the push process because the cluster service has it locked");
            }
        }
        return statuses;
    }

    private RemoteNodeStatus pushToNode(Node remote, Node identity, NodeSecurity identitySecurity) {
        RemoteNodeStatus status = new RemoteNodeStatus(remote.getNodeId());
        IOutgoingWithResponseTransport transport = null;
        try {
            transport = transportManager.getPushTransport(remote, identity,
                    identitySecurity.getNodePassword(), parameterService.getRegistrationUrl());

            List<OutgoingBatch> extractedBatches = dataExtractorService.extract(remote, transport);
            if (extractedBatches.size() > 0) {
                log.info("Push data sent to {}", remote);
                BufferedReader reader = transport.readResponse();
                String ackString = reader.readLine();
                String ackExtendedString = reader.readLine();

                log.debug("Reading ack: {}", ackString);
                log.debug("Reading extend ack: {}", ackExtendedString);

                String line = null;
                do {
                    line = reader.readLine();
                    if (line != null) {
                        log.info("Read another unexpected line {}", line);
                    }
                } while (line != null);

                if (StringUtils.isBlank(ackString)) {
                    log.error("Did not receive an acknowledgement for the batches sent");
                }

                List<BatchAck> batches = transportManager.readAcknowledgement(ackString,
                        ackExtendedString);

                for (BatchAck batchInfo : batches) {
                    log.debug("Saving ack: {}, {}", batchInfo.getBatchId(),
                            (batchInfo.isOk() ? "OK" : "error"));
                    acknowledgeService.ack(batchInfo);
                }

                status.updateOutgoingStatus(extractedBatches, batches);
            }
        } catch (ConnectException ex) {
            log.warn("", (remote.getSyncUrl() == null ? parameterService.getRegistrationUrl()
                    : remote.getSyncUrl()));
            fireOffline(ex, remote, status);
        } catch (ConnectionRejectedException ex) {
            log.warn("The server was too busy to accept the connection");
            fireOffline(ex, remote, status);
        } catch (SocketException ex) {
            log.warn("{}", ex.getMessage());
            fireOffline(ex, remote, status);
        } catch (TransportException ex) {
            log.warn("{}", ex.getMessage());
            fireOffline(ex, remote, status);
        } catch (AuthenticationException ex) {
            log.warn("Could not authenticate with node");
            fireOffline(ex, remote, status);
        } catch (SyncDisabledException ex) {
            log.warn("Synchronization is disabled on the server node");
            fireOffline(ex, remote, status);
        } catch (RegistrationRequiredException ex) {
            log.warn("Registration has not been opened for this node");
            fireOffline(ex, remote, status);
        } catch (Exception ex) {
            // just report the error because we want to push to other nodes
            // in our list
            log.error(ex.getMessage(), ex);
            fireOffline(ex, remote, status);
        } finally {
            try {
                transport.close();
            } catch (Exception e) {
            }
        }
        return status;
    }

}
