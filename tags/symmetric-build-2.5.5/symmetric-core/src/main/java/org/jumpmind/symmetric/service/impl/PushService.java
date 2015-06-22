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


package org.jumpmind.symmetric.service.impl;

import java.io.BufferedReader;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.model.BatchInfo;
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

    private IDataExtractorService extractor;

    private IAcknowledgeService ackService;

    private ITransportManager transportManager;

    private INodeService nodeService;

    private IClusterService clusterService;

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
                                log.debug("DataPushing", node);
                                RemoteNodeStatus status = pushToNode(node, identity, identitySecurity);
                                statuses.add(status);
                                if (status.getBatchesProcessed() > 0) {
                                    log.info("DataPushed", node);
                                } else if (status.failed()) {
                                    log.warn("DataPushingFailed");
                                }
                                log.debug("DataPushingCompleted", node);
                            }
                        } else {
                            log.error("NodeSecurityMissing", identity.getNodeId());
                        }
                    }
                } finally {
                    clusterService.unlock(ClusterConstants.PUSH);
                }
            } else {
                log.info("DataPushingFailedLock");
            }
        }
        return statuses;
    }

    private RemoteNodeStatus pushToNode(Node remote, Node identity, NodeSecurity identitySecurity) {
        RemoteNodeStatus status = new RemoteNodeStatus(remote.getNodeId());
        IOutgoingWithResponseTransport transport = null;
        try {
            transport = transportManager.getPushTransport(remote, identity, identitySecurity
                    .getNodePassword(), parameterService.getRegistrationUrl());

            List<OutgoingBatch> extractedBatches = extractor.extract(remote, transport);
            if (extractedBatches.size() > 0) {
                log.info("DataSent", remote);
                BufferedReader reader = transport.readResponse();
                String ackString = reader.readLine();
                String ackExtendedString = reader.readLine();

                log.debug("DataAckReading", ackString);
                log.debug("DataAckExtendedReading", ackExtendedString);

                if (StringUtils.isBlank(ackString)) {
                    log.error("DataAckReadingFailed");
                }

                List<BatchInfo> batches = transportManager.readAcknowledgement(ackString,
                        ackExtendedString);

                for (BatchInfo batchInfo : batches) {
                    log.debug("DataAckSaving", batchInfo.getBatchId(), (batchInfo.isOk() ? "OK"
                            : "error"));
                    ackService.ack(batchInfo);
                }
                
                status.updateOutgoingStatus(extractedBatches, batches);
            } 
        } catch (ConnectException ex) {
            log.warn("TransportFailedConnectionUnavailable",
                    (remote.getSyncUrl() == null ? parameterService.getRegistrationUrl() : remote
                            .getSyncUrl()));
            fireOffline(ex, remote, status);
        } catch (ConnectionRejectedException ex) {
            log.warn("TransportFailedConnectionBusy");
            fireOffline(ex, remote, status);
        } catch (SocketException ex) {
            log.warn("Message", ex.getMessage());
            fireOffline(ex, remote, status);
        } catch (TransportException ex) {
            log.warn("Message", ex.getMessage());
            fireOffline(ex, remote, status);
        } catch (AuthenticationException ex) {
            log.warn("AuthenticationFailed");
            fireOffline(ex, remote, status);
        } catch (SyncDisabledException ex) {
            log.warn("SyncDisabled");
            fireOffline(ex, remote, status);
        } catch (RegistrationRequiredException ex) {
            log.warn("RegistrationRequired");
            fireOffline(ex, remote, status);
        } catch (Exception ex) {
            // just report the error because we want to push to other nodes
            // in our list
            log.error(ex);
            fireOffline(ex, remote, status);
        } finally {
            try {
                transport.close();
            } catch (Exception e) {
            }
        }
        return status;
    }

    public void setExtractor(IDataExtractorService extractor) {
        this.extractor = extractor;
    }

    public void setTransportManager(ITransportManager tm) {
        this.transportManager = tm;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setAckService(IAcknowledgeService ackService) {
        this.ackService = ackService;
    }
    
    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
    }

}