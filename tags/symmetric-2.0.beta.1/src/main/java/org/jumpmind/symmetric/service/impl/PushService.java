/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.service.impl;

import java.io.BufferedReader;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IPushService;
import org.jumpmind.symmetric.service.LockActionConstants;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.transport.TransportException;

public class PushService extends AbstractOfflineDetectorService implements IPushService {

    private IDataExtractorService extractor;

    private IAcknowledgeService ackService;

    private ITransportManager transportManager;

    private INodeService nodeService;

    private IDataService dataService;

    private IClusterService clusterService;

    private static enum PushStatus {
        PUSHED, ERROR, NOTHING_TO_PUSH
    };

    synchronized public boolean pushData() {
        boolean pushedData = false;
        boolean inError = false;
        if (clusterService.lock(LockActionConstants.PUSH)) {
            try {
                List<Node> nodes = nodeService.findNodesToPushTo();
                if (nodes != null && nodes.size() > 0) {
                    for (Node node : nodes) {
                        log.debug("DataPushing", node);
                        PushStatus status = pushToNode(node);
                        if (status == PushStatus.PUSHED) {
                            pushedData = true;
                            log.info("DataPushed", node);
                        } else if (status == PushStatus.ERROR) {
                            inError = true;
                            log.warn("DataPushingFailed");
                        }
                        log.debug("DataPushingCompleted", node);

                    }
                }
            } finally {
                clusterService.unlock(LockActionConstants.PUSH);
            }
        } else {
            log.info("DataPushingFailedLock");
        }
        return pushedData && !inError;
    }

    private PushStatus pushToNode(Node remote) {
        PushStatus status = PushStatus.ERROR;
        IOutgoingWithResponseTransport transport = null;
        try {
            NodeSecurity nodeSecurity = nodeService.findNodeSecurity(remote.getNodeId());
            if (nodeSecurity != null) {
                if (nodeSecurity.isInitialLoadEnabled()) {
                    dataService.insertReloadEvent(remote);
                }
            }

            transport = transportManager.getPushTransport(remote, nodeService.findIdentity());

            if (extractor.extract(remote, transport)) {
                log.info("DataSent", remote);
                BufferedReader reader = transport.readResponse();
                String ackString = reader.readLine();
                String ackExtendedString = reader.readLine();

                log.debug("DataAckReading", ackString);
                log.debug("DataAckExtendedReading", ackExtendedString);

                if (StringUtils.isBlank(ackString)) {
                    log.error("DataAckReadingFailed");
                }

                List<BatchInfo> batches = transportManager.readAcknowledgement(ackString, ackExtendedString);

                status = PushStatus.PUSHED;

                for (BatchInfo batchInfo : batches) {
                    log.debug("DataAckSaving", batchInfo.getBatchId(), (batchInfo.isOk() ? "OK" : "error"));
                    if (!batchInfo.isOk()) {
                        status = PushStatus.ERROR;
                    }
                    ackService.ack(batchInfo);

                }
            } else {
                status = PushStatus.NOTHING_TO_PUSH;
            }
        } catch (ConnectException ex) {            
            log.warn("TransportFailedConnectionUnavailable", (remote.getSyncURL() == null ? parameterService
                    .getRegistrationUrl() : remote.getSyncURL()));
            fireOffline(ex, remote);
        } catch (ConnectionRejectedException ex) {
            log.warn("TransportFailedConnectionBusy");
            fireOffline(ex, remote);
        } catch (SocketException ex) {
            log.warn("Message", ex.getMessage());
            fireOffline(ex, remote);
        } catch (TransportException ex) {
            log.warn("Message", ex.getMessage());
            fireOffline(ex, remote);
        } catch (AuthenticationException ex) {
            log.warn("AuthenticationFailed");
            fireOffline(ex, remote);
        } catch (Exception ex) {
            // just report the error because we want to push to other nodes
            // in our list
            log.error(ex);
            fireOffline(ex, remote);
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

    public void setDataService(IDataService dataService) {
        this.dataService = dataService;
    }

    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
    }

}
