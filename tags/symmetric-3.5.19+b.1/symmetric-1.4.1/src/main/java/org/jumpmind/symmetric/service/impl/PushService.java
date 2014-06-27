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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.ErrorConstants;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IPushService;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.transport.TransportException;

public class PushService extends AbstractService implements IPushService {

    private static final Log logger = LogFactory.getLog(PushService.class);

    private IDataExtractorService extractor;

    private IAcknowledgeService ackService;

    private ITransportManager transportManager;

    private INodeService nodeService;

    private IDataService dataService;

    public void pushData() {
        List<Node> nodes = nodeService.findNodesToPushTo();
        if (nodes != null && nodes.size() > 0) {
            for (Node node : nodes) {
                logger.info("Push requested for " + node);
                if (pushToNode(node)) {
                    logger.info("Push completed for " + node);
                } else {
                    logger.info("Push unsuccessful for " + node);
                }
            }
        }
    }

    private boolean pushToNode(Node remote) {
        IOutgoingWithResponseTransport transport = null;
        boolean success = false;
        try {
            NodeSecurity nodeSecurity = nodeService.findNodeSecurity(remote.getNodeId());
            if (nodeSecurity != null) {
                if (nodeSecurity.isInitialLoadEnabled()) {
                    dataService.insertReloadEvent(remote);
                }
            }

            transport = transportManager.getPushTransport(remote, nodeService.findIdentity());

            if (extractor.extract(remote, transport)) {
                logger.info("Push data sent to " + remote);
                BufferedReader reader = transport.readResponse();
                String ackString = reader.readLine();
                String ackExtendedString = reader.readLine();

                if (logger.isDebugEnabled()) {
                    logger.debug("Reading ack: " + ackString);
                    logger.debug("Reading extended ack: " + ackExtendedString);
                }

                List<BatchInfo> batches = transportManager.readAcknowledgement(ackString, ackExtendedString);

                for (BatchInfo batchInfo : batches) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Saving ack: " + batchInfo.getBatchId() + ", "
                                + (batchInfo.isOk() ? "OK" : "error"));
                    }
                    ackService.ack(batchInfo);
                }
            }
            success = true;
        } catch (ConnectException ex) {
            logger.warn(ErrorConstants.COULD_NOT_CONNECT_TO_TRANSPORT + " url=" + remote.getSyncURL());
        } catch (ConnectionRejectedException ex) {
            logger.warn(ErrorConstants.TRANSPORT_REJECTED_CONNECTION);
        } catch (SocketException ex) {
            logger.warn(ex.getMessage());
        } catch (TransportException ex) {
            logger.warn(ex.getMessage());
        } catch (AuthenticationException ex) {
            logger.warn(ErrorConstants.NOT_AUTHENTICATED);
        } catch (Exception e) {
            // just report the error because we want to push to other nodes
            // in our list
            logger.error(e, e);
        } finally {
            try {
                transport.close();
            } catch (Exception e) {
            }
        }
        return success;
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
}
