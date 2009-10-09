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

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.List;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IPullService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.LockActionConstants;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.TransportException;

public class PullService extends AbstractOfflineDetectorService implements IPullService {

    private INodeService nodeService;

    private IDataLoaderService dataLoaderService;

    private IRegistrationService registrationService;

    private IClusterService clusterService;

    synchronized public boolean pullData() {
        boolean dataPulled = false;
        if (clusterService.lock(LockActionConstants.PULL)) {
            try {
                // register if we haven't already been registered
                registrationService.registerWithServer();

                List<Node> nodes = nodeService.findNodesToPull();
                if (nodes != null && nodes.size() > 0) {
                    for (Node node : nodes) {
                        String nodeName = " for " + node;
                        try {
                            log.debug("DataPulling", nodeName);
                            if (dataLoaderService.loadData(node, nodeService.findIdentity())) {
                                log.info("DataPulled", nodeName);
                                dataPulled = true;
                            } else {
                                log.debug("DataPullingFailed", nodeName);

                            }
                        } catch (ConnectException ex) {
                            log.warn("TransportFailedConnectionUnavailable",
                                    (node.getSyncURL() == null ? parameterService.getRegistrationUrl() : node
                                            .getSyncURL()));
                            fireOffline(ex, node);
                        } catch (ConnectionRejectedException ex) {
                            log.warn("TransportFailedConnectionBusy");
                            fireOffline(ex, node);
                        } catch (AuthenticationException ex) {
                            log.warn("AuthenticationFailed");
                            fireOffline(ex, node);
                        } catch (SocketException ex) {
                            log.warn("Message", ex.getMessage());
                            fireOffline(ex, node);
                        } catch (TransportException ex) {
                            log.warn("Message", ex.getMessage());
                            fireOffline(ex, node);
                        } catch (IOException ex) {
                            log.error(ex);
                            fireOffline(ex, node);
                        }
                    }
                }
            } finally {
                clusterService.unlock(LockActionConstants.PULL);

            }

        } else {
            log.info("DataPullingFailedLock");
        }

        return dataPulled;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setDataLoaderService(IDataLoaderService dataLoaderService) {
        this.dataLoaderService = dataLoaderService;
    }

    public void setRegistrationService(IRegistrationService registrationService) {
        this.registrationService = registrationService;
    }

    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
    }

}
