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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.ErrorConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IPullService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.TransportException;

public class PullService extends AbstractService implements IPullService {

    private static final Log logger = LogFactory.getLog(PullService.class);

    private INodeService nodeService;

    private IDataLoaderService dataLoaderService;

    private IRegistrationService registrationService;
    
    private IBootstrapService bootstrapService;

    public void pullData() {

        // register if we haven't already been registered
        if (!registrationService.isRegisteredWithServer()) {
            registrationService.registerWithServer();
            bootstrapService.syncTriggers();
        }

        List<Node> nodes = nodeService.findNodesToPull();
        if (nodes != null && nodes.size() > 0) {
            for (Node node : nodes) {
                String nodeName = " for " + node;
                try {
                    logger.info("Pull requested" + nodeName);
                    if (dataLoaderService.loadData(node, nodeService.findIdentity())) {
                        logger.info("Pull data received" + nodeName);
                    } else {
                        logger.info("Pull no data received" + nodeName);
                    }
                } catch (ConnectException ex) {
                    logger.warn(ErrorConstants.COULD_NOT_CONNECT_TO_TRANSPORT + " url=" + node.getSyncURL());
                } catch (ConnectionRejectedException ex) {
                    logger.warn(ErrorConstants.TRANSPORT_REJECTED_CONNECTION);
                } catch (AuthenticationException ex) {
                    logger.warn(ErrorConstants.NOT_AUTHENTICATED);
                } catch (SocketException ex) {
                    logger.warn(ex.getMessage());
                } catch (TransportException ex) {
                    logger.warn(ex.getMessage());
                } catch (IOException e) {
                    logger.error(e, e);
                }
            }
        }
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

    public void setBootstrapService(IBootstrapService bootstrapService) {
        this.bootstrapService = bootstrapService;
    }

}
