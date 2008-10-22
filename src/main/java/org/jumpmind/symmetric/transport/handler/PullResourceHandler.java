/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>,
 *               Keith Naas <knaas@users.sourceforge.net>
 *               
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

package org.jumpmind.symmetric.transport.handler;

import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public class PullResourceHandler extends AbstractTransportResourceHandler {
    private static final Log logger = LogFactory.getLog(PullResourceHandler.class);

    private INodeService nodeService;

    private IDataService dataService;

    private IDataExtractorService dataExtractorService;

    private IRegistrationService registrationService;

    public void pull(String nodeId, OutputStream outputStream) throws Exception {
        INodeService nodeService = getNodeService();
        NodeSecurity nodeSecurity = nodeService.findNodeSecurity(nodeId);
        if (nodeSecurity != null) {
            if (nodeSecurity.isRegistrationEnabled()) {
                getRegistrationService().registerNode(nodeService.findNode(nodeId), outputStream);
            } else {
                if (nodeSecurity.isInitialLoadEnabled()) {
                    getDataService().insertReloadEvent(nodeService.findNode(nodeId));
                }
                IOutgoingTransport outgoingTransport = createOutgoingTransport(outputStream);
                getDataExtractorService().extract(nodeService.findNode(nodeId), outgoingTransport);
                outgoingTransport.close();
            }
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn(String.format("Node %s does not exist.", nodeId));
            }
        }
    }

    private INodeService getNodeService() {
        return nodeService;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    private IRegistrationService getRegistrationService() {
        return registrationService;
    }

    public void setRegistrationService(IRegistrationService registrationService) {
        this.registrationService = registrationService;
    }

    private IDataService getDataService() {
        return dataService;
    }

    public void setDataService(IDataService dataService) {
        this.dataService = dataService;
    }

    private IDataExtractorService getDataExtractorService() {
        return dataExtractorService;
    }

    public void setDataExtractorService(IDataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
    }
}
