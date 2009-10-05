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

import java.io.IOException;
import java.io.OutputStream;

import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public class PullResourceHandler extends AbstractTransportResourceHandler {

    private INodeService nodeService;

    private IConfigurationService configurationService;

    private IDataService dataService;

    private IDataExtractorService dataExtractorService;

    private IRegistrationService registrationService;

    public void pull(String nodeId, OutputStream outputStream, ChannelMap map) throws IOException {
        INodeService nodeService = getNodeService();
        NodeSecurity nodeSecurity = nodeService.findNodeSecurity(nodeId);
         ;
        ChannelMap remoteSuspendIgnoreChannelsList = configurationService.getSuspendIgnoreChannelLists(nodeId);
        map.addSuspendChannels(remoteSuspendIgnoreChannelsList.getSuspendChannels());
        map.addIgnoreChannels(remoteSuspendIgnoreChannelsList.getIgnoreChannels());

        if (nodeSecurity != null) {
            if (nodeSecurity.isRegistrationEnabled()) {
                registrationService.registerNode(nodeService.findNode(nodeId), outputStream, false);
            } else {
                if (nodeSecurity.isInitialLoadEnabled()) {
                    dataService.insertReloadEvent(nodeService.findNode(nodeId));
                }
                IOutgoingTransport outgoingTransport = createOutgoingTransport(outputStream, map);
                dataExtractorService.extract(nodeService.findNode(nodeId), outgoingTransport);
                outgoingTransport.close();
            }
        } else {
            log.warn("NodeMissing", nodeId);
        }
    }

    private INodeService getNodeService() {
        return nodeService;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public IConfigurationService getConfigurationService() {
        return configurationService;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setRegistrationService(IRegistrationService registrationService) {
        this.registrationService = registrationService;
    }

    public void setDataService(IDataService dataService) {
        this.dataService = dataService;
    }

    public void setDataExtractorService(IDataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
    }

}
