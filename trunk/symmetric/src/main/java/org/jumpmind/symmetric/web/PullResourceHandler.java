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

package org.jumpmind.symmetric.web;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.springframework.context.ApplicationContext;

public class PullResourceHandler extends ResourceHandler {
    private static final Log logger = LogFactory
            .getLog(PullResourceHandler.class);

    public PullResourceHandler(ApplicationContext context,
            InputStream inputStream, OutputStream outputStream) {
        super(context, inputStream, outputStream);
    }

    public void pull(String nodeId) throws Exception {
        INodeService nodeService = getNodeService();
        NodeSecurity nodeSecurity = nodeService.findNodeSecurity(nodeId);
        if (nodeSecurity != null) {
            if (nodeSecurity.isRegistrationEnabled()) {
                getRegistrationService().registerNode(
                        nodeService.findNode(nodeId), outputStream);
            } else {
                if (nodeSecurity.isInitialLoadEnabled()) {
                    getDataService().insertReloadEvent(
                            nodeService.findNode(nodeId));
                }
                IOutgoingTransport outgoingTransport = createOutgoingTransport(outputStream);
                getDataExtractorService().extract(nodeService.findNode(nodeId),
                        outgoingTransport);
                outgoingTransport.close();
            }
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn(String.format("Node %s does not exist.", nodeId));
            }
        }
    }
}
