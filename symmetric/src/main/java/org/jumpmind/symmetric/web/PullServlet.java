/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>
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

import java.io.IOException;
import java.net.SocketException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;


public class PullServlet extends AbstractServlet {

    private static final Log logger = LogFactory.getLog(PullServlet.class);

    private static final long serialVersionUID = 1L;

    @Override
    public void handleGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        handlePost(req, resp);
    }

    @Override
    protected void handlePost(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {

        String nodeId = req.getParameter(WebConstants.NODE_ID);

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Pull request received from %s", nodeId));
        }

        if (StringUtils.isBlank(nodeId)) {
            sendError(resp, HttpServletResponse.SC_BAD_REQUEST, "Node must be specified");
            return;
        }

        nodeId = nodeId.trim();
        INodeService nodeService = getNodeService();
        try {
            NodeSecurity nodeSecurity = nodeService.findNodeSecurity(nodeId);
            if (nodeSecurity != null) {
	            if (nodeSecurity.isRegistrationEnabled()) {
	                getRegistrationService().registerNode(nodeService.findNode(nodeId), resp.getOutputStream());
	            } else {
	                if (nodeSecurity.isInitialLoadEnabled()) {
	                    getDataService().insertReloadEvent(nodeService.findNode(nodeId));
	                }
	                IOutgoingTransport out = createOutgoingTransport(resp);
	                getDataExtractorService().extract(getNodeService().findNode(nodeId), out);
	                out.close();
	            }
            } else {
            	if (logger.isWarnEnabled()) {
            		logger.warn(String.format("Node %s does not exist.", nodeId));
            	}
            }
            
        } catch (SocketException ex) {
        	if (logger.isWarnEnabled()) {
        		logger.warn(String.format("Socket error while procesing pull data for %s.", nodeId), ex);
        	}
        } catch (Exception ex) {
        	if (logger.isErrorEnabled()) {
        		logger.error(String.format("Error while pulling data for %s", nodeId), ex);
        	}
            sendError(resp, HttpServletResponse.SC_NOT_IMPLEMENTED); // SC_INTERNAL_SERVER_ERROR?
        }
    }

	@Override
    protected Log getLogger() {
        return logger;
    }

}
