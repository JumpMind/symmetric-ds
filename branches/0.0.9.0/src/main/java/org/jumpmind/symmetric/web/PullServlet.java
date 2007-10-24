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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

/**
 * @author awilcox
 * 
 */
public class PullServlet extends AbstractServlet {

    private static final Log logger = LogFactory.getLog(PullServlet.class);

    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {

        String nodeId = req.getParameter(WebConstants.NODE_ID);

        if (logger.isDebugEnabled()) {
            logger.debug("Pull request received from " + nodeId);
        }

        if (StringUtils.isBlank(nodeId)) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        nodeId = nodeId.trim();
        INodeService nodeService = getNodeService();
        try {
            if (nodeService.isRegistrationEnabled(nodeId)) {
                IRegistrationService registrationService = getRegistrationService();
                registrationService.registerNode(nodeService.findNode(nodeId), resp.getOutputStream());
            } else {
                IOutgoingTransport out = createOutgoingTransport(resp);
                getDataExtractorService().extract(getNodeService().findNode(nodeId), out);
                out.close();
            }
        } catch (Exception ex) {
            logger.error("Error while pulling data for " + nodeId, ex);
            resp.sendError(501);
        }
    }

    @Override
    protected Log getLogger() {
        return logger;
    }

}
