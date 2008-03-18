/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>,
 *               Keith Naas <knaas@users.sourceforge.net>
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.springframework.context.ApplicationContext;

public class RegistrationServlet extends AbstractServlet {

    private static final long serialVersionUID = 1L;

    protected static final Log logger = LogFactory
            .getLog(RegistrationServlet.class);

    @Override
    protected void handleGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        ApplicationContext ctx = getContext();
        IRegistrationService service = (IRegistrationService) ctx
                .getBean(Constants.REGISTRATION_SERVICE);
        Node node = new Node();
        node.setNodeGroupId(req.getParameter(WebConstants.NODE_GROUP_ID));
        node.setSymmetricVersion(req
                .getParameter(WebConstants.SYMMETRIC_VERSION));
        node.setExternalId(req.getParameter(WebConstants.EXTERNAL_ID));
        String syncUrlString = req.getParameter(WebConstants.SYNC_URL);
        if (syncUrlString != null && !syncUrlString.trim().equals("")) {
            node.setSyncURL(syncUrlString);
        }
        node.setSchemaVersion(req.getParameter(WebConstants.SCHEMA_VERSION));
        node.setDatabaseType(req.getParameter(WebConstants.DATABASE_TYPE));
        node
                .setDatabaseVersion(req
                        .getParameter(WebConstants.DATABASE_VERSION));
        if (!service.registerNode(node, resp.getOutputStream())) {
            if (logger.isWarnEnabled()) {
                logger.warn(String.format("%s was not allowed to register.",
                        node));
            }
            sendError(resp, WebConstants.REGISTRATION_NOT_OPEN, String.format(
                    "%s was not allowed to register.", node));
        }
    }

    @Override
    protected Log getLogger() {
        return logger;
    }

}
