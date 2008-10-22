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
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.transport.handler.RegistrationResourceHandler;

public class RegistrationServlet extends AbstractTransportResourceServlet<RegistrationResourceHandler> {

    private static final long serialVersionUID = 1L;

    protected static final Log logger = LogFactory.getLog(RegistrationServlet.class);

    @Override
    public boolean isContainerCompatible() {
        return true;
    }

    @Override
    protected void handleGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Node node = transform(req);

        OutputStream outputStream = createOutputStream(resp);
        if (!getTransportResourceHandler().registerNode(node, outputStream)) {
            if (logger.isWarnEnabled()) {
                logger.warn(String.format("%s was not allowed to register.", node));
            }
            sendError(resp, WebConstants.REGISTRATION_NOT_OPEN, String.format("%s was not allowed to register.", node));
        }
    }

    private Node transform(HttpServletRequest req) {
        Node node = new Node();
        node.setNodeGroupId(getParameter(req, WebConstants.NODE_GROUP_ID));
        node.setSymmetricVersion(getParameter(req, WebConstants.SYMMETRIC_VERSION));
        node.setExternalId(getParameter(req, WebConstants.EXTERNAL_ID));
        String syncUrlString = getParameter(req, WebConstants.SYNC_URL);
        if (StringUtils.isNotBlank(syncUrlString)) {
            node.setSyncURL(syncUrlString);
        }
        node.setSchemaVersion(getParameter(req, WebConstants.SCHEMA_VERSION));
        node.setDatabaseType(getParameter(req, WebConstants.DATABASE_TYPE));
        node.setDatabaseVersion(getParameter(req, WebConstants.DATABASE_VERSION));
        return node;
    }

    @Override
    protected Log getLogger() {
        return logger;
    }

}
