/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Andrew Wilcox <andrewbwilcox@users.sourceforge.net>,
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
import java.io.InputStream;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.transport.handler.PushResourceHandler;

public class PushServlet extends AbstractTransportResourceServlet<PushResourceHandler> {
    private static final long serialVersionUID = 1L;

    private static final Log logger = LogFactory.getLog(PushServlet.class);

    @Override
    public boolean isContainerCompatible() {
        return true;
    }

    @Override
    protected void handleHead(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        // HTTP OK
    }

    @Override
    protected void handlePut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        String nodeId = getParameter(req, WebConstants.NODE_ID);

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Push request received from %s", nodeId));
        }

        InputStream inputStream = createInputStream(req);
        OutputStream outputStream = createOutputStream(resp);

        getTransportResourceHandler().push(inputStream, outputStream);

        outputStream.flush(); // TODO: why is this necessary?

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Done with Push request from %s", nodeId));
        }
    }

    @Override
    protected Log getLogger() {
        return logger;
    }

}
