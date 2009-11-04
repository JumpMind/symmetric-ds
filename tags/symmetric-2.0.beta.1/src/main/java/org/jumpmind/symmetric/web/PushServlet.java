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

import org.jumpmind.symmetric.transport.handler.PushResourceHandler;

public class PushServlet extends AbstractTransportResourceServlet<PushResourceHandler> {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean isContainerCompatible() {
        return true;
    }

    @Override
    protected void doHead(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        // HTTP OK
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        String nodeId = getParameter(req, WebConstants.NODE_ID);
        log.debug("DataPushing", nodeId);
        InputStream inputStream = createInputStream(req);
        OutputStream outputStream = createOutputStream(resp);

        getTransportResourceHandler().push(inputStream, outputStream);

        // Not sure if this is necessary, but it's been here and it hasn't hurt
        // anything ...
        outputStream.flush();
        log.debug("DataPushingCompleted", nodeId);
    }
}
