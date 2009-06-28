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

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.transport.handler.PullResourceHandler;

public class PullServlet extends AbstractTransportResourceServlet<PullResourceHandler> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean isContainerCompatible() {
        return true;
    }

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {

        String nodeId = getParameter(req, WebConstants.NODE_ID);

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Pull request received from %s", nodeId));
        }

        if (StringUtils.isBlank(nodeId)) {
            sendError(resp, HttpServletResponse.SC_BAD_REQUEST, "Node must be specified");
            return;
        }
        OutputStream outputStream = createOutputStream(resp);
        getTransportResourceHandler().pull(nodeId, outputStream);

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Done with Pull request from %s", nodeId));
        }
    }

}
