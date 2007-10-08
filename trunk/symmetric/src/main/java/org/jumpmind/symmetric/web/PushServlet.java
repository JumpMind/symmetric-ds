/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
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

/**
 * @author awilcox
 */
public class PushServlet extends AbstractServlet {
    private static final long serialVersionUID = 1L;

    private static final Log logger = LogFactory.getLog(PushServlet.class);

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        String nodeId = req.getParameter(WebConstants.NODE_ID);
        
        if (logger.isDebugEnabled()) {
            logger.debug("Push request received from " + nodeId);
        }

        try {
            InputStream is = createInputStream(req);
            OutputStream out = createOutputStream(resp);
            getDataLoaderService().loadData(is, out);
            out.flush();
        } catch (Exception ex) {
            logger
                    .error("Error while processing pushed data for " + nodeId,
                            ex);
            resp.sendError(501);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Done with push request from "
                    + req.getParameter(WebConstants.NODE_ID));
        }
    }

    @Override
    protected Log getLogger() {
        return logger;
    }

}
