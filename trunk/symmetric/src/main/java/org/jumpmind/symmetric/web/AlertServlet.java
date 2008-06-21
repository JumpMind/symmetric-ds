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
import org.jumpmind.symmetric.transport.handler.AlertResourceHandler;

import com.sun.syndication.io.FeedException;

/**
 * This is a place holder for the idea that we could create an RSS feed for
 * alerts.
 */
public class AlertServlet extends AbstractTransportResourceServlet<AlertResourceHandler> {

    private static final long serialVersionUID = 1L;

    private static final Log logger = LogFactory.getLog(AlertServlet.class);

    @Override
    public boolean isContainerCompatible() {
        return true;
    }

    @Override
    public void handleGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException,
            FeedException {

        resp.setContentType("application/rss+xml");
        getTransportResourceHandler().write(req.getRequestURL(), resp.getWriter());
    }

    @Override
    protected Log getLogger() {
        return logger;
    }

}
