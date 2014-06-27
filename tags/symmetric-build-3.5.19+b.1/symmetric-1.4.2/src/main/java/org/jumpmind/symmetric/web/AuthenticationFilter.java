/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
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

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.transport.handler.AuthenticationResourceHandler;
import org.jumpmind.symmetric.transport.handler.AuthenticationResourceHandler.AuthenticationStatus;

/**
 * This better be the first filter that executes ! TODO: if this thing fails,
 * should it prevent further processing of the request?
 * 
 */
public class AuthenticationFilter extends AbstractTransportFilter<AuthenticationResourceHandler> {

    private static final Log logger = LogFactory.getLog(AuthenticationFilter.class);

    @Override
    public boolean isContainerCompatible() {
        return true;
    }

    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws IOException,
            ServletException {
        String securityToken = req.getParameter(WebConstants.SECURITY_TOKEN);
        String nodeId = req.getParameter(WebConstants.NODE_ID);

        if (StringUtils.isEmpty(securityToken) || StringUtils.isEmpty(nodeId)) {
            sendError(resp, HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        final AuthenticationStatus status = getTransportResourceHandler().status(nodeId, securityToken);
        if (AuthenticationStatus.FORBIDDEN.equals(status)) {
            sendError(resp, HttpServletResponse.SC_FORBIDDEN);
        } else if (AuthenticationStatus.REGISTRATION_REQUIRED.equals(status)) {
            sendError(resp, WebConstants.REGISTRATION_REQUIRED);
        } else if (AuthenticationStatus.ACCEPTED.equals(status)) {
            chain.doFilter(req, resp);
        }
    }

    @Override
    protected Log getLogger() {
        return logger;
    }
}
