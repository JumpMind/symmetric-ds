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

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.springframework.context.ApplicationContext;

/**
 * This better be the first filter that executes !
 * TODO: if this thing fails, should it prevent further processing of the request?
 *
 */
public class AuthenticationFilter extends AbstractFilter
{
    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain)
        throws IOException, ServletException
    {
        String securityToken = req.getParameter(WebConstants.SECURITY_TOKEN);
        String nodeId = req.getParameter(WebConstants.NODE_ID);

        if (StringUtils.isEmpty(securityToken) || StringUtils.isEmpty(nodeId))
        {
            sendError(resp, HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        ApplicationContext ctx = getContext();
        INodeService sc = (INodeService) ctx.getBean(Constants.NODE_SERVICE);

        if (!sc.isNodeAuthorized(nodeId, securityToken))
        {
            IRegistrationService registrationService = (IRegistrationService) ctx
                    .getBean(Constants.REGISTRATION_SERVICE);
            if (registrationService.isAutoRegistration()) {
            	sendError(resp, WebConstants.REGISTRATION_REQUIRED);
            } else {
                sendError(resp, HttpServletResponse.SC_FORBIDDEN);
            }
            return;
        }

        chain.doFilter(req, resp);
    }
}
