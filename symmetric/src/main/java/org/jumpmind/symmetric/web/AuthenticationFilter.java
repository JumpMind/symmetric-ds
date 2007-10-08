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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.service.INodeService;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

public class AuthenticationFilter implements Filter
{
    private ServletContext context;

    public void destroy()
    {
    }

    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain)
        throws IOException, ServletException
    {
        String securityToken = req.getParameter(WebConstants.SECURITY_TOKEN);
        String clientId = req.getParameter(WebConstants.NODE_ID);

        if (securityToken == null || clientId == null)
        {
            ((HttpServletResponse)resp).sendError(HttpServletResponse.SC_FORBIDDEN);
        }

        ApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(context);
        INodeService sc = (INodeService) ctx.getBean(Constants.NODE_SERVICE);

        if (!sc.isNodeAuthorized(clientId, securityToken))
        {
            ((HttpServletResponse)resp).sendError(HttpServletResponse.SC_FORBIDDEN);
        }

        chain.doFilter(req, resp);
    }

    public void init(FilterConfig config) throws ServletException
    {
        context = config.getServletContext();
    }

}
