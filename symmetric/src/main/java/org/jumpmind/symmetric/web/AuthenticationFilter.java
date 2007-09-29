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
