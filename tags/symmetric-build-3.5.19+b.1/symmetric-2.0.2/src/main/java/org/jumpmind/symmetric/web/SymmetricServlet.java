/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Keith Naas <knaas@users.sourceforge.net>
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.springframework.context.ApplicationContext;

/**
 * The SymmetricServlet manages all of the other servlets. This allows for
 * easier configuration since spring manages the individual servlets.
 * 
 * Configured within web.xml
 * 
 * <pre>
 *  &lt;servlet&gt;
 *    &lt;servlet-name&gt;SymmetricServlet&lt;/filter-name&gt;
 *    &lt;servlet-class&gt;
 *      org.jumpmind.symmetric.web.SymmetricServlet
 *    &lt;/servlet-class&gt;
 *  &lt;/servlet&gt;
 * 
 *  &lt;servlet-mapping&gt;
 *    &lt;servlet-name&gt;SymmetricServlet&lt;/servlet-name&gt;
 *    &lt;url-pattern&gt;*&lt;/url-pattern&gt;
 *  &lt;/servlet-mapping&gt;
 * </pre>
 * 
 * @since 1.4.0
 */
public class SymmetricServlet extends AbstractServlet {

    private static final long serialVersionUID = 1L;

    private static final ILog log = LogFactory.getLog(SymmetricServlet.class);

    private List<IServletExtension> servlets;

    @Override
    protected ILog getLog() {
        return log;
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        servlets = new ArrayList<IServletExtension>();
        ApplicationContext ctx = ServletUtils.getApplicationContext(getServletContext());
        final Map<String, IServletExtension> servletBeans = new LinkedHashMap<String, IServletExtension>();
        servletBeans.putAll(ctx.getBeansOfType(IServletExtension.class));
        if (ctx.getParent() != null) {
            servletBeans.putAll(ctx.getParent().getBeansOfType(IServletExtension.class));
        }
        // TODO order using initOrder
        for (final Map.Entry<String, IServletExtension> servletEntry : servletBeans.entrySet()) {
            log.debug("ServletInitializing", servletEntry.getKey());
            final IServletExtension extension = servletEntry.getValue();
            extension.getServlet().init(config);
            servlets.add(extension);
        }

        if (servlets.size() == 0) {
            log.error("ServletNoneFound");
        }
    }

    public void destroy() {
        for (final IServletExtension extension : servlets) {
            extension.getServlet().destroy();
        }
    }

    protected Servlet findMatchingServlet(ServletRequest req, ServletResponse resp) {
        Servlet retVal = null;
        for (Iterator<IServletExtension> iterator = servlets.iterator(); retVal == null && iterator.hasNext();) {
            IServletExtension extension = iterator.next();
            if (!extension.isDisabled() && matches(extension, req)) {
                retVal = extension.getServlet();
            }
        }
        return retVal;
    }

    public boolean matches(IServletExtension ext, ServletRequest request) {
        boolean retVal = true;
        if (request instanceof HttpServletRequest) {
            final HttpServletRequest httpRequest = (HttpServletRequest) request;
            final String uri = normalizeRequestUri(httpRequest);
            if (!ArrayUtils.isEmpty(ext.getUriPatterns())) {
                retVal = matchesUriPatterns(uri, ext.getUriPatterns());
            }
        }
        return retVal;
    }

    protected boolean matchesUriPatterns(String uri, String[] uriPatterns) {
        boolean retVal = false;
        for (int i = 0; !retVal && i < uriPatterns.length; i++) {
            retVal = matchesUriPattern(uri, uriPatterns[i]);
        }
        return retVal;
    }

    protected boolean matchesUriPattern(String uri, String uriPattern) {
        boolean retVal = false;
        String path = StringUtils.defaultIfEmpty(uri, "/");
        final String pattern = StringUtils.defaultIfEmpty(uriPattern, "/");
        if ("/".equals(pattern) || "/*".equals(pattern) || pattern.equals(path)) {
            retVal = true;
        } else {
            final String[] patternParts = StringUtils.split(pattern, "/");
            final String[] pathParts = StringUtils.split(path, "/");
            boolean matches = true;
            for (int i = 0; i < patternParts.length && i < pathParts.length && matches; i++) {
                final String patternPart = patternParts[i];
                matches = "*".equals(patternPart) || patternPart.equals(pathParts[i]);
            }
            retVal = matches;
        }
        return retVal;
    }

    /**
     * Returns the part of the path we are interested in when doing pattern
     * matching. This should work whether or not the servlet or filter is
     * explicitly mapped inside of the web.xml since it always strips off the
     * contextPath.
     * 
     * @param httpRequest
     * @return
     */
    protected String normalizeRequestUri(HttpServletRequest httpRequest) {
        String retVal = httpRequest.getRequestURI();
        String contextPath = httpRequest.getContextPath();
        if (retVal.startsWith(contextPath)) {
            retVal = retVal.substring(contextPath.length());
        }
        return retVal;
    }

    @Override
    public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
        Servlet servlet = findMatchingServlet(req, res);
        if (servlet != null) {
            try {
                servlet.service(req, res);
            } catch (IOException e) {
                logException(req, e, false);
            } catch (Exception e) {
                logException(req, e, true);
                if (!res.isCommitted()) {
                    if (res instanceof HttpServletResponse) {
                        ((HttpServletResponse) res).sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    }
                }
            }
        } else {
            if (req instanceof HttpServletRequest) {
                HttpServletRequest httpRequest = (HttpServletRequest) req;
                log.error("ServletNotFoundToHandleRequest", normalizeRequestUri(httpRequest));
            }
        }
    }

    protected void logException(ServletRequest req, Exception ex, boolean isError) {
        String nodeId = req.getParameter(WebConstants.NODE_ID);
        String externalId = req.getParameter(WebConstants.EXTERNAL_ID);
        String address = req.getRemoteAddr();
        String hostName = req.getRemoteHost();
        String method = req instanceof HttpServletRequest ? ((HttpServletRequest) req).getMethod() : "";
        if (getLog().isErrorEnabled() && isError) {
            getLog().error("ServletProcessingFailedError", ex, method, externalId, nodeId, address, hostName);
        } else if (getLog().isWarnEnabled()) {
            getLog().warn("ServletProcessingFailedWarning", method, externalId, nodeId, address, hostName,
                    ex.getMessage());
        }
    }
}
