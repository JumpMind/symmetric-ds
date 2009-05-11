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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

    private static final Log logger = LogFactory.getLog(SymmetricServlet.class);

    private List<HttpServlet> servlets;

    @Override
    protected Log getLogger() {

        return logger;
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

        servlets = new ArrayList<HttpServlet>();
        @SuppressWarnings("unchecked")
        final Map<String, HttpServlet> servletBeans = getDefaultApplicationContext().getBeansOfType(HttpServlet.class);
        // they will need to be sorted somehow, right now its just the order
        // they appear in the spring file
        for (final Map.Entry<String, HttpServlet> servletEntry : servletBeans.entrySet()) {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("Initializing servlet %s", servletEntry.getKey()));
            }
            final HttpServlet servlet = servletEntry.getValue();
            servlet.init(this.getServletConfig());
            servlets.add(servlet);
        }
    }

    public void destroy() {
        for (final HttpServlet servlet : servlets) {
            servlet.destroy();
        }
    }

    protected AbstractResourceServlet<?> findMatchingServlet(HttpServletRequest req, HttpServletResponse resp) {
        AbstractResourceServlet<?> retVal = null;
        for (Iterator<HttpServlet> iterator = servlets.iterator(); retVal == null && iterator.hasNext();) {
            HttpServlet servlet = iterator.next();
            if (servlet instanceof AbstractResourceServlet) {
                final AbstractResourceServlet<?> builtinServlet = (AbstractResourceServlet<?>) servlet;
                if (!builtinServlet.isDisabled() && builtinServlet.matches(req)) {
                    retVal = builtinServlet;
                }
            }
        }
        return retVal;
    }

    @Override
    protected void handleDelete(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        AbstractResourceServlet<?> servlet = findMatchingServlet(req, resp);
        if (servlet != null) {
            servlet.handleDelete(req, resp);
        } else {
            super.handleDelete(req, resp);
        }
    }

    @Override
    protected void handleGet(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        AbstractResourceServlet<?> servlet = findMatchingServlet(req, resp);
        if (servlet != null) {
            servlet.handleGet(req, resp);
        } else {
            super.handleGet(req, resp);
        }
    }

    @Override
    protected void handleHead(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        AbstractResourceServlet<?> servlet = findMatchingServlet(req, resp);
        if (servlet != null) {
            servlet.handleHead(req, resp);
        } else {
            super.handleHead(req, resp);
        }
    }

    @Override
    protected void handleOptions(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        AbstractResourceServlet<?> servlet = findMatchingServlet(req, resp);
        if (servlet != null) {
            servlet.handleOptions(req, resp);
        } else {
            super.handleOptions(req, resp);
        }
    }

    @Override
    protected void handlePost(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        AbstractResourceServlet<?> servlet = findMatchingServlet(req, resp);
        if (servlet != null) {
            servlet.handlePost(req, resp);
        } else {
            super.handlePost(req, resp);
        }
    }

    @Override
    protected void handlePut(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        AbstractResourceServlet<?> servlet = findMatchingServlet(req, resp);
        if (servlet != null) {
            servlet.handlePut(req, resp);
        } else {
            super.handlePut(req, resp);
        }
    }

    @Override
    protected void handleTrace(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        AbstractResourceServlet<?> servlet = findMatchingServlet(req, resp);
        if (servlet != null) {
            servlet.handleTrace(req, resp);
        } else {
            super.handleTrace(req, resp);
        }
    }

}
