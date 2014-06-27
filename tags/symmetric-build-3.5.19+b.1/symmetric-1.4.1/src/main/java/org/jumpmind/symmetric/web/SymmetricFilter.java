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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * This filter allows us simplify the configuration of symmetric by defining
 * filters directly within spring configuration files.
 * 
 * Configured within web.xml
 * 
 * <pre>
 *  &lt;filter&gt;
 *    &lt;filter-name&gt;SymmetricFilter&lt;/filter-name&gt;
 *    &lt;filter-class&gt;
 *      org.jumpmind.symmetric.web.SymmetricFilter
 *    &lt;/filter-class&gt;
 *  &lt;/filter&gt;
 * 
 *  &lt;filter-mapping&gt;
 *    &lt;filter-name&gt;SymmetricFilter&lt;/filter-name&gt;
 *    &lt;url-pattern&gt;*&lt;/url-pattern&gt;
 *  &lt;/filter-mapping&gt;
 * </pre>
 * 
 * @since 1.4.0
 * 
 */
public class SymmetricFilter implements Filter {

    private static final Log logger = LogFactory.getLog(SymmetricFilter.class);

    private ServletContext servletContext;

    private List<Filter> filters;

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException,
            ServletException {
        new SymmetricFilterChain(chain).doFilter(request, response);
    }

    @SuppressWarnings("unchecked")
    public void init(FilterConfig filterConfig) throws ServletException {
        servletContext = filterConfig.getServletContext();
        filters = new ArrayList<Filter>();
        ApplicationContext ctx = getContext();
        Map<String, Filter> filterBeans = ctx.getBeansOfType(Filter.class);
        if (filterBeans.size() == 0) {
            filterBeans = ctx.getParent().getBeansOfType(Filter.class);
        }
        // they will need to be sorted somehow, right now its just the order
        // they appear in the spring file
        for (final Map.Entry<String, Filter> filterEntry : filterBeans.entrySet()) {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("Initializing filter %s", filterEntry.getKey()));
            }
            final Filter filter = filterEntry.getValue();
            filter.init(filterConfig);
            filters.add(filter);
        }
    }

    public void destroy() {
        for (final Filter filter : filters) {
            filter.destroy();
        }

    }

    protected ApplicationContext getContext() {
        return WebApplicationContextUtils.getWebApplicationContext(getServletContext());
    }

    public ServletContext getServletContext() {
        return servletContext;
    }

    /**
     * The chain will visit each filter in turn. When done, it will pass along
     * to the original chain. The chain skips disabled filters. I'm wondering if
     * this should be moved to the {@link SymmetricFilter#init(FilterConfig)}.
     * 
     * @author Keith
     * 
     */
    private class SymmetricFilterChain implements FilterChain {

        private FilterChain chain;
        private int index;

        public SymmetricFilterChain(FilterChain chain) {
            this.chain = chain;
            index = 0;
        }

        public void doFilter(ServletRequest request, ServletResponse response) throws IOException, ServletException {
            if (!response.isCommitted()) {
                if (index < filters.size()) {
                    final Filter filter = filters.get(index++);
                    if (filter instanceof AbstractFilter) {
                        final AbstractFilter builtinFilter = (AbstractFilter) filter;
                        if (!builtinFilter.isDisabled() && builtinFilter.matches(request)) {
                            builtinFilter.doFilter(request, response, this);
                        } else {
                            this.doFilter(request, response);
                        }
                    } else {
                        filter.doFilter(request, response, this);
                    }
                } else {
                    chain.doFilter(request, response);
                }
            }
        }
    }

}
