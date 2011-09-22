/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */

package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.jumpmind.symmetric.service.INodeService;
import org.springframework.context.ApplicationContext;

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
 */
public class SymmetricFilter implements Filter {

    private static final ILog log = LogFactory.getLog(SymmetricFilter.class);

    private ServletContext servletContext;

    private List<Filter> filters;

    private INodeService nodeService;

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        new SymmetricFilterChain(chain, filters, nodeService.findIdentity(false), ServletUtils.normalizeRequestUri((HttpServletRequest)request)).doFilter(request,
                response);
    }

    public void init(FilterConfig filterConfig) throws ServletException {
        servletContext = filterConfig.getServletContext();
        filters = new ArrayList<Filter>();
        ApplicationContext ctx = ServletUtils.getApplicationContext(getServletContext());
        nodeService = (INodeService) ctx.getBean(Constants.NODE_SERVICE);
        Map<String, IServletFilterExtension> filterBeans = new LinkedHashMap<String, IServletFilterExtension>();
        filterBeans.putAll(ctx.getBeansOfType(IServletFilterExtension.class));
        if (ctx.getParent() != null) {
            filterBeans.putAll(ctx.getParent().getBeansOfType(IServletFilterExtension.class));
        }
        // they will need to be sorted somehow, right now its just the order
        // they appear in the spring file
        for (final Map.Entry<String, IServletFilterExtension> filterEntry : filterBeans.entrySet()) {
            final Filter filter = filterEntry.getValue();
            if (filter instanceof IExtensionPoint) {
                String filterKey = filterEntry.getKey();
                log.debug("FilterInitializing", filterKey);
                filter.init(filterConfig);
                filters.add(filter);
            } else {
                log.warn("FilterSkipping");
            }
        }
    }

    public void destroy() {
        for (final Filter filter : filters) {
            filter.destroy();
        }
    }

    public ServletContext getServletContext() {
        return servletContext;
    }

}