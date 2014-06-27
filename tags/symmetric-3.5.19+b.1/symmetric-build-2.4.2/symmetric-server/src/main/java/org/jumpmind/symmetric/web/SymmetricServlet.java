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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.DeploymentType;
import org.springframework.context.ApplicationContext;

/**
 * The SymmetricServlet manages all of the other Servlets. This allows for
 * easier configuration since Spring manages the individual Servlets.
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

    private List<IServletExtension> servlets;

    @Override
    public void init() throws ServletException {
        servlets = new ArrayList<IServletExtension>();
        ApplicationContext ctx = ServletUtils.getApplicationContext(getServletContext());
        DeploymentType deploymentType = ctx.getBean(DeploymentType.class);
        deploymentType.setServletRegistered(true);
        final Map<String, IServletExtension> servletBeans = new LinkedHashMap<String, IServletExtension>();
        servletBeans.putAll(ctx.getBeansOfType(IServletExtension.class));
        if (ctx.getParent() != null) {
            servletBeans.putAll(ctx.getParent().getBeansOfType(IServletExtension.class));
        }

        for (final Map.Entry<String, IServletExtension> servletEntry : servletBeans.entrySet()) {
            log.debug("ServletInitializing", servletEntry.getKey());
            final IServletExtension extension = servletEntry.getValue();
            extension.getServlet().init(getServletConfig());
            servlets.add(extension);
        }

        if (servlets.size() == 0) {
            log.error("ServletNoneFound");
        }
    }

    @Override
    public void destroy() {
        if (servlets != null) {
            for (final IServletExtension extension : servlets) {
                extension.getServlet().destroy();
            }
        }
    }

    protected Servlet findMatchingServlet(HttpServletRequest req) throws ServletException {
        Servlet retVal = null;
        for (Iterator<IServletExtension> iterator = servlets.iterator(); retVal == null
                && iterator.hasNext();) {
            IServletExtension extension = iterator.next();
            if (!extension.isDisabled() && matches(extension, req)) {
                retVal = extension.getServlet();
            }
        }
        return retVal;
    }

    @Override
    public void service(HttpServletRequest req, HttpServletResponse res) throws ServletException,
            IOException {
        Servlet servlet = findMatchingServlet(req);
        if (servlet != null) {
            try {
                servlet.service(req, res);
            } catch (Exception e) {
                logException(req, e,
                        !(e instanceof IOException && StringUtils.isNotBlank(e.getMessage())));
                if (!res.isCommitted()) {
                    res.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                }
            }
        } else {
            log.error("ServletNotFoundToHandleRequest", ServletUtils.normalizeRequestUri(req),
                    req.getRemoteHost(), req.getRemoteAddr(), req.getQueryString());
            res.sendRedirect("/");
        }
    }

}