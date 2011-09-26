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
import java.util.Collection;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;

/**
 * This servlet handles web requests to SymmetricDS.
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
public class SymmetricServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    protected final ILog log = LogFactory.getLog(getClass());

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
        String method = req.getMethod();
        ISymmetricEngine engine = findEngine(req);
        IUriHandler handler = findMatchingHandler(engine, req);
        if (handler != null) {
            List<IInterceptor> interceptors = handler.getInterceptors();
            try {
                if (interceptors != null) {
                    for (IInterceptor interceptor : interceptors) {
                        if (!interceptor.before(req, res)) {
                            return;
                        }
                    }
                }
                handler.handle(req, res);
            } catch (Exception e) {
                logException(req, e,
                        !(e instanceof IOException && StringUtils.isNotBlank(e.getMessage())));
                if (!res.isCommitted()) {
                    ServletUtils.sendError(res, HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                }
            } finally {
                if (interceptors != null) {
                    for (IInterceptor interceptor : interceptors) {
                        interceptor.after(req, res);
                    }
                }
            }
        } else {
            log.error("HandlerNotFound", ServletUtils.normalizeRequestUri(req),
                    req.getRemoteHost(), req.getRemoteAddr(), req.getQueryString());            
            if (method.equals(WebConstants.METHOD_GET)) {
                res.sendRedirect("/");
            } else {
                ServletUtils.sendError(res, HttpServletResponse.SC_FORBIDDEN);                
            }
        }
    }

    protected ISymmetricEngine findEngine(HttpServletRequest req) {
        String engineName = getEngineNameFromUrl((HttpServletRequest) req);
        ISymmetricEngine engine = null;
        SymmetricEngineHolder holder = ServletUtils.getSymmetricEngineHolder(getServletContext());
        if (holder != null) {
            if (engineName != null) {
                engine = holder.getEngines().get(engineName);
            }

            if (engine == null && holder.getEngines().size() > 0) {
                engine = holder.getEngines().values().iterator().next();
            }
        }
        return engine;
    }

    protected static String getEngineNameFromUrl(HttpServletRequest req) {
        String engineName = null;
        String normalizedUri = ServletUtils.normalizeRequestUri(req);
        int startIndex = normalizedUri.startsWith("/") ? 1 : 0;
        int endIndex = normalizedUri.indexOf("/", startIndex);
        if (endIndex > 0) {
            engineName = normalizedUri.substring(startIndex, endIndex);
        }
        return engineName;
    }

    protected Collection<IUriHandler> getUriHandlersFrom(ISymmetricEngine engine) {
        if (engine != null) {
            return engine.getApplicationContext().getBeansOfType(IUriHandler.class).values();
        } else {
            return null;
        }
    }

    protected IUriHandler findMatchingHandler(ISymmetricEngine engine, HttpServletRequest req)
            throws ServletException {
        Collection<IUriHandler> handlers = getUriHandlersFrom(engine);
        if (handlers != null) {
            for (IUriHandler handler : handlers) {
                if (matchesUriPattern(normalizeUri(engine, req), handler.getUriPattern())
                        && handler.isEnabled()) {
                    return handler;
                }
            }
        }
        return null;
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

    protected String normalizeUri(ISymmetricEngine engine, HttpServletRequest req) {
        String uri = ServletUtils.normalizeRequestUri((HttpServletRequest) req);
        if (engine != null) {
            String removeString = "/" + engine.getEngineName();
            if (uri.startsWith(removeString)) {
                uri = uri.substring(removeString.length());
            }
        }
        return uri;
    }

    protected void logException(ServletRequest req, Exception ex, boolean isError) {
        String nodeId = req.getParameter(WebConstants.NODE_ID);
        String externalId = req.getParameter(WebConstants.EXTERNAL_ID);
        String address = req.getRemoteAddr();
        String hostName = req.getRemoteHost();
        String method = req instanceof HttpServletRequest ? ((HttpServletRequest) req).getMethod()
                : "";
        if (log.isErrorEnabled() && isError) {
            log.error("ServletProcessingFailedError", ex, method, externalId, nodeId, address,
                    hostName);
        } else if (log.isWarnEnabled()) {
            log.warn("ServletProcessingFailedWarning", method, externalId, nodeId, address,
                    hostName, ex.getMessage());
        }
    }

}