/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
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
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

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

    protected final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
        ServerSymmetricEngine engine = findEngine(req);
        if (engine == null) {
            boolean nodesBeingCreated = ServletUtils.getSymmetricEngineHolder(getServletContext())
                    .areEnginesStarting();
            if (nodesBeingCreated) {
                log.info(
                        "The client node request is being rejected because the server node does not exist yet.  There are nodes being initialized.  It might be that the node is not ready or that the database is unavailable.  Please be patient.  The request was {} from the host {} with an ip address of {}.  The query string was: {}",
                        new Object[] { ServletUtils.normalizeRequestUri(req), req.getRemoteHost(),
                                req.getRemoteAddr(), req.getQueryString() });
            } else {
                log.error(
                        "The client node request is being rejected because the server node does not exist.  Please check that the engine.name exists in the url.  It should be of the pattern http://host:port/sync/{engine.name}/{action}.  If it does not, then check the sync.url of this node or the registration.url of the client making the request.  The request was {} from the host {} with an ip address of {}.  The query string was: {}",
                        new Object[] { ServletUtils.normalizeRequestUri(req), req.getRemoteHost(),
                                req.getRemoteAddr(), req.getQueryString() });
            }
            ServletUtils.sendError(res, HttpServletResponse.SC_SERVICE_UNAVAILABLE);

        } else if (engine.isStarted()) {
            IUriHandler handler = findMatchingHandler(engine, req);
            if (handler != null) {
                MDC.put("engineName", engine.getEngineName());
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
                log.error(
                        "The request path of the url is not supported.  The request was {} from the host {} with an ip address of {}.  The query string was: {}",
                        new Object[] { ServletUtils.normalizeRequestUri(req), req.getRemoteHost(),
                                req.getRemoteAddr(), req.getQueryString() });
                ServletUtils.sendError(res, HttpServletResponse.SC_BAD_REQUEST);
            }
        } else if (engine.isStarting()) {
            log.info(
                    "The client node request is being rejected because the server node is currently starting.  Please be patient.  The request was {} from the host {} with an ip address of {} will not be processed.  The query string was: {}",
                    new Object[] { ServletUtils.normalizeRequestUri(req), req.getRemoteHost(),
                            req.getRemoteAddr(), req.getQueryString() });
            ServletUtils.sendError(res, HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        } else if (!engine.isStarted() && !engine.isConfigured()) {
            log.warn(
                    "The client node request is being rejected because the server node was not started because it is not configured properly. The request {} from the host {} with an ip address of {} will not be processed.  The query string was: {}",
                    new Object[] { ServletUtils.normalizeRequestUri(req), req.getRemoteHost(),
                            req.getRemoteAddr(), req.getQueryString() });
            ServletUtils.sendError(res, HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }

    }

    protected ServerSymmetricEngine findEngine(HttpServletRequest req) {
        String engineName = getEngineNameFromUrl((HttpServletRequest) req);
        ServerSymmetricEngine engine = null;
        SymmetricEngineHolder holder = ServletUtils.getSymmetricEngineHolder(getServletContext());
        if (holder != null) {
            if (engineName != null) {
                engine = holder.getEngines().get(engineName);
            }

            if (holder.getEngineCount() == 1 && engine == null && holder.getNumerOfEnginesStarting() <= 1 && 
                    holder.getEngines().size() == 1) {
                engine = holder.getEngines().values().iterator().next();
            }
        }
        return engine != null ? engine : null;
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

    protected Collection<IUriHandler> getUriHandlersFrom(ServerSymmetricEngine engine) {
        if (engine != null) {
            return engine.getUriHandlers();
        } else {
            return null;
        }
    }

    protected IUriHandler findMatchingHandler(ServerSymmetricEngine engine, HttpServletRequest req)
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

    protected void logException(HttpServletRequest req, Exception ex, boolean isError) {
        String nodeId = req.getParameter(WebConstants.NODE_ID);
        String externalId = req.getParameter(WebConstants.EXTERNAL_ID);
        String address = req.getRemoteAddr();
        String hostName = req.getRemoteHost();
        String method = req instanceof HttpServletRequest ? ((HttpServletRequest) req).getMethod()
                : "";
        if (isError) {
            log.error(
                    "Error while processing {} request for externalId: {}, node: {} at {} ({}) with path: {}",
                    new Object[] { method, externalId, nodeId, address, hostName,
                            ServletUtils.normalizeRequestUri(req) });
            log.error(ex.getMessage(), ex);
        } else {
            log.warn(
                    "Error while processing {} request for externalId: {}, node: {} at {} ({}).  The message is: {}",
                    new Object[] { method, externalId, nodeId, address, hostName, ex.getMessage() });
        }
    }

}
