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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
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
    private static final int MAX_NETWORK_ERROR_FOR_LOGGING = 5;
    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected Map<String, Integer> rejectionStatusByEngine = new HashMap<String, Integer>();

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
        ServerSymmetricEngine engine = findEngine(req);
        MDC.put("engineName", engine != null ? engine.getEngineName() : "?");
        if (engine == null) {
            boolean nodesBeingCreated = ServletUtils.getSymmetricEngineHolder(getServletContext())
                    .areEnginesStarting();
            if (nodesBeingCreated) {
                if (shouldLog(getEngineNameFromUrl(req), 1)) {
                    log.info("Requests for engine " + getEngineNameFromUrl(req) + " are being rejected because nodes are still initializing");
                }
                log.debug(
                        "The client node request is being rejected because the server node does not exist yet.  There are nodes being initialized.  It might be that the node is not ready or that the database is unavailable.  Please be patient.  The request was {} from the host {} with an ip address of {}.  The query string was: {}",
                        new Object[] { ServletUtils.normalizeRequestUri(req), req.getRemoteHost(),
                                req.getRemoteAddr(), req.getQueryString() });
            } else {
                log.warn(
                        "The client node request is being rejected because the server node does not exist.  Please check that the engine.name exists in the url.  It should be of the pattern http://host:port/sync/{engine.name}/{action}.  If it does not, then check the sync.url of this node or the registration.url of the client making the request.  The request was {} from the host {} with an ip address of {}.  The query string was: {}",
                        new Object[] { ServletUtils.normalizeRequestUri(req), req.getRemoteHost(),
                                req.getRemoteAddr(), req.getQueryString() });
            }
            ServletUtils.sendError(res, WebConstants.SC_SERVICE_UNAVAILABLE);
        } else if (engine.isStarted()) {
            IUriHandler handler = findMatchingHandler(engine, req);
            if (handler != null) {
                List<IInterceptor> beforeInterceptors = handler.getInterceptors();
                List<IInterceptor> afterInterceptors = null;
                try {
                    String nodeId = req.getParameter(WebConstants.NODE_ID);
                    if (beforeInterceptors != null) {
                        afterInterceptors = new ArrayList<IInterceptor>(beforeInterceptors.size());
                        for (IInterceptor interceptor : beforeInterceptors) {
                            if (interceptor.before(req, res)) {
                                afterInterceptors.add(interceptor);
                            } else {
                                return;
                            }
                        }
                    }
                    handler.handle(req, res);
                    engine.resetErrorCountForNode(nodeId);
                } catch (Exception e) {
                    logException(req, engine, e);
                    if (!res.isCommitted()) {
                        ServletUtils.sendError(res, WebConstants.SC_INTERNAL_ERROR, "Internal error occurred, see log file");
                    }
                } finally {
                    if (afterInterceptors != null) {
                        for (IInterceptor interceptor : afterInterceptors) {
                            interceptor.after(req, res);
                        }
                    }
                }
            } else {
                log.warn(
                        "The request path of the url could not be handled. Check the engine.name of the target node vs. the sync URL of the source node. The request was {} from the host {} with an ip address of {}.  The query string was: {}",
                        new Object[] { ServletUtils.normalizeRequestUri(req), req.getRemoteHost(),
                                req.getRemoteAddr(), req.getQueryString() });
                ServletUtils.sendError(res, WebConstants.SC_NO_ENGINE, "No engine here with that name");
            }
        } else if (engine.isStarting()) {
            if (shouldLog(engine.getEngineName(), 2)) {
                log.info("Requests for engine " + engine.getEngineName() + " are being rejected while it is starting");
            }
            log.debug(
                    "The client node request is being rejected because the server node is currently starting.  Please be patient.  The request was {} from the host {} with an ip address of {} will not be processed.  The query string was: {}",
                    new Object[] { ServletUtils.normalizeRequestUri(req), req.getRemoteHost(),
                            req.getRemoteAddr(), req.getQueryString() });
            ServletUtils.sendError(res, WebConstants.SC_SERVICE_UNAVAILABLE);
        } else if (!engine.isStarted() && !engine.isConfigured()) {
            if (shouldLog(engine.getEngineName(), 3)) {
                log.info("Requests for engine " + engine.getEngineName() + " are being rejected because it is not configured properly");
            }
            log.debug(
                    "The client node request is being rejected because the server node was not started because it is not configured properly. The request {} from the host {} with an ip address of {} will not be processed.  The query string was: {}",
                    new Object[] { ServletUtils.normalizeRequestUri(req), req.getRemoteHost(),
                            req.getRemoteAddr(), req.getQueryString() });
            ServletUtils.sendError(res, WebConstants.SC_SERVICE_UNAVAILABLE);
        } else {
            if (shouldLog(engine.getEngineName(), 4)) {
                log.info("Requests for engine " + engine.getEngineName() + " are being rejected because it is not started");
            }
            log.debug(
                    "The client node request is being rejected because the server node is not started. The request {} from the host {} with an ip address of {} will not be processed.  The query string was: {}",
                    new Object[] { ServletUtils.normalizeRequestUri(req), req.getRemoteHost(),
                            req.getRemoteAddr(), req.getQueryString() });
            ServletUtils.sendError(res, WebConstants.SC_SERVICE_UNAVAILABLE);
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

    protected void logException(HttpServletRequest req, ServerSymmetricEngine engine, Exception ex) {
        String nodeId = req.getParameter(WebConstants.NODE_ID);
        String externalId = req.getParameter(WebConstants.EXTERNAL_ID);
        String address = req.getRemoteAddr();
        String hostName = req.getRemoteHost();
        String method = req instanceof HttpServletRequest ? ((HttpServletRequest) req).getMethod()
                : "";
        Throwable root = ExceptionUtils.getRootCause(ex);
        int errorCount = engine.getErrorCountFor(nodeId);
        String msg = String.format("Error while processing %s request for node: %s", method, nodeId);
        if (!StringUtils.isEmpty(externalId) && !StringUtils.equals(nodeId, externalId)) {
            msg += String.format(" externalId: %s", externalId);
        }
        if (!StringUtils.isEmpty(hostName) && !StringUtils.isEmpty(address)) {
            if (StringUtils.equals(hostName, address)) {
                msg += String.format(" at %s", hostName);
            } else {
                msg += String.format(" at %s (%s)", address, hostName);
            }
        } else if (!StringUtils.isEmpty(hostName)) {
            msg += String.format(" at %s", hostName);
        } else if (!StringUtils.isEmpty(address)) {
            msg += String.format(" at %s", address);
        }
        msg += String.format(" with path: %s", ServletUtils.normalizeRequestUri(req));
        if (!(ex instanceof IOException || root instanceof IOException) || errorCount >= MAX_NETWORK_ERROR_FOR_LOGGING) {
            log.error(msg, ex);
        } else {
            if (log.isDebugEnabled()) {
                log.info(msg, ex);
            } else {
                log.info(msg + " The message is: " + ex.getMessage());
            }
        }
        engine.incrementErrorCountForNode(nodeId);
    }

    protected boolean shouldLog(String engineName, int status) {
        Integer lastStatus = rejectionStatusByEngine.get(engineName);
        if (lastStatus == null || lastStatus.intValue() < status) {
            rejectionStatusByEngine.put(engineName, status);
            return true;
        }
        return false;
    }
}
