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

import jakarta.servlet.Servlet;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.text.StringEscapeUtils;

/**
 * Utility methods for working with {@link Servlet}s
 */
public class ServletUtils {
    /**
     * Because you can't send an error when the response is already committed, this helps to avoid unnecessary errors in the logs.
     * 
     * @param resp
     * @param statusCode
     * @return true if the error could be sent to the response
     * @throws IOException
     */
    public static boolean sendError(final HttpServletResponse resp, final int statusCode) throws IOException {
        return sendError(resp, statusCode, null);
    }

    public static String whereAreYou(HttpServletRequest request) {
        String ip = request.getHeader("X-Forwarded-For");
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("HTTP_CLIENT_IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("HTTP_X_FORWARDED_FOR");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        return ip;
    }

    /**
     * Because you can't send an error when the response is already committed, this helps to avoid unnecessary errors in the logs.
     * 
     * @param resp
     * @param statusCode
     * @param message
     *            a message to put in the body of the response
     * @return true if the error could be sent to the response
     * @throws IOException
     */
    public static boolean sendError(final HttpServletResponse resp, final int statusCode, final String message)
            throws IOException {
        boolean retVal = false;
        if (!resp.isCommitted()) {
            resp.sendError(statusCode, StringEscapeUtils.escapeHtml4(message));
            retVal = true;
        }
        return retVal;
    }

    /**
     * Because you can't send an error when the response is already committed, this helps to avoid unnecessary errors in the logs.
     * 
     * @param resp
     * @param statusCode
     * @return true if the error could be sent to the response
     * @throws IOException
     */
    public static boolean sendError(final ServletResponse resp, final int statusCode) throws IOException {
        return sendError(resp, statusCode, null);
    }

    /**
     * Because you can't send an error when the response is already committed, this helps to avoid unnecessary errors in the logs.
     * 
     * @param resp
     * @param statusCode
     * @param message
     *            a message to put in the body of the response
     * @return true if the error could be sent to the response
     * @throws IOException
     */
    public static boolean sendError(final ServletResponse resp, final int statusCode, final String message)
            throws IOException {
        boolean retVal = false;
        if (resp instanceof HttpServletResponse) {
            retVal = sendError((HttpServletResponse) resp, statusCode, message);
        }
        return retVal;
    }

    /**
     * Returns the part of the path we are interested in when doing pattern matching. This should work whether or not the servlet or filter is explicitly mapped
     * inside of the web.xml since it always strips off the contextPath.
     * 
     * @param httpRequest
     * @return
     */
    public static String normalizeRequestUri(HttpServletRequest httpRequest) {
        String retVal = httpRequest.getRequestURI();
        String contextPath = httpRequest.getContextPath();
        if (retVal.startsWith(contextPath)) {
            retVal = retVal.substring(contextPath.length());
        }
        String servletPath = httpRequest.getServletPath();
        if (retVal.startsWith(servletPath)) {
            retVal = retVal.substring(servletPath.length());
        }
        return retVal;
    }

    public static ServerSymmetricEngine findEngine(HttpServletRequest req, ServletContext ctx) {
        String engineName = getEngineNameFromUrl((HttpServletRequest) req);
        ServerSymmetricEngine engine = null;
        SymmetricEngineHolder holder = ServletUtils.getSymmetricEngineHolder(ctx);
        if (holder != null) {
            if (engineName != null) {
                engine = holder.getEngines().get(engineName);
            }
            if (holder.getEngineCount() == 1 && engine == null && holder.getNumerOfEnginesStarting() <= 1 &&
                    holder.getEngines().size() == 1) {
                engine = holder.getEngines().values().iterator().next();
            }
        }
        return engine;
    }

    public static String getEngineNameFromUrl(HttpServletRequest req) {
        String engineName = null;
        String normalizedUri = ServletUtils.normalizeRequestUri(req);
        int startIndex = normalizedUri.startsWith("/") ? 1 : 0;
        int endIndex = normalizedUri.indexOf("/", startIndex);
        if (endIndex > 0) {
            engineName = normalizedUri.substring(startIndex, endIndex);
        }
        return engineName;
    }

    /**
     * Returns the parameter with that name, trimmed to null
     * 
     * @param request
     * @param name
     * @return
     */
    public static String getParameter(HttpServletRequest request, String name) {
        return StringUtils.trimToNull(request.getParameter(name));
    }

    /**
     * Returns the parameter with that name, trimmed to null. If the trimmed string is null, defaults to the defaultValue.
     * 
     * @param request
     * @param name
     * @return
     */
    public static String getParameter(HttpServletRequest request, String name, String defaultValue) {
        return StringUtils.defaultIfEmpty(StringUtils.trimToNull(request.getParameter(name)), defaultValue);
    }

    public static long getParameterAsNumber(HttpServletRequest request, String name) {
        return NumberUtils.toLong(StringUtils.trimToNull(request.getParameter(name)));
    }

    public static SymmetricEngineHolder getSymmetricEngineHolder(ServletContext ctx) {
        return (SymmetricEngineHolder) ctx.getAttribute(
                WebConstants.ATTR_ENGINE_HOLDER);
    }
}