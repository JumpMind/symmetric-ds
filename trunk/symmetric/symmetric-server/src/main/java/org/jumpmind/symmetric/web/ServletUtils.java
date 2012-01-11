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
 * under the License.  */
package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

/**
 * Utility methods for working with {@link Servlet}s
 */
public class ServletUtils {
    
    /**
     * Because you can't send an error when the response is already committed,
     * this helps to avoid unnecessary errors in the logs.
     * 
     * @param resp
     * @param statusCode
     * @return true if the error could be sent to the response
     * @throws IOException
     */
    public static boolean sendError(final HttpServletResponse resp, final int statusCode) throws IOException {
        return sendError(resp, statusCode, null);
    }

    /**
     * Because you can't send an error when the response is already committed,
     * this helps to avoid unnecessary errors in the logs.
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
            resp.sendError(statusCode, message);
            retVal = true;
        }
        return retVal;
    }

    /**
     * Because you can't send an error when the response is already committed,
     * this helps to avoid unnecessary errors in the logs.
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
     * Because you can't send an error when the response is already committed,
     * this helps to avoid unnecessary errors in the logs.
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
     * Returns the part of the path we are interested in when doing pattern
     * matching. This should work whether or not the servlet or filter is
     * explicitly mapped inside of the web.xml since it always strips off the
     * contextPath.
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
     * Returns the parameter with that name, trimmed to null. If the trimmed
     * string is null, defaults to the defaultValue.
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