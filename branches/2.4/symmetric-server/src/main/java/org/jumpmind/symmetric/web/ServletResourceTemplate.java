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
import java.util.Iterator;

import javax.servlet.ServletContext;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * All symmetric servlets and filters (other than {@link SymmetricFilter} and
 * {@link SymmetricServlet}) should extend this class. It it managed by Spring.
 *
 * 
 */
public class ServletResourceTemplate implements IServletResource {
    protected ServletContext servletContext;

    private boolean disabled;
    protected String[] uriPatterns;
    protected IParameterService parameterService;

    public void init(ServletContext servletContext) {
        this.servletContext = servletContext;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    public void setUriPattern(String uriPattern) {
        this.uriPatterns = new String[] { uriPattern };
    }

    public void setUriPatterns(String[] uriPatterns) {
        this.uriPatterns = uriPatterns;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public String[] getUriPatterns() {
        return uriPatterns;
    }

    protected boolean matchesUriPatterns(String uri) {
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

    protected ServletContext getServletContext() {
        return servletContext;
    }

    public void destroy() {

    }

    public boolean matches(ServletRequest request) {
        boolean retVal = true;
        if (request instanceof HttpServletRequest) {
            final HttpServletRequest httpRequest = (HttpServletRequest) request;
            final String uri = normalizeRequestUri(httpRequest);
            if (!ArrayUtils.isEmpty(uriPatterns)) {
                retVal = matchesUriPatterns(uri);
            } 
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

    /**
     * Because you can't send an error when the response is already committed,
     * this helps to avoid unnecessary errors in the logs.
     * 
     * @param resp
     * @param statusCode
     * @throws IOException
     */
    protected boolean sendError(ServletResponse resp, int statusCode) throws IOException {
        return ServletUtils.sendError(resp, statusCode);
    }

    /**
     * Because you can't send an error when the response is already committed,
     * this helps to avoid unnecessary errors in the logs.
     * 
     * @param resp
     * @param statusCode
     * @param message
     *            a message to put in the body of the response
     * @throws IOException
     */
    protected boolean sendError(ServletResponse resp, int statusCode, String message) throws IOException {
        return ServletUtils.sendError(resp, statusCode, message);
    }

    protected ApplicationContext getDefaultApplicationContext() {
        return WebApplicationContextUtils.getWebApplicationContext(getServletContext());
    }

    /**
     * Returns true if this is a spring managed resource.
     */
    public boolean isSpringManaged() {
        boolean managed = getDefaultApplicationContext().getBeansOfType(this.getClass()).values().contains(this);
        if (!managed && getDefaultApplicationContext().getParent() != null) {
            managed = getDefaultApplicationContext().getParent().getBeansOfType(this.getClass()).values()
                    .contains(this);
        }
        return managed;
    }

    /**
     * Returns true if this is a container managed resource.
     */
    public IServletResource getSpringBean() {
        IServletResource retVal = this;
        if (!isSpringManaged()) {
            Iterator<?> iterator = getDefaultApplicationContext().getBeansOfType(this.getClass()).values().iterator();
            if (iterator.hasNext()) {
                retVal = (IServletResource) iterator.next();
            }

            if (retVal == null && getDefaultApplicationContext().getParent() != null) {
                iterator = getDefaultApplicationContext().getParent().getBeansOfType(this.getClass()).values()
                        .iterator();
                if (iterator.hasNext()) {
                    retVal = (IServletResource) iterator.next();
                }
            }
        }
        return retVal;
    }

    /**
     * Returns true if this should be container compatible
     * 
     * @return
     */
    public boolean isContainerCompatible() {
        return false;
    }

    protected void setServletContext(ServletContext servletContext) {
        this.servletContext = servletContext;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }
}