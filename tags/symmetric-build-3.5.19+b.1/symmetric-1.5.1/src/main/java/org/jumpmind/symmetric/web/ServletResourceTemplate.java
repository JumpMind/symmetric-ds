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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

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
 */
public class ServletResourceTemplate implements IServletResource {
    protected ServletContext servletContext;

    private boolean disabled;
    protected String[] uriPatterns;
    private String[] regexPatterns;
    protected Pattern[] compiledRegexPatterns;
    protected IParameterService parameterService;

    public void init(ServletContext servletContext) {
        this.servletContext = servletContext;
        compileRegexPatterns();
    }

    public void refresh() {
        compileRegexPatterns();
    }

    protected void compileRegexPatterns() {
        final List<Pattern> compiledRegexPatterns;
        if (!ArrayUtils.isEmpty(regexPatterns)) {
            compiledRegexPatterns = new ArrayList<Pattern>(regexPatterns.length);
            for (String regexPattern : regexPatterns) {
                compiledRegexPatterns.add(Pattern.compile(regexPattern));
            }
        } else {
            compiledRegexPatterns = Collections.emptyList();
        }
        this.compiledRegexPatterns = compiledRegexPatterns.toArray(new Pattern[compiledRegexPatterns.size()]);
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

    public void setRegexPattern(String regexPattern) {
        this.regexPatterns = new String[] { regexPattern };
    }

    public void setRegexPatterns(String[] regexPatterns) {
        this.regexPatterns = regexPatterns;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public String[] getUriPatterns() {
        return uriPatterns;
    }

    public String[] getRegexPatterns() {
        return regexPatterns;
    }

    protected boolean matchesRegexPatterns(String uri) {
        boolean retVal = false;
        for (int i = 0; !retVal && i < compiledRegexPatterns.length; i++) {
            retVal = matchesRegexPattern(uri, compiledRegexPatterns[i]);
        }
        return retVal;
    }

    protected boolean matchesRegexPattern(String uri, Pattern compiledRegexPattern) {
        return compiledRegexPattern.matcher(uri).matches();
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
            } else if (!ArrayUtils.isEmpty(compiledRegexPatterns)) {
                retVal = matchesRegexPatterns(uri);
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
     *                a message to put in the body of the response
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
     * 
     * @return
     */
    public boolean isSpringManaged() {
        return getDefaultApplicationContext().getBeansOfType(this.getClass()).values().contains(this);
    }

    /**
     * Returns true if this is a container managed resource.
     * 
     * @return
     */
    @SuppressWarnings("unchecked")
    public IServletResource getSpringBean() {
        IServletResource retVal = this;
        if (!isSpringManaged()) {
            Iterator iterator = getDefaultApplicationContext().getBeansOfType(this.getClass()).values().iterator();
            if (iterator.hasNext()) {
                retVal = (IServletResource) iterator.next();
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