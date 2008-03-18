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
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import javax.servlet.Filter;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * All symmetric filters (other than {@link SymmetricFilter}) should extend
 * this class. It it managed by Spring.
 */
public abstract class AbstractFilter implements Filter {

    private ServletContext servletContext;

    private boolean disabled;

    private String[] uriPatterns;

    private String[] regexPatterns;

    private Pattern[] compiledRegexPatterns;

    public void init(FilterConfig filterConfig) throws ServletException {
        servletContext = filterConfig.getServletContext();
        compiledRegexPatterns = compileRegexPatterns();
    }

    private Pattern[] compileRegexPatterns() {
        final List<Pattern> compiledRegexPatterns;
        if (!ArrayUtils.isEmpty(regexPatterns)) {
            compiledRegexPatterns = new ArrayList<Pattern>(regexPatterns.length);
            for (String regexPattern : regexPatterns) {
                compiledRegexPatterns.add(Pattern.compile(regexPattern));
            }
        } else {
            compiledRegexPatterns = Collections.emptyList();
        }
        return compiledRegexPatterns.toArray(new Pattern[compiledRegexPatterns
                .size()]);
    }

    protected ServletContext getServletContext() {
        return servletContext;
    }

    public void destroy() {

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

    /**
     * Returns true if the request path matches the uriPattern on this filter.
     * 
     * @param request
     * @return
     */
    public boolean matches(ServletRequest request) {
        boolean retVal = true;
        if (request instanceof HttpServletRequest) {
            final HttpServletRequest httpRequest = (HttpServletRequest) request;
            if (!ArrayUtils.isEmpty(uriPatterns)) {
                retVal = matchesUriPatterns(httpRequest.getServletPath());
            } else if (!ArrayUtils.isEmpty(compiledRegexPatterns)) {
                retVal = matchesRegexPatterns(httpRequest.getServletPath());
            }
        }
        return retVal;
    }

    private boolean matchesRegexPatterns(String servletPath) {
        boolean retVal = false;
        for (int i = 0; !retVal && i < compiledRegexPatterns.length; i++) {
            retVal = matchesRegexPattern(servletPath, compiledRegexPatterns[i]);
        }
        return retVal;
    }

    private boolean matchesRegexPattern(String servletPath,
            Pattern compiledRegexPattern) {
        return compiledRegexPattern.matcher(servletPath).matches();
    }

    private boolean matchesUriPatterns(String servletPath) {
        boolean retVal = false;
        for (int i = 0; !retVal && i < uriPatterns.length; i++) {
            retVal = matchesUriPattern(servletPath, uriPatterns[i]);
        }
        return retVal;
    }

    private boolean matchesUriPattern(String servletPath, String uriPattern) {
        boolean retVal = false;
        String path = StringUtils.defaultIfEmpty(servletPath, "/");
        final String pattern = StringUtils.defaultIfEmpty(uriPattern, "/");
        if ("/".equals(pattern) || "/*".equals(pattern) || pattern.equals(path)) {
            retVal = true;
        } else {
            final String[] patternParts = StringUtils.split(pattern, "/");
            final String[] pathParts = StringUtils.split(path, "/");
            for (int i = 0; i < patternParts.length && i < pathParts.length
                    && retVal; i++) {
                final String patternPart = patternParts[i];
                retVal = "*".equals(patternPart)
                        || patternPart.equals(pathParts[i]);
            }
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
    protected boolean sendError(ServletResponse resp, int statusCode)
            throws IOException {
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
    protected boolean sendError(ServletResponse resp, int statusCode,
            String message) throws IOException {
        return ServletUtils.sendError(resp, statusCode, message);
    }

    protected ApplicationContext getApplicationContext() {
        return WebApplicationContextUtils
                .getWebApplicationContext(getServletContext());
    }

}
