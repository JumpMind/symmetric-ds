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

import javax.servlet.ServletContext;
import javax.servlet.ServletRequest;

import org.jumpmind.symmetric.transport.ITransportResourceHandler;

public abstract class AbstractResourceServlet<T extends ITransportResourceHandler>
        extends AbstractServlet implements IServletResource {
    private ServletResourceTemplate servletResourceTemplate = new ServletResourceTemplate();

    public void destroy() {
        servletResourceTemplate.destroy();
    }

    public boolean isDisabled() {
        return servletResourceTemplate.isDisabled();
    }

    public boolean matches(ServletRequest request) {
        return servletResourceTemplate.matches(request);
    }

    public void setDisabled(boolean disabled) {
        servletResourceTemplate.setDisabled(disabled);
    }

    public void setRegexPattern(String regexPattern) {
        servletResourceTemplate.setRegexPattern(regexPattern);
    }

    public void setRegexPatterns(String[] regexPatterns) {
        servletResourceTemplate.setRegexPatterns(regexPatterns);
    }

    public void setUriPattern(String uriPattern) {
        servletResourceTemplate.setUriPattern(uriPattern);
    }

    public void setUriPatterns(String[] uriPatterns) {
        servletResourceTemplate.setUriPatterns(uriPatterns);
    }

    public String toString() {
        return servletResourceTemplate.toString();
    }

    public void init(ServletContext servletContext) {
        servletResourceTemplate.init(servletContext);
    }

}
