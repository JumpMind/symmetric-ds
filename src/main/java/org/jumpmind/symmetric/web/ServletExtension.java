/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
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

import javax.servlet.Servlet;

/**
 * This is an extension that an be used to register a Servlet that gets called by
 * SymmetricDS's {@link SymmetricServlet} 
 */
public class ServletExtension implements IServletExtension {

    boolean autoRegister = true;

    Servlet servlet;

    String[] uriPatterns;

    int initOrder = 0;

    boolean disabled = false;

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public void setInitOrder(int initOrder) {
        this.initOrder = initOrder;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public void setServlet(Servlet servlet) {
        this.servlet = servlet;
    }

    public void setUriPatterns(String[] uriPatterns) {
        this.uriPatterns = uriPatterns;
    }
    
    public void setUriPattern(String uriPattern) {
        this.uriPatterns = new String[] { uriPattern };
    }

    public Servlet getServlet() {
        return this.servlet;
    }

    public int getInitOrder() {
        return this.initOrder;
    }

    public String[] getUriPatterns() {
        return this.uriPatterns;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    public boolean isDisabled() {
        return this.disabled;
    }
}
