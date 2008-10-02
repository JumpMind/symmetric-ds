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

import javax.servlet.ServletRequest;

/**
 * This interface can be used by a servlet or a filter that is managed by
 * Spring.
 * 
 * @since 1.4.0
 * 
 */
public interface IServletResource {

    public abstract void setDisabled(boolean disabled);

    public abstract void setUriPattern(String uriPattern);

    public abstract void setUriPatterns(String[] uriPatterns);

    public abstract void setRegexPattern(String regexPattern);

    public abstract void setRegexPatterns(String[] regexPatterns);

    public abstract boolean isDisabled();

    public abstract String[] getUriPatterns();

    public abstract String[] getRegexPatterns();

    public abstract void destroy();

    public abstract void refresh();

    /**
     * Returns true if the request path matches the uriPattern on this filter.
     * 
     * @param request
     * @return
     */
    public abstract boolean matches(ServletRequest request);

    /**
     * Returns true if this is a container managed resource.
     * 
     * @return
     */
    public abstract boolean isSpringManaged();

    /**
     * Returns the spring managed bean
     * 
     * @return
     */
    public abstract IServletResource getSpringBean();

    /**
     * Returns true if this should be container compatible
     * 
     * @return
     */
    public boolean isContainerCompatible();

}
