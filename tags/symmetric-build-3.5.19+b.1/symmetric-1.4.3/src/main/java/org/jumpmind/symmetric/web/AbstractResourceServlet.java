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

import java.util.Iterator;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;

import org.jumpmind.symmetric.transport.ITransportResource;
import org.jumpmind.symmetric.transport.ITransportResourceHandler;
import org.springframework.beans.BeanUtils;

/**
 * @since 1.4.0
 * 
 * @param <T>
 */
public abstract class AbstractResourceServlet<T extends ITransportResourceHandler> extends AbstractServlet implements
        IServletResource {
    private ServletResourceTemplate servletResourceTemplate = new ServletResourceTemplate();

    /**
     * Returns true if this should be container compatible
     * 
     * @return
     */
    public boolean isContainerCompatible() {
        return false;
    }

    public void destroy() {
        servletResourceTemplate.destroy();
    }

    public boolean isDisabled() {
        return servletResourceTemplate.isDisabled();
    }

    public String[] getRegexPatterns() {
        return servletResourceTemplate.getRegexPatterns();
    }

    public String[] getUriPatterns() {
        return servletResourceTemplate.getUriPatterns();
    }

    public boolean matches(ServletRequest request) {
        return servletResourceTemplate.matches(request);
    }

    public void setDisabled(boolean disabled) {
        servletResourceTemplate.setDisabled(disabled);
    }
    
    public void setEnabled(boolean enabled) {
        servletResourceTemplate.setDisabled(!enabled);
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

    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
        super.init(servletConfig);
        servletResourceTemplate.init(getServletContext());
        if (isContainerCompatible() && !this.isSpringManaged()) {
            final IServletResource springBean = getSpringBean();
            if (this != springBean) { // this != is deliberate!
                if (getLogger().isInfoEnabled()) {
                    getLogger().info(String.format("Initializing servlet %s", springBean.getClass().getSimpleName()));
                }
                BeanUtils.copyProperties(springBean, this, IServletResource.class);
                BeanUtils.copyProperties(springBean, this, ITransportResource.class);
                BeanUtils.copyProperties(springBean, this, this.getClass());

                this.refresh();
            }
        }
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

    public void init(ServletContext servletContext) {
        servletResourceTemplate.init(servletContext);
    }

    public void refresh() {
        servletResourceTemplate.refresh();
    }
}
