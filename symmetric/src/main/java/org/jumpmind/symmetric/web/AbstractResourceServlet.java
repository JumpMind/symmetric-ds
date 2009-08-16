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

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;

import org.jumpmind.symmetric.transport.ITransportResource;
import org.springframework.beans.BeanUtils;

/**
 * @since 1.4.0
 * 
 * @param <T>
 */
public abstract class AbstractResourceServlet extends AbstractServlet implements IServletResource, IServletExtension {

    private static final long serialVersionUID = 1L;
    private ServletResourceTemplate servletResourceTemplate = new ServletResourceTemplate();
    private int initOrder;

    /**
     * Returns true if this should be container compatible
     * 
     * @return
     */
    public boolean isContainerCompatible() {
        return false;
    }

    public int getInitOrder() {
        return this.initOrder;
    }

    public void setInitOrder(int initOrder) {
        this.initOrder = initOrder;
    }

    public Servlet getServlet() {
        return this;
    }

    public boolean isAutoRegister() {
        return true;
    }

    public void destroy() {
        servletResourceTemplate.destroy();
    }

    public boolean isDisabled() {
        return servletResourceTemplate.isDisabled();
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
                getLog().info("ServletInitializing", springBean.getClass().getSimpleName());
                BeanUtils.copyProperties(springBean, this, IServletResource.class);
                BeanUtils.copyProperties(springBean, this, ITransportResource.class);
                BeanUtils.copyProperties(springBean, this, this.getClass());
            }
        }
    }

    /**
     * Returns true if this is a spring managed resource.
     */
    protected boolean isSpringManaged() {
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
    @SuppressWarnings("unchecked")
    protected IServletResource getSpringBean() {
        IServletResource retVal = this;
        if (!isSpringManaged()) {
            Iterator iterator = getDefaultApplicationContext().getBeansOfType(this.getClass()).values().iterator();
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

    public void init(ServletContext servletContext) {
        servletResourceTemplate.init(servletContext);
    }
}
