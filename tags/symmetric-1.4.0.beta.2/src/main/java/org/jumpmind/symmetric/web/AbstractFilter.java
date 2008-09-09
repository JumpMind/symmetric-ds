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

import javax.servlet.Filter;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;

import org.apache.commons.logging.Log;
import org.jumpmind.symmetric.transport.ITransportResource;
import org.springframework.beans.BeanUtils;

/**
 * All symmetric filters (other than {@link SymmetricFilter}) should extend
 * this class. It is managed by Spring.
 * 
 * @since 1.4.0
 */
public abstract class AbstractFilter extends ServletResourceTemplate implements Filter {

    protected abstract Log getLogger();

    public void init(FilterConfig filterConfig) throws ServletException {
        init(filterConfig.getServletContext());
        if (isContainerCompatible() && !this.isSpringManaged()) {
            final IServletResource springBean = getSpringBean();
            if (this != springBean) { // this != is deliberate!
                if (getLogger().isInfoEnabled()) {
                    getLogger().info(String.format("Initializing filter %s", springBean.getClass().getSimpleName()));
                }
                BeanUtils.copyProperties(springBean, this, IServletResource.class);
                BeanUtils.copyProperties(springBean, this, ITransportResource.class);
                BeanUtils.copyProperties(springBean, this, this.getClass());
                this.refresh();
            }
        }
    }
}
