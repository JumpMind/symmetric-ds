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

import org.jumpmind.symmetric.transport.ITransportResource;
import org.jumpmind.symmetric.transport.ITransportResourceHandler;

/**
 * This is a filter that is used by a transport.
 * 
 * 
 * @param <T>
 * @since 1.4.0
 * 
 */
public abstract class AbstractTransportFilter<T extends ITransportResourceHandler> extends AbstractFilter implements
        ITransportResource<T> {

    private T transportResourceHandler;

    public void setTransportResourceHandler(T transportResourceHandler) {
        this.transportResourceHandler = transportResourceHandler;
    }

    public T getTransportResourceHandler() {
        return transportResourceHandler;
    }

}
