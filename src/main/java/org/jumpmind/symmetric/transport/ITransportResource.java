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
package org.jumpmind.symmetric.transport;

/**
 * This marks a resource that is used by a transport. For instance for a
 * traditional application server, various HttpServlets might extends this. For
 * an alternative transport, a different technology would likely extend it.
 * 
 * 
 * @param <T>
 */
public interface ITransportResource<T extends ITransportResourceHandler> {

    public abstract void setTransportResourceHandler(T transportResourceHandler);

    public abstract T getTransportResourceHandler();

}