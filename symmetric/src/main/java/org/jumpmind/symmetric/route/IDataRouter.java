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
package org.jumpmind.symmetric.route;

import java.util.Collection;
import java.util.Set;

import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;

/**
 * The data router is an extension point that allows the end user to target 
 * certain nodes with data changes.  SymmetricDS comes with a build-in data routers like
 * {@link SubSelectDataRouter} and {@link ColumnMatchDataRouter}.
 * <p>
 * In order to configure a data router you use the router_name and routing_expression column on
 * sym_trigger. The given Spring bean name of the {@link IDataRouter} is the router_name and 
 * each data router is configured using the routing_expression according to its implementation. 
 * 
 * @since 2.0
 */
public interface IDataRouter extends IExtensionPoint {

    Collection<String> routeToNodes(DataMetaData dataMetaData, Set<Node> nodes, boolean initialLoad);

}
