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
