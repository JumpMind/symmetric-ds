package org.jumpmind.symmetric.route;

import java.util.Set;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;

/**
 * The data router is an extension point that allows the end user to target 
 * certain nodes with data changes.  SymmetricDS comes with a build-in data routers like
 * {@link SubSelectDataRouter} and {@link ColumnMatchDataRouter}.
 * <p>
 * In order to configure a data router you use the router_type and router_expression column on
 * the trigger table. The given Spring bean name of the {@link IDataRouter} is the router_type and 
 * each data router is configured using the routing_expression according to its implementation. 
 * 
 * @since 2.0
 * @see SubSelectDataRouter
 * @see ColumnMatchDataRouter
 *
 * 
 */
public interface IDataRouter extends IExtensionPoint {

    public Set<String> routeToNodes(SimpleRouterContext context, DataMetaData dataMetaData, Set<Node> nodes, boolean initialLoad);
    
    public void completeBatch(SimpleRouterContext context, OutgoingBatch batch);
    
    public void contextCommitted(SimpleRouterContext context);

}