package org.jumpmind.symmetric.route;

import java.util.List;
import java.util.Set;

import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.Trigger;

/**
 * We would provide several implementations of the data router and it can also
 * be an extension point. It would be configured by trigger. Some examples are:
 * ColumMatchDataRouter, DatabaseExpressionDataRouter,
 * RegistrationRedirectDataRouter, GroovyDataRouter
 * <p>
 * In order to configure a data router you use the route_expression column on
 * sym_trigger. You use the given Spring bean name of the {@link IDataRouter}
 * and configure it using a series of name/value pairs which will be set on the
 * bean.
 * <p>
 * For example, column:columName=store_id, would associate a
 * ColumnMatchDataRouter to the trigger and make the store_id column for the
 * table the driver column for routing.
 * 
 * @since 2.0
 */
public interface IDataRouter extends IExtensionPoint {

    Set<String> routeToNodes(Data data, Trigger trigger, List<Node> nodes, NodeChannel channel);

}
