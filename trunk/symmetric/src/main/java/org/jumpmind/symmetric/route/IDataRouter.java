package org.jumpmind.symmetric.route;

import java.util.Set;

import org.jumpmind.symmetric.model.Data;

/**
 * We would provide several implementations of the data router and it can also be an extension point.  
 * It would be configured by trigger. Some examples are: ColumRouter, DatabaseExpressionRouter, RedirectRouter
 * @since 2.0
 *
 */
public interface IDataRouter {

    Set<String> routeToNodes(Data data);
    
}
