package org.jumpmind.symmetric.route;

import java.util.Set;

import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;

/**
 * This data router will route data to all of the nodes that are passed to it.
 */
public class DefaultDataRouter extends AbstractDataRouter {

    public Set<String> routeToNodes(SimpleRouterContext routingContext, DataMetaData dataMetaData, Set<Node> nodes,
            boolean initialLoad) {
        return toNodeIds(nodes, null);
    }

    public void completeBatch(SimpleRouterContext context, OutgoingBatch batch) {

    }

}