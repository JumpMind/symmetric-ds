package org.jumpmind.symmetric.route;

import java.util.Collection;
import java.util.Set;

import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;

public class DefaultDataRouter extends AbstractDataRouter {

    public Collection<String> routeToNodes(DataMetaData dataMetaData, Set<Node> nodes, boolean initialLoad) {
        return toNodeIds(nodes);
    }

}
