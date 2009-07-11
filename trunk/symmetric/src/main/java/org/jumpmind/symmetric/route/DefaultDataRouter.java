package org.jumpmind.symmetric.route;

import java.util.Set;

import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.Trigger;

public class DefaultDataRouter extends AbstractDataRouter {

    public Set<String> routeToNodes(Data data, Trigger trigger, Set<Node> nodes, NodeChannel channel,
            boolean initialLoad) {
        return toNodeIds(nodes);
    }

}
