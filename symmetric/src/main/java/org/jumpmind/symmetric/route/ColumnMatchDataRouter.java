package org.jumpmind.symmetric.route;

import java.util.Set;

import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.Trigger;

public class ColumnMatchDataRouter extends AbstractDataRouter implements IDataRouter {

    public Set<String> routeToNodes(Data data, Trigger trigger, Set<org.jumpmind.symmetric.model.Node> nodes,
            NodeChannel channel, boolean initialLoad) {

        
        return null;
    }

}
