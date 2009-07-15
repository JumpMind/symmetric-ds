package org.jumpmind.symmetric.route;

import java.util.Collection;
import java.util.Set;

import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;

public class RegistrationRedirectDataRouter extends AbstractDataRouter {

    public Collection<String> routeToNodes(DataMetaData dataMetaData, Set<Node> nodes, boolean initialLoad) {
        // TODO Cache registration redirect map per run and look at specified
        // column to determine which node ids to route to
        return null;
    }

}
