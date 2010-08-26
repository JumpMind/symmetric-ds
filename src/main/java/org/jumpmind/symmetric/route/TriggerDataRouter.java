package org.jumpmind.symmetric.route;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;

/**
 * This is a router that is tied to the trigger table. It prevents triggers from
 * being routed to pre-2.0 versions of SymmetricDS.
 */
public class TriggerDataRouter extends AbstractDataRouter {

    public void completeBatch(IRouterContext context, OutgoingBatch batch) {
    }

    public Collection<String> routeToNodes(IRouterContext context, DataMetaData dataMetaData,
            Set<Node> nodes, boolean initialLoad) {
        Set<String> nodeIds = new HashSet<String>();
        if (!initialLoad) {
            for (Node node : nodes) {
                String version = node.getSymmetricVersion();
                if (version != null) {
                    int max = Version.parseVersion(version)[Version.MAJOR_INDEX];
                    if (max >= 2) {
                        nodeIds.add(node.getNodeId());
                    }
                }
            }
            return nodeIds;
        } else {
            return toNodeIds(nodes, nodeIds);
        }
    }

    public boolean isAutoRegister() {
        return false;
    }

}
