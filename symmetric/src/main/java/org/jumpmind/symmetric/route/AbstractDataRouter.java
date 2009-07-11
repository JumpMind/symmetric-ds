package org.jumpmind.symmetric.route;

import java.util.HashSet;
import java.util.Set;

import org.jumpmind.symmetric.model.Node;

public abstract class AbstractDataRouter implements IDataRouter {

    private boolean autoRegister;

    private boolean applyToInitialLoad = true;

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public void setApplyToInitialLoad(boolean applyToInitialLoad) {
        this.applyToInitialLoad = applyToInitialLoad;
    }

    public boolean isApplyToInitialLoad() {
        return applyToInitialLoad;
    }

    protected Set<String> toNodeIds(Set<Node> nodes) {
        Set<String> nodeIds = new HashSet<String>(nodes.size());
        for (Node node : nodes) {
            nodeIds.add(node.getNodeId());
        }
        return nodeIds;
    }
}
