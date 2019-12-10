package org.jumpmind.symmetric.web.rest.model;

import org.jumpmind.symmetric.job.ping.NodeOnlineStatus;

public class NodeOnlineStat {

    private String nodeId;

    private NodeOnlineStatus.PossibleStatus status;

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public NodeOnlineStatus.PossibleStatus getStatus() {
        return status;
    }

    public void setStatus(NodeOnlineStatus.PossibleStatus status) {
        this.status = status;
    }
}
