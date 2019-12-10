package org.jumpmind.symmetric.web.rest.model;

import org.jumpmind.symmetric.job.ping.NodeOnlineStatus;

import java.util.ArrayList;
import java.util.List;

public class NodeOnlineStatusList {

    List<NodeOnlineStat> statusList;

    public NodeOnlineStatusList() {
        this.statusList = new ArrayList<NodeOnlineStat>();
    }

    public void setStatusList(NodeOnlineStat[] list) {
        this.statusList = new ArrayList<NodeOnlineStat>();
        for(NodeOnlineStat s : list) {
            this.statusList.add(s);
        }
    }

    public NodeOnlineStat[] getStatusList() {
        return statusList.toArray(new NodeOnlineStat[statusList.size()]);
    }

    public void addNodeStatus(String nodeId, NodeOnlineStatus.PossibleStatus status) {
        NodeOnlineStat s = new NodeOnlineStat();
        s.setNodeId(nodeId);
        s.setStatus(status);
        this.statusList.add(s);
    }
}
