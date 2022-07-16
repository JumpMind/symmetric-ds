package org.jumpmind.symmetric.job.ping;

public class PingResult {

    private String hostname;
    private NodeOnlineStatus.PossibleStatus resultingStatus;
    private String nodeId;

    public PingResult(String nodeId, String hostname, NodeOnlineStatus.PossibleStatus resultingStatus) {
        this.nodeId = nodeId;
        this.hostname = hostname;
        this.resultingStatus = resultingStatus;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHostname() {
        return hostname;
    }

    public NodeOnlineStatus.PossibleStatus getResultingStatus() {
        return resultingStatus;
    }

    public String toString() {
        return "nodeId : "+ nodeId + " hostname : "+ hostname + " Result Code : "+ resultingStatus;
    }
}