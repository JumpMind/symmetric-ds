package org.jumpmind.symmetric.model;

public class TableReloadRequestKey {

    protected String targetNodeId;
    protected String sourceNodeId;
    protected String triggerId;
    protected String routerId;
    protected String receivedFromNodeId;

    public TableReloadRequestKey(String targetNodeId, String sourceNodeId, String triggerId,
            String routerId, String receivedFromNodeId) {
        this.targetNodeId = targetNodeId;
        this.sourceNodeId = sourceNodeId;
        this.triggerId = triggerId;
        this.routerId = routerId;
        this.receivedFromNodeId = receivedFromNodeId;
    }

    public String getRouterId() {
        return routerId;
    }

    public String getSourceNodeId() {
        return sourceNodeId;
    }

    public String getTargetNodeId() {
        return targetNodeId;
    }

    public String getTriggerId() {
        return triggerId;
    }
    
    public void setReceivedFromNodeId(String receivedFromNodeId) {
		this.receivedFromNodeId = receivedFromNodeId;
	}
    
    public String getReceivedFromNodeId() {
		return receivedFromNodeId;
	}

}
