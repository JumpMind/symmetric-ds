package org.jumpmind.symmetric.model;

public class NodeChannel extends Channel {

    private static final long serialVersionUID = -2493052366767513160L;
    
    String nodeId;

    private boolean ignored;

    private boolean suspended;

    public boolean isIgnored() {
        return ignored;
    }

    public void setIgnored(boolean ignore) {
        this.ignored = ignore;
    }

    public boolean isSuspended() {
        return suspended;
    }

    public void setSuspended(boolean suspend) {
        this.suspended = suspend;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
}
