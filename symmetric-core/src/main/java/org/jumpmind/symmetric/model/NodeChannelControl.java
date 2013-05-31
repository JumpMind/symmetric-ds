
package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

/**
 * 
 */
public class NodeChannelControl implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private String nodeId = null;
    
    private String channelId = null;

    private boolean ignoreEnabled = false;

    private boolean suspendEnabled = false;

    private Date lastExtractTime = null;
    
    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
    
    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public boolean isIgnoreEnabled() {
        return ignoreEnabled;
    }

    public void setIgnoreEnabled(boolean ignored) {
        this.ignoreEnabled = ignored;
    }

    public boolean isSuspendEnabled() {
        return suspendEnabled;
    }

    public void setSuspendEnabled(boolean suspended) {
        this.suspendEnabled = suspended;
    }

    public Date getLastExtractTime() {
        return lastExtractTime;
    }

    public void setLastExtractTime(Date lastExtractedTime) {
        this.lastExtractTime = lastExtractedTime;
    }

}