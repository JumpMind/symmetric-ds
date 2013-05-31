package org.jumpmind.symmetric.statistic;

import java.util.Date;

abstract public class AbstractNodeHostStats {

    private String nodeId;
    private String hostName;
    private Date startTime;
    private Date endTime;
    
    public AbstractNodeHostStats() {
    }
    
    public AbstractNodeHostStats(String nodeId, String hostName, Date startTime, Date endTime) {
        this.nodeId = nodeId;
        this.hostName = hostName;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }
    
    public Date getEndTime() {
        return endTime;
    }
    
    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
    
    public String getNodeId() {
        return nodeId;
    }
    
    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }
    
    public Date getStartTime() {
        return startTime;
    }
    
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }
    
    public String getHostName() {
        return hostName;
    }
}