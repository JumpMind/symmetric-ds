package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

public class NodeCommunication implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum CommunicationType {
        PULL, PUSH, FILE_PUSH, FILE_PULL
    };
    
    private transient Node node;

    private String nodeId;

    private CommunicationType communicationType;

    private Date lockTime;

    private Date lastLockTime = new Date();

    private long lastLockMillis;

    private String lockingServerId;

    private long successCount;

    private long failCount;

    private long totalSuccessCount;

    private long totalFailCount;

    private long totalSuccessMillis;

    private long totalFailMillis;

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public CommunicationType getCommunicationType() {
        return communicationType;
    }

    public void setCommunicationType(CommunicationType communicationType) {
        this.communicationType = communicationType;
    }

    public Date getLockTime() {
        return lockTime;
    }

    public void setLockTime(Date startTime) {
        this.lockTime = startTime;
    }

    public String getLockingServerId() {
        return lockingServerId;
    }

    public void setLockingServerId(String lockingServerId) {
        this.lockingServerId = lockingServerId;
    }

    public long getSuccessCount() {
        return successCount;
    }

    public void setSuccessCount(long successCount) {
        this.successCount = successCount;
    }

    public long getFailCount() {
        return failCount;
    }

    public void setFailCount(long failCount) {
        this.failCount = failCount;
    }

    public long getTotalSuccessCount() {
        return totalSuccessCount;
    }

    public void setTotalSuccessCount(long totalSuccessCount) {
        this.totalSuccessCount = totalSuccessCount;
    }

    public long getTotalFailCount() {
        return totalFailCount;
    }

    public void setTotalFailCount(long totalFailCount) {
        this.totalFailCount = totalFailCount;
    }

    public long getTotalSuccessMillis() {
        return totalSuccessMillis;
    }

    public void setTotalSuccessMillis(long totalSuccessMillis) {
        this.totalSuccessMillis = totalSuccessMillis;
    }

    public long getTotalFailMillis() {
        return totalFailMillis;
    }

    public void setTotalFailMillis(long totalFailMillis) {
        this.totalFailMillis = totalFailMillis;
    }

    public void setLastLockMillis(long lastLockMillis) {
        this.lastLockMillis = lastLockMillis;
    }

    public long getLastLockMillis() {
        return lastLockMillis;
    }

    public void setLastLockTime(Date lastLockTime) {
        this.lastLockTime = lastLockTime;
    }

    public Date getLastLockTime() {
        return lastLockTime;
    }
    
    public void setNode(Node node) {
        this.node = node;
    }
    
    public Node getNode() {
        return node;
    }
    
    public boolean isLocked() {
        return lockTime != null;
    }
    
    public long getAverageSuccessPeriod() {
        if (totalSuccessCount > 0 && totalSuccessMillis > 0) {
            return totalSuccessMillis/totalSuccessCount;
        } else {
            return 0l;
        }
    }
    
    public long getAverageFailurePeriod() {
        if (totalFailCount > 0 && totalFailMillis > 0) {
            return totalFailMillis/totalFailCount;
        } else {
            return 0l;
        }
    }    

}
