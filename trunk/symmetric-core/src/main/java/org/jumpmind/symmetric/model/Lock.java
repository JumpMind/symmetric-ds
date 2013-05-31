package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

public class Lock implements Serializable {

    public static final String STOPPED = "STOPPED";
    
    private static final long serialVersionUID = 1L;

    private String lockAction;
    private String lockingServerId;
    private Date lockTime;
    private Date lastLockTime;
    private String lastLockingServerId;

    public String getLockAction() {
        return lockAction;
    }
    
    public boolean isStopped() {
       return STOPPED.equals(lockingServerId) && lockTime != null;
    }
    
    public boolean isLockedByOther(String serverId) {
        return lockTime != null  && lockingServerId != null && !lockingServerId.equals(serverId);
    }

    public void setLockAction(String lockAction) {
        this.lockAction = lockAction;
    }

    public String getLockingServerId() {
        return lockingServerId;
    }

    public void setLockingServerId(String lockingServerId) {
        this.lockingServerId = lockingServerId;
    }

    public Date getLockTime() {
        return lockTime;
    }

    public void setLockTime(Date lockTime) {
        this.lockTime = lockTime;
    }

    public Date getLastLockTime() {
        return lastLockTime;
    }

    public void setLastLockTime(Date lastLockTime) {
        this.lastLockTime = lastLockTime;
    }

    public String getLastLockingServerId() {
        return lastLockingServerId;
    }

    public void setLastLockingServerId(String lastLockingServerId) {
        this.lastLockingServerId = lastLockingServerId;
    }

}
