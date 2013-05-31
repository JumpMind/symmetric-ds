package org.jumpmind.symmetric.model;

import java.util.Date;

public class TableReloadRequest {

    protected String targetNodeId;
    protected String sourceNodeId;
    protected String triggerId;
    protected String routerId;
    protected String reloadSelect;
    protected String reloadDeleteStmt;
    protected boolean reloadEnabled = true;
    protected Date reloadTime;
    protected Date createTime = new Date();
    protected Date lastUpdateTime = new Date();
    protected String lastUpdateBy;
    
    public TableReloadRequest(TableReloadRequestKey key) {
        this.targetNodeId = key.getTargetNodeId();
        this.sourceNodeId = key.getSourceNodeId();
        this.triggerId = key.getTriggerId();
        this.routerId = key.getRouterId();
    }
    
    public TableReloadRequest() {
    }

    public String getTargetNodeId() {
        return targetNodeId;
    }

    public void setTargetNodeId(String targetNodeId) {
        this.targetNodeId = targetNodeId;
    }

    public String getSourceNodeId() {
        return sourceNodeId;
    }

    public void setSourceNodeId(String sourceNodeId) {
        this.sourceNodeId = sourceNodeId;
    }

    public String getTriggerId() {
        return triggerId;
    }

    public void setTriggerId(String triggerId) {
        this.triggerId = triggerId;
    }

    public String getRouterId() {
        return routerId;
    }

    public void setRouterId(String routerId) {
        this.routerId = routerId;
    }

    public String getReloadSelect() {
        return reloadSelect;
    }

    public void setReloadSelect(String reloadSelect) {
        this.reloadSelect = reloadSelect;
    }

    public String getReloadDeleteStmt() {
        return reloadDeleteStmt;
    }

    public void setReloadDeleteStmt(String reloadDeleteStmt) {
        this.reloadDeleteStmt = reloadDeleteStmt;
    }

    public boolean isReloadEnabled() {
        return reloadEnabled;
    }

    public void setReloadEnabled(boolean reloadEnabled) {
        this.reloadEnabled = reloadEnabled;
    }

    public Date getReloadTime() {
        return reloadTime;
    }

    public void setReloadTime(Date reloadTime) {
        this.reloadTime = reloadTime;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getLastUpdateBy() {
        return lastUpdateBy;
    }

    public void setLastUpdateBy(String lastUpdateBy) {
        this.lastUpdateBy = lastUpdateBy;
    }

}
