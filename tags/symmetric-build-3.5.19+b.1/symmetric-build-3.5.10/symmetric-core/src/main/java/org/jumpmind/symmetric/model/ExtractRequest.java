package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

public class ExtractRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum ExtractStatus {
        NE, OK
    };

    private long requestId;
    private String nodeId;
    private ExtractStatus status;
    private long startBatchId;
    private long endBatchId;
    private TriggerRouter triggerRouter;
    private Date lastUpdateTime;
    private Date createTime;

    public long getRequestId() {
        return requestId;
    }

    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public ExtractStatus getStatus() {
        return status;
    }

    public void setStatus(ExtractStatus status) {
        this.status = status;
    }

    public long getStartBatchId() {
        return startBatchId;
    }

    public void setStartBatchId(long startBatchId) {
        this.startBatchId = startBatchId;
    }

    public long getEndBatchId() {
        return endBatchId;
    }

    public void setEndBatchId(long endBatchId) {
        this.endBatchId = endBatchId;
    }

    public TriggerRouter getTriggerRouter() {
        return triggerRouter;
    }

    public void setTriggerRouter(TriggerRouter triggerRouter) {
        this.triggerRouter = triggerRouter;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

}
