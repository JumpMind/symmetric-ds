package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

/**
 * Holder class for summary information about incoming batches
 */
public class IncomingBatchSummary implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private String nodeId;
    private int batchCount;
    private int dataCount;
    private IncomingBatch.Status status;
    private Date oldestBatchCreateTime;

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public int getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(int batchCount) {
        this.batchCount = batchCount;
    }

    public int getDataCount() {
        return dataCount;
    }

    public void setDataCount(int dataCount) {
        this.dataCount = dataCount;
    }

    public IncomingBatch.Status getStatus() {
        return status;
    }

    public void setStatus(IncomingBatch.Status status) {
        this.status = status;
    }

    public Date getOldestBatchCreateTime() {
        return oldestBatchCreateTime;
    }

    public void setOldestBatchCreateTime(Date oldestBatchCreateTime) {
        this.oldestBatchCreateTime = oldestBatchCreateTime;
    }

}
