package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

public class OutgoingBatchByNodeChannelCount implements Serializable {

    private static final long serialVersionUID = 1L;
    
    String nodeId;
    
    String channelId;
    
    boolean inError;
    
    Date earliestCreateTime;
    
    Date latestUpdateTime;
    
    int maxSentCount;
    
    int processingOrder;
    
    int batchCount;
    
    int dataCount;
        
    boolean reloadFlag;

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

    public boolean isInError() {
        return inError;
    }

    public void setInError(boolean inError) {
        this.inError = inError;
    }

    public Date getEarliestCreateTime() {
        return earliestCreateTime;
    }

    public void setEarliestCreateTime(Date earliestCreateTime) {
        this.earliestCreateTime = earliestCreateTime;
    }

    public Date getLatestUpdateTime() {
        return latestUpdateTime;
    }

    public void setLatestUpdateTime(Date latestUpdateTime) {
        this.latestUpdateTime = latestUpdateTime;
    }

    public int getMaxSentCount() {
        return maxSentCount;
    }

    public void setMaxSentCount(int maxSentCount) {
        this.maxSentCount = maxSentCount;
    }

    public int getProcessingOrder() {
        return processingOrder;
    }

    public void setProcessingOrder(int processingOrder) {
        this.processingOrder = processingOrder;
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
    
    public void setReloadFlag(boolean reloadFlag) {
        this.reloadFlag = reloadFlag;
    }
    
    public boolean isReloadFlag() {
        return reloadFlag;
    }
    
    @Override
    public String toString() {
        return nodeId + ":" + channelId;
    }

}
