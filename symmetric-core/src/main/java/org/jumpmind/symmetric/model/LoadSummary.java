package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

public class LoadSummary  implements Serializable {

    private static final long serialVersionUID = 1L;

    private long loadId;
    private String nodeId;
    private boolean inError;
    private int finishedBatchCount;
    private int pendingBatchCount;
    private long currentBatchId;
    private long currentDataEventCount;
    private String createBy;
    private Date createTime;
    private Date lastUpdateTime;
    private String channelQueue;
    private int tableCount;
    private boolean isFullLoad;
    private boolean isCreateFirst;
    private boolean isDeleteFirst;
    private boolean isRequestProcessed;
    private boolean isConditional;
    private boolean isCustomSql;
    private long batchCount;
    private String currentTableName;
    private long dataCount;
    private String processStatus;
    private String processName;
    private int targetNodeCount;
    private int ignoreCount;
    
    public boolean isActive() {
        return pendingBatchCount > 0;
    }

    public void setInError(boolean inError) {
        this.inError = inError;
    }

    public boolean isInError() {
        return inError;
    }

    public long getLoadId() {
        return loadId;
    }
    
    public void setIgnoreCount(int count){
    	this.ignoreCount = count;
    }
    
    public int getIgnoreCount(){
    	return ignoreCount;
    }

    public void setLoadId(long loadId) {
        this.loadId = loadId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public int getFinishedBatchCount() {
        return finishedBatchCount;
    }

    public void setFinishedBatchCount(int finishedBatchCount) {
        this.finishedBatchCount = finishedBatchCount;
    }

    public int getPendingBatchCount() {
        return pendingBatchCount;
    }

    public void setPendingBatchCount(int pendingBatchCount) {
        this.pendingBatchCount = pendingBatchCount;
    }

    public long getCurrentBatchId() {
        return currentBatchId;
    }

    public void setCurrentBatchId(long currentBatchId) {
        this.currentBatchId = currentBatchId;
    }

    public long getCurrentDataEventCount() {
        return currentDataEventCount;
    }

    public void setCurrentDataEventCount(long currentDataEventCount) {
        this.currentDataEventCount = currentDataEventCount;
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
   
    public void setCreateBy(String createBy) {
        this.createBy = createBy;
    }

    public String getCreateBy() {
        return createBy;
    }
    
    public String getLoadNodeId() {
        return String.format("%010d-%s", loadId, nodeId);
    }

    public String getChannelQueue() {
        return channelQueue;
    }

    public void setChannelQueue(String channelQueue) {
        this.channelQueue = channelQueue;
    }

    public int getTableCount() {
        return tableCount;
    }

    public void setTableCount(int tableCount) {
        this.tableCount = tableCount;
    }

    public boolean isFullLoad() {
        return isFullLoad;
    }

    public void setFullLoad(boolean isFullLoad) {
        this.isFullLoad = isFullLoad;
    }

    public boolean isCreateFirst() {
        return isCreateFirst;
    }

    public void setCreateFirst(boolean isCreateFirst) {
        this.isCreateFirst = isCreateFirst;
    }

    public boolean isDeleteFirst() {
        return isDeleteFirst;
    }

    public void setDeleteFirst(boolean isDeleteFirst) {
        this.isDeleteFirst = isDeleteFirst;
    }

    public boolean isRequestProcessed() {
        return isRequestProcessed;
    }

    public void setRequestProcessed(boolean isRequestProcessed) {
        this.isRequestProcessed = isRequestProcessed;
    }

    public boolean isConditional() {
        return isConditional;
    }

    public void setConditional(boolean isConditional) {
        this.isConditional = isConditional;
    }

    public boolean isCustomSql() {
        return isCustomSql;
    }

    public void setCustomSql(boolean isCustomSql) {
        this.isCustomSql = isCustomSql;
    }

    public long getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(long batchCount) {
        this.batchCount = batchCount;
    }

    public String getCurrentTableName() {
        return currentTableName;
    }

    public void setCurrentTableName(String currentTableName) {
        this.currentTableName = currentTableName;
    }

    public long getDataCount() {
        return dataCount;
    }

    public void setDataCount(long dataCount) {
        this.dataCount = dataCount;
    }

    public String getProcessStatus() {
        return processStatus;
    }

    public void setProcessStatus(String processStatus) {
        this.processStatus = processStatus;
    }

    public String getProcessName() {
        return processName;
    }

    public void setProcessName(String processName) {
        this.processName = processName;
    }

    public int getTargetNodeCount() {
        return targetNodeCount;
    }

    public void setTargetNodeCount(int targetNodeCount) {
        this.targetNodeCount = targetNodeCount;
    }
    
    
    
    
}

