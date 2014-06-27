package org.jumpmind.symmetric.io.data;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.db.util.BinaryEncoding;

public class Batch {
    
    public static final long UNKNOWN_BATCH_ID = -9999;

    public enum BatchType { EXTRACT, LOAD };
    
    protected long batchId = UNKNOWN_BATCH_ID;
    protected String sourceNodeId;
    protected String targetNodeId;
    protected boolean initialLoad;    
    protected String channelId;
    protected BinaryEncoding binaryEncoding;   
    protected Date startTime;
    protected long lineCount;
    protected long dataReadMillis;
    protected long dataWriteMillis;
    protected boolean ignored = false;
    protected boolean common = false;
    protected boolean complete = false;
    protected BatchType batchType;
    
    protected Map<String, Long> timers = new HashMap<String, Long>();
    
    public Batch(BatchType batchType, long batchId, String channelId, BinaryEncoding binaryEncoding, String sourceNodeId, String targetNodeId, boolean common) {
        this.batchType = batchType;
        this.batchId = batchId;
        this.channelId = channelId;
        this.sourceNodeId = sourceNodeId;
        this.targetNodeId = targetNodeId;
        this.binaryEncoding = binaryEncoding;
        this.common = common;
    }
    
    public Batch() {
        this.startTime = new Date();
    }

    public long incrementLineCount() {
        return ++lineCount;
    }
        
    public void incrementDataReadMillis(long millis) {
        dataReadMillis += millis;
    }

    public void incrementDataWriteMillis(long millis) {
        dataWriteMillis += millis;
    }
    
    public void startTimer(String name) {
        timers.put(name, System.currentTimeMillis());
    }

    public long endTimer(String name) {
        Long startTime = (Long)timers.remove(name);
        if (startTime != null) {
            return System.currentTimeMillis() - startTime;
        } else {
            return 0l;
        }
    }

    public long getDataReadMillis() {
        return dataReadMillis;
    }
    
    public long getDataWriteMillis() {
        return dataWriteMillis;
    }
    
    public long getLineCount() {
        return lineCount;
    }

    public void setLineCount(long lineCount) {
        this.lineCount = lineCount;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public String getSourceNodeId() {
        return sourceNodeId;
    }
    
    public String getTargetNodeId() {
        return targetNodeId;
    }
    
    public String getSourceNodeBatchId() {
        return String.format("%s-%d", sourceNodeId, batchId);
    }
    
    public long getBatchId() {
        return batchId;
    }

    public String getChannelId() {
        return channelId;
    }

    public boolean isInitialLoad() {
        return initialLoad;
    }
    
    public BinaryEncoding getBinaryEncoding() {
        return binaryEncoding;
    }
    
    public void setIgnored(boolean ignored) {
        this.ignored = ignored;
    }
    
    public boolean isIgnored() {
        return ignored;
    }
    
    public void setCommon(boolean commonFlag) {
        this.common = commonFlag;
    }
    
    public boolean isCommon() {
        return common;
    }
    
    public String getStagedLocation() {
        if (batchType == BatchType.EXTRACT) {
            return getStagedLocation(common, targetNodeId);            
        } else {
            return getStagedLocation(common, sourceNodeId);    
        }        
    }
    
    public static String getStagedLocation(boolean common, String nodeId) {
        return common ? "common" : nodeId;
    }
    
    public void setComplete(boolean complete) {
        this.complete = complete;
    }
    
    public boolean isComplete() {
        return complete;
    }
    
}
