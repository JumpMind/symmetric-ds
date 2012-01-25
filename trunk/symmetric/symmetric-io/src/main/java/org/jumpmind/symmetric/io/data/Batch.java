package org.jumpmind.symmetric.io.data;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.db.util.BinaryEncoding;

public class Batch {
    
    public static final long UNKNOWN_BATCH_ID = -9999;

    protected long batchId = UNKNOWN_BATCH_ID;
    protected String nodeId;
    protected boolean initialLoad;    
    protected String channelId;
    protected BinaryEncoding binaryEncoding;   
    protected Date startTime;
    protected long lineCount;
    protected long dataReadMillis;
    protected long dataWriteMillis;
    
    protected Map<String, Long> timers = new HashMap<String, Long>();

    public Batch(long batchId) {
        this();
        this.batchId = batchId;
    }
    
    public Batch(long batchId, String channelId, BinaryEncoding binaryEncoding, String nodeId) {
        this(batchId);
        this.channelId = channelId;
        this.nodeId = nodeId;
        this.binaryEncoding = binaryEncoding;
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

    public String getNodeId() {
        return nodeId;
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

    public String getNodeBatchId() {
        return nodeId + "-" + batchId;
    }
    
}
