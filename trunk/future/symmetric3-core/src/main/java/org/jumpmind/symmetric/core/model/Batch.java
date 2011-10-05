package org.jumpmind.symmetric.core.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Batch {
    
    public static final long UNKNOWN_BATCH_ID = -9999;

    protected long batchId = UNKNOWN_BATCH_ID;
    protected String sourceNodeId;
    protected boolean initialLoad;    
    protected String channelId;
    protected Date startTime;
    protected long filterMillis;
    protected long databaseMillis;
    protected long readByteCount;
    protected long lineCount;
    protected long expectedRowCount;
    protected long insertCount;
    protected long deleteCount;
    protected long updateCount;
    protected long sqlCount;
    protected long sqlRowsAffectedCount;
    protected long otherCount;
    protected long fallbackInsertCount;
    protected long fallbackUpdateCount;
    protected long fallbackUpdateWithNewKeysCount;
    protected long dataReadMillis;
    protected long dataWriteMillis;
    protected long missingDeleteCount;
    protected long insertCollisionCount;
    
    protected Map<String, Long> timers = new HashMap<String, Long>();

    public Batch(long batchId) {
        this();
        this.batchId = batchId;
    }
    
    public Batch(long batchId, String channelId) {
        this(batchId);
        this.channelId = channelId;
    }
    
    public Batch() {
        this.startTime = new Date();
    }

    public long incrementLineCount() {
        return ++lineCount;
    }
    
    public long incrementInsertCollisionCount() {
        return ++insertCollisionCount;
    }

    public long incrementFallbackInsertCount() {
        return ++fallbackInsertCount;
    }

    public long incrementFallbackUpdateWithNewKeysCount() {
        return ++fallbackUpdateWithNewKeysCount;
    }

    public long incrementFallbackUpdateCount() {
        return ++fallbackUpdateCount;
    }

    public long incrementMissingDeleteCount() {
        return ++missingDeleteCount;
    }

    public long incrementInsertCount() {
        return ++insertCount;
    }

    public long decrementInsertCount(int number) {
        return insertCount -= number;
    }

    public long incrementUpdateCount() {
        return ++updateCount;
    }

    public long incrementDeleteCount() {
        return ++deleteCount;
    }

    public long incrementSqlCount() {
        return ++sqlCount;
    }

    public long incrementSqlRowsAffected(int count) {
        return sqlRowsAffectedCount+=count;
    }
    
    public long incrementOtherCount() {
        return ++otherCount;
    }

    public void incrementFilterMillis(long millis) {
        filterMillis += millis;
    }

    public void incrementDatabaseMillis(long millis) {
        databaseMillis += millis;
    }
    
    public void incrementDataReadMillis(long millis) {
        dataReadMillis += millis;
    }

    public void incrementDataWriteMillis(long millis) {
        dataWriteMillis += millis;
    }
    
    public void incrementReadByteCount(long count) {
        readByteCount += count;
    }
    
    public void incrementInsertCollisionCount(long count) {
        insertCollisionCount+=count;
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

    public long getFallbackInsertCount() {
        return fallbackInsertCount;
    }

    public void setFallbackInsertCount(long fallbackInsertCount) {
        this.fallbackInsertCount = fallbackInsertCount;
    }

    public long getFallbackUpdateCount() {
        return fallbackUpdateCount;
    }

    public void setFallbackUpdateCount(long fallbackUpdateCount) {
        this.fallbackUpdateCount = fallbackUpdateCount;
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

    public long getDeleteCount() {
        return deleteCount;
    }

    public long getInsertCount() {
        return insertCount;
    }

    public long getOtherCount() {
        return otherCount;
    }

    public long getSqlCount() {
        return sqlCount;
    }

    public long getUpdateCount() {
        return updateCount;
    }

    public long getMissingDeleteCount() {
        return missingDeleteCount;
    }

    public void setMissingDeleteCount(long missingDeleteCount) {
        this.missingDeleteCount = missingDeleteCount;
    }

    public long getDatabaseMillis() {
        return databaseMillis;
    }

    public void setDatabaseMillis(long databaseMillis) {
        this.databaseMillis = databaseMillis;
    }

    public long getFilterMillis() {
        return filterMillis;
    }

    public void setFilterMillis(long filterMillis) {
        this.filterMillis = filterMillis;
    }

    public long getReadByteCount() {
        return readByteCount;
    }

    public void setReadByteCount(long byteCount) {
        this.readByteCount = byteCount;
    }

    public String getSourceNodeId() {
        return sourceNodeId;
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
    
    public long getFallbackUpdateWithNewKeysCount() {
        return fallbackUpdateWithNewKeysCount;
    }
    
    public long getSqlRowsAffectedCount() {
        return sqlRowsAffectedCount;
    }

    public long getInsertCollisionCount() {
        return insertCollisionCount;
    }
}
