package org.jumpmind.symmetric.core.model;

import java.util.Date;

public class Batch {

    protected String sourceNodeId;
    protected boolean initialLoad;
    protected long batchId;
    protected String channelId;
    protected Date startTime;
    protected long filterMillis;
    protected long databaseMillis;
    protected long byteCount;
    protected long lineCount;
    protected long insertCount;
    protected long deleteCount;
    protected long updateCount;
    protected long sqlCount;
    protected long otherCount;
    protected long fallbackInsertCount;
    protected long fallbackUpdateCount;
    protected long fallbackUpdateWithNewKeysCount;
    protected long missingDeleteCount;
    protected long timerMillis;

    public Batch() {
        this.startTime = new Date();
    }

    public long incrementLineCount() {
        return ++lineCount;
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

    public long incrementOtherCount() {
        return ++otherCount;
    }

    public void incrementFilterMillis(long millis) {
        filterMillis += millis;
    }

    public void incrementDatabaseMillis(long millis) {
        databaseMillis += millis;
    }

    public void incrementByteCount(long count) {
        byteCount += count;
    }

    public void startTimer() {
        timerMillis = System.currentTimeMillis();
    }

    public long endTimer() {
        return System.currentTimeMillis() - timerMillis;
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

    public long getByteCount() {
        return byteCount;
    }

    public void setByteCount(long byteCount) {
        this.byteCount = byteCount;
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
}
