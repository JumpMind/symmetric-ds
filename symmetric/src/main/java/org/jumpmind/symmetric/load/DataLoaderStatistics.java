package org.jumpmind.symmetric.load;

import java.util.Date;

public class DataLoaderStatistics implements IDataLoaderStatistics {

    private Date startTime;
    
    private long lineCount;
    
    private long statementCount;
    
    private long fallbackInsertCount;
    
    private long fallbackUpdateCount;
    
    private long missingDeleteCount;

    public DataLoaderStatistics() {
        this.startTime = new Date();
    }
    
    public long incrementLineCount() {
        return ++lineCount;
    }

    public long incrementFallbackInsertCount() {
        return ++fallbackInsertCount;
    }

    public long incrementFallbackUpdateCount() {
        return ++fallbackUpdateCount;
    }

    public long incrementMissingDeleteCount() {
        return ++missingDeleteCount;
    }

    public long incrementStatementCount() {
        return ++statementCount;
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

    public long getStatementCount() {
        return statementCount;
    }

    public void setStatementCount(long statementCount) {
        this.statementCount = statementCount;
    }

    public long getMissingDeleteCount() {
        return missingDeleteCount;
    }

    public void setMissingDeleteCount(long missingDeleteCount) {
        this.missingDeleteCount = missingDeleteCount;
    }

}
