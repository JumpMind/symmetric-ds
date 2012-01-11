package org.jumpmind.db.platform;

public class DatabasePlatformSettings {

    protected int fetchSize = 1000;
    protected int queryTimeout;
    protected int batchSize = 100;
    
    public DatabasePlatformSettings() {     
    }
        
    public DatabasePlatformSettings(int fetchSize, int queryTimeout, int batchSize) {
        this.fetchSize = fetchSize;
        this.queryTimeout = queryTimeout;
        this.batchSize = batchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setQueryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }
    
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
    
    public int getBatchSize() {
        return batchSize;
    }

}
