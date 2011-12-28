package org.jumpmind.db.platform;

public class DatabasePlatformSettings {

    protected int fetchSize = 1000;
    protected int queryTimeout;
    
    public DatabasePlatformSettings() {     
    }
        
    public DatabasePlatformSettings(int fetchSize, int queryTimeout) {
        this.fetchSize = fetchSize;
        this.queryTimeout = queryTimeout;
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

}
