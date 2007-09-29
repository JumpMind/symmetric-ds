package org.jumpmind.symmetric.load;


public class DataLoaderContext implements IDataLoaderContext {

    private String version;

    private String clientId;

    private String tableName;
    
    private String[] keyNames;
    
    private String[] columnNames;

    private String batchId;
    
    private boolean isSkipping;
    
    public DataLoaderContext() {
    }
    
    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
        isSkipping = false;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String locationId) {
        this.clientId = locationId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public boolean isSkipping() {
        return isSkipping;
    }

    public void setSkipping(boolean isSkipping) {
        this.isSkipping = isSkipping;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public String[] getKeyNames() {
        return keyNames;
    }

    public void setKeyNames(String[] keyNames) {
        this.keyNames = keyNames;
    }

}
