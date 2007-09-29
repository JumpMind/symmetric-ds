package org.jumpmind.symmetric.load;

public interface IDataLoaderContext {

    public String getBatchId();

    public String getClientId();

    public String getTableName();

    public String getVersion();

    public boolean isSkipping();

    public String[] getColumnNames();

    public String[] getKeyNames();

}