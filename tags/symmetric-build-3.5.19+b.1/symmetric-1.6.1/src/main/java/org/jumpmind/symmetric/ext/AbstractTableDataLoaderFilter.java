package org.jumpmind.symmetric.ext;

import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.INodeGroupDataLoaderFilter;

abstract public class AbstractTableDataLoaderFilter implements INodeGroupDataLoaderFilter {

    private boolean autoRegister = true;

    private String[] nodeGroupIdsToApplyTo;

    private boolean loadDataInTargetDatabase = true;

    private String tableName;

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        if (context.getTableName().equals(tableName)) {
            filterDeleteForTable(context, keyValues);
        }
        return loadDataInTargetDatabase;
    }

    abstract protected void filterDeleteForTable(IDataLoaderContext context, String[] keyValues);

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        if (context.getTableName().equals(tableName)) {
            filterInsertForTable(context, columnValues);
        }
        return loadDataInTargetDatabase;
    }

    abstract protected void filterInsertForTable(IDataLoaderContext context, String[] columnValues);

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues) {
        if (context.getTableName().equals(tableName)) {
            filterUpdateForTable(context, columnValues, keyValues);
        }
        return loadDataInTargetDatabase;
    }

    abstract protected void filterUpdateForTable(IDataLoaderContext context, String[] columnValues, String[] keyValues);

    public boolean isAutoRegister() {
        return this.autoRegister;
    }

    public String[] getNodeGroupIdsToApplyTo() {
        return this.nodeGroupIdsToApplyTo;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public void setNodeGroupIdsToApplyTo(String[] nodeGroupIdsToApplyTo) {
        this.nodeGroupIdsToApplyTo = nodeGroupIdsToApplyTo;
    }

    public void setNodeGroupIdToApplyTo(String nodeGroupId) {
        this.nodeGroupIdsToApplyTo = new String[] { nodeGroupId };
    }

    public void setLoadDataInTargetDatabase(boolean loadDataInTargetDatabase) {
        this.loadDataInTargetDatabase = loadDataInTargetDatabase;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

}
