package org.jumpmind.symmetric.map;

public class TableColumnValueFilter {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private String columnName;
    private IValueFilter filter;
    private boolean enabled = true;    

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public IValueFilter getFilter() {
        return filter;
    }

    public void setFilter(IValueFilter filter) {
        this.filter = filter;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public boolean isEnabled() {
        return enabled;
    }
}