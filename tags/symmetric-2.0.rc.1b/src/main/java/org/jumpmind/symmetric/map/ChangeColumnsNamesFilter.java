package org.jumpmind.symmetric.map;

import java.util.Map;
import java.util.TreeMap;

import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.ext.INodeGroupExtensionPoint;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.ITableColumnFilter;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;

public class ChangeColumnsNamesFilter implements ITableColumnFilter, INodeGroupExtensionPoint {

    private String[] tables;

    private Map<String, String> columnNameMapping;

    private boolean autoRegister = true;

    private String[] nodeGroupIdsToApplyTo;

    public String[] getTables() {
        return tables;
    }

    public void setTables(String[] tables) {
        this.tables = tables;
    }

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public String[] getNodeGroupIdsToApplyTo() {
        return nodeGroupIdsToApplyTo;
    }

    public void setNodeGroupIdsToApplyTo(String[] nodeGroupIdsToApplyTo) {
        this.nodeGroupIdsToApplyTo = nodeGroupIdsToApplyTo;
    }

    public Map<String, String> getColumnNameMapping() {
        return columnNameMapping;
    }

    public void setColumnNameMapping(Map<String, String> columns) {
        this.columnNameMapping = new TreeMap<String, String>(columns);
    }

    public String[] filterColumnsNames(IDataLoaderContext ctx, DmlType dml, Table table,
            String[] columnNames) {
        if (columnNameMapping != null) {
            for (int i = 0; i < columnNames.length; i++) {
                String newColumnName = columnNameMapping.get(columnNames[i]);
                if (newColumnName != null) {
                    columnNames[i] = newColumnName;
                }
            }
        }
        return columnNames;
    }

    public Object[] filterColumnsValues(IDataLoaderContext ctx, DmlType dml, Table table,
            Object[] columnValues) {
        return columnValues;
    }

}