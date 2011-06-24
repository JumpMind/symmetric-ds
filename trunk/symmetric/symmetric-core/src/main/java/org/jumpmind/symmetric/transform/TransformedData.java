package org.jumpmind.symmetric.transform;

import java.util.Iterator;
import java.util.TreeMap;

import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;

public class TransformedData {

    protected DmlType dmlType;
    protected String tableName;
    protected String catalogName;
    protected String schemaName;

    protected TreeMap<String, String> columnValues;
    protected TreeMap<String, String> keyValues;

    public TransformedData(String catalogName, String schemaName, String tableName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String[] getKeyNames() {
        return keyValues.keySet().toArray(new String[keyValues.size()]);
    }

    public String[] getColumnNames() {
        return columnValues.keySet().toArray(new String[columnValues.size()]);
    }

    public String getKeyString() {
        Iterator<String> values = keyValues.values().iterator();
        if (values.hasNext()) {
            String keyString = values.next();
            while (values.hasNext()) {
                keyString += "||";
                keyString += values.next();
            }
            return keyString;
        }
        return null;
    }

    public String getFullyQualifiedTableName() {
        return Table.getFullyQualifiedTableName(tableName, schemaName, catalogName);
    }

    public DmlType getDmlType() {
        return dmlType;
    }

    public void setDmlType(DmlType dmlType) {
        this.dmlType = dmlType;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

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

    public void put(String columnName, String columnValue, boolean pk) {
        if (pk) {
            if (keyValues == null) {
                keyValues = new TreeMap<String, String>();
            }
            keyValues.put(columnName, columnValue);

        } else {
            if (columnValues == null) {
                columnValues = new TreeMap<String, String>();
            }
            columnValues.put(columnName, columnValue);
        }
    }

    public String[] getColumnValues() {
        return columnValues.values().toArray(new String[columnValues.size()]);
    }

    public String[] getKeyValues() {
        return keyValues.values().toArray(new String[keyValues.size()]);
    }

}
