package org.jumpmind.db.platform.postgresql;

import java.util.Map;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.platform.DatabaseInfo;

public class PostgreSqlDmlStatement95 extends PostgreSqlDmlStatement {

    public PostgreSqlDmlStatement95(DmlType type, String catalogName, String schemaName, String tableName,
            Column[] keysColumns, Column[] columns, boolean[] nullKeyValues, DatabaseInfo databaseInfo,
            boolean useQuotedIdentifiers, String textColumnExpression) {
        super(type, catalogName, schemaName, tableName, keysColumns, columns, nullKeyValues, databaseInfo, useQuotedIdentifiers,
                textColumnExpression);
    }

    @Override
    public String buildInsertSql(String tableName, Column[] keys, Column[] columns) {
        StringBuilder sql = new StringBuilder("insert into " + tableName + " (");
        appendColumns(sql, columns, false);
        sql.append(") values (");
        appendColumnParameters(sql, columns);
        sql.append(") on conflict do nothing");
        return sql.toString();
    }

    @Override
    public Column[] getMetaData() {
        if (dmlType == DmlType.INSERT) {
            return getColumns();
        } else {
            return super.getMetaData();
        }
    }

    @Override
    public <T> T[] getValueArray(T[] columnValues, T[] keyValues) {
        if (dmlType == DmlType.INSERT) {
            return columnValues;
        } else {
            return super.getValueArray(columnValues, keyValues);
        }
    }

    @Override
    public Object[] getValueArray(Map<String, Object> params) {
        if (dmlType == DmlType.INSERT) {
            int index = 0;
            Object[] args = new Object[columns.length];
            for (Column column : columns) {
                args[index++] = params.get(column.getName());
            }
            return args;
        }
        return super.getValueArray(params);
    }

    @Override
    protected int[] buildTypes(Column[] keys, Column[] columns, boolean isDateOverrideToTimestamp) {
        if (dmlType == DmlType.INSERT) {
            return buildTypes(columns, isDateOverrideToTimestamp);
        } else {
            return super.buildTypes(keys, columns, isDateOverrideToTimestamp);
        }
    }
    
}
