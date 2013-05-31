package org.jumpmind.db.sql;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;

/**
 * Builds a SQL DML statement
 */
public class DmlStatement {

    public enum DmlType {
        INSERT, UPDATE, DELETE, COUNT, FROM, SELECT, SELECT_ALL, UNKNOWN
    };

    protected DmlType dmlType;

    protected String sql;

    protected int[] types;

    protected String quote;

    protected Column[] keys;

    protected Column[] columns;

    protected boolean[] nullKeyValues;

    public DmlStatement(DmlType type, String catalogName, String schemaName, String tableName,
            Column[] keysColumns, Column[] columns, boolean isDateOverrideToTimestamp,
            String identifierQuoteString, boolean[] nullKeyValues) {
        this.columns = columns;
        if (nullKeyValues == null || keysColumns == null
                || nullKeyValues.length != keysColumns.length) {
            this.keys = keysColumns;
            this.nullKeyValues = keysColumns == null ? null : new boolean[keysColumns.length];
        } else {
            List<Column> cols = new ArrayList<Column>(keysColumns.length);
            // weed out null values
            for (int i = 0; i < keysColumns.length; i++) {
                if (!nullKeyValues[i]) {
                    cols.add(keysColumns[i]);
                }
            }
            this.keys = cols.toArray(new Column[cols.size()]);
            this.nullKeyValues = nullKeyValues;
        }
        this.quote = identifierQuoteString == null ? "" : identifierQuoteString;
        if (type == DmlType.INSERT) {
            this.sql = buildInsertSql(Table.getFullyQualifiedTableName(catalogName, schemaName,
                    tableName, identifierQuoteString), keysColumns, columns);
        } else if (type == DmlType.UPDATE) {
            this.sql = buildUpdateSql(Table.getFullyQualifiedTableName(catalogName, schemaName,
                    tableName, identifierQuoteString), keysColumns, columns);
        } else if (type == DmlType.DELETE) {
            this.sql = buildDeleteSql(Table.getFullyQualifiedTableName(catalogName, schemaName,
                    tableName, identifierQuoteString), keysColumns);
        } else if (type == DmlType.COUNT) {
            this.sql = buildCountSql(Table.getFullyQualifiedTableName(catalogName, schemaName,
                    tableName, identifierQuoteString), keysColumns);
        } else if (type == DmlType.FROM) {
            this.sql = buildFromSql(Table.getFullyQualifiedTableName(catalogName, schemaName,
                    tableName, identifierQuoteString), keysColumns);
        } else if (type == DmlType.SELECT) {
            this.sql = buildSelectSql(Table.getFullyQualifiedTableName(catalogName, schemaName,
                    tableName, identifierQuoteString), keysColumns, columns);
        } else if (type == DmlType.SELECT_ALL) {
            this.sql = buildSelectSqlAll(Table.getFullyQualifiedTableName(catalogName, schemaName,
                    tableName, identifierQuoteString), keysColumns, columns);
        } else {
            throw new NotImplementedException("Unimplemented SQL type: " + type);
        }
        this.dmlType = type;
        this.types = buildTypes(this.keys, columns, isDateOverrideToTimestamp);

    }

    protected int[] buildTypes(Column[] keys, Column[] columns, boolean isDateOverrideToTimestamp) {
        switch (dmlType) {
            case UPDATE:
                int[] columnTypes = buildTypes(columns, isDateOverrideToTimestamp);
                int[] keyTypes = buildTypes(keys, isDateOverrideToTimestamp);
                return ArrayUtils.addAll(columnTypes, keyTypes);
            case INSERT:
                return buildTypes(columns, isDateOverrideToTimestamp);
            case DELETE:
                return buildTypes(keys, isDateOverrideToTimestamp);
            case COUNT:
                return buildTypes(keys, isDateOverrideToTimestamp);
            default:
                break;
        }
        return null;

    }

    protected int getTypeCode(Column column, boolean isDateOverrideToTimestamp) {
        int type = column.getMappedTypeCode();
        if (type == Types.DATE && isDateOverrideToTimestamp) {
            type = Types.TIMESTAMP;
        } else if (type == Types.FLOAT || type == Types.DOUBLE || type == Types.REAL) {
            type = Types.DECIMAL;
        }
        return type;
    }

    protected int[] buildTypes(Column[] columns, boolean isDateOverrideToTimestamp) {
        if (columns != null) {
            ArrayList<Integer> list = new ArrayList<Integer>(columns.length);
            for (int i = 0; i < columns.length; i++) {
                if (columns[i] != null) {
                    list.add(getTypeCode(columns[i], isDateOverrideToTimestamp));
                }
            }

            int[] types = new int[list.size()];
            for (int index = 0; index < list.size(); index++) {
                types[index] = list.get(index);
            }
            return types;
        } else {
            return null;
        }
    }

    protected String buildInsertSql(String tableName, Column[] keys, Column[] columns) {
        StringBuilder sql = new StringBuilder("insert into " + tableName + " (");
        appendColumns(sql, columns, false);
        sql.append(") values (");
        appendColumnQuestions(sql, columns);
        sql.append(")");
        return sql.toString();
    }

    protected String buildUpdateSql(String tableName, Column[] keyColumns, Column[] columns) {
        StringBuilder sql = new StringBuilder("update ").append(tableName).append(" set ");
        appendColumnEquals(sql, columns, ", ");
        if (keyColumns != null && keyColumns.length > 0) {
            sql.append(" where ");
            appendColumnEquals(sql, keyColumns, nullKeyValues, " and ");
        }
        return sql.toString();
    }

    protected String buildDeleteSql(String tableName, Column[] keyColumns) {
        StringBuilder sql = new StringBuilder("delete from ").append(tableName).append(" where ");
        appendColumnEquals(sql, keyColumns, nullKeyValues, " and ");
        return sql.toString();
    }

    protected String buildFromSql(String tableName, Column[] keyColumns) {
        StringBuilder sql = new StringBuilder(" from ").append(tableName).append(" where ");
        appendColumnEquals(sql, keyColumns, nullKeyValues, " and ");
        return sql.toString();
    }

    protected String buildCountSql(String tableName, Column[] keyColumns) {
        StringBuilder sql = new StringBuilder("select count(*) from ").append(tableName);
        if (keyColumns != null && keyColumns.length > 0) {
            sql.append(" where ");
            appendColumnEquals(sql, keyColumns, nullKeyValues, " and ");
        }
        return sql.toString();
    }

    protected String buildSelectSql(String tableName, Column[] keyColumns, Column[] columns) {
        StringBuilder sql = new StringBuilder("select ");
        appendColumns(sql, columns, true);
        sql.append(" from ").append(tableName).append(" where ");
        appendColumnEquals(sql, keyColumns, nullKeyValues, " and ");
        return sql.toString();
    }

    protected String buildSelectSqlAll(String tableName, Column[] keyColumns, Column[] columns) {
        StringBuilder sql = new StringBuilder("select ");
        appendColumns(sql, columns, true);
        sql.append(" from ").append(tableName);
        return sql.toString();
    }

    protected void appendColumnEquals(StringBuilder sql, Column[] columns, String separator) {
        appendColumnEquals(sql, columns, new boolean[columns.length], separator);
    }

    protected void appendColumnEquals(StringBuilder sql, Column[] columns, boolean[] nullColumns,
            String separator) {
        int existingCount = 0;
        if (columns != null) {
            for (int i = 0; i < columns.length && i < nullColumns.length; i++) {
                if (columns[i] != null) {
                    if (existingCount++ > 0) {
                        sql.append(separator);
                    }
                    if (!nullColumns[i]) {
                        sql.append(quote).append(columns[i].getName()).append(quote).append(" = ?");
                    } else {
                        sql.append(quote).append(columns[i].getName()).append(quote)
                                .append(" is NULL");
                    }
                }
            }
        }
    }

    protected int appendColumns(StringBuilder sql, Column[] columns, boolean select) {
        int existingCount = 0;
        if (columns != null) {
            for (int i = 0; i < columns.length; i++) {
                if (columns[i] != null) {
                    if (existingCount++ > 0) {
                        sql.append(", ");
                    }
                    appendColumnNameForSql(sql, columns[i], select);
                }
            }
        }
        return existingCount;
    }
    
    protected void appendColumnNameForSql(StringBuilder sql, Column column, boolean select) {
        String columnName = column.getName();
        sql.append(quote).append(columnName).append(quote);
    }
    

    protected void appendColumnQuestions(StringBuilder sql, Column[] columns) {
        if (columns != null) {
            for (int i = 0; i < columns.length; i++) {
                if (columns[i] != null) {
                    sql.append("?").append(",");
                }
            }

            if (columns.length > 0) {
                sql.replace(sql.length() - 1, sql.length(), "");
            }
        }

    }

    public String getColumnsSql(Column[] columns) {
        StringBuilder sql = new StringBuilder("select ");
        appendColumns(sql, columns, true);
        sql.append(getSql());
        return sql.toString();
    }

    public String getSql() {
        return sql;
    }

    public DmlType getDmlType() {
        return dmlType;
    }

    public int[] getTypes() {
        return types;
    }

    public Column[] getColumns() {
        return columns;
    }

    public Column[] getColumnKeyMetaData() {
        return (Column[]) ArrayUtils.addAll(columns, keys);
    }

    public Column[] getMetaData() {
        switch (dmlType) {
            case UPDATE:
                return getColumnKeyMetaData();
            case INSERT:
                return getColumns();
            case DELETE:
                return getKeys();
            default:
                break;
        }
        return null;
    }

    public Column[] getKeys() {
        return keys;
    }
   
    @SuppressWarnings("unchecked")
    public <T> T[] getValueArray(T[] columnValues, T[] keyValues) {
        switch (dmlType) {
            case UPDATE:
                return (T[]) ArrayUtils.addAll(columnValues, keyValues);
            case INSERT:
                return columnValues;
            case DELETE:
                return keyValues;
            default:
                break;
        }
        return null;
    }    

    public Object[] getValueArray(Map<String, Object> params) {
        Object[] args = null;
        if (params != null) {
            int index = 0;
            switch (dmlType) {
                case INSERT:
                    args = new Object[columns.length];
                    for (Column column : columns) {
                        args[index++] = params.get(column.getName());
                    }
                    break;
                case UPDATE:
                    args = new Object[columns.length + keys.length];
                    for (Column column : columns) {
                        args[index++] = params.get(column.getName());
                    }
                    for (Column column : keys) {
                        args[index++] = params.get(column.getName());
                    }
                    break;
                case DELETE:
                    args = new Object[keys.length];
                    for (Column column : keys) {
                        args[index++] = params.get(column.getName());
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        return args;
    }
}