/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */


package org.jumpmind.db.sql;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;

/**
 * Builds a SQL DML statement
 */
public class DmlStatement {
    
    public enum DmlType {
        INSERT, UPDATE, DELETE, COUNT
    };

    protected DmlType dmlType;

    protected String sql;

    protected int[] types;

    protected String quote;

    protected Column[] keys;

    protected Column[] columns;

    public DmlStatement(DmlType type, String catalogName, String schemaName, String tableName, Column[] keys, Column[] columns, 
            boolean isDateOverrideToTimestamp, String identifierQuoteString) {
        this.keys = keys;
        this.columns = columns;
        quote = identifierQuoteString == null ? "" : identifierQuoteString;
        if (type == DmlType.INSERT) {
            sql = buildInsertSql(Table.getFullyQualifiedTableName(catalogName, schemaName, tableName, identifierQuoteString), keys, columns);
        } else if (type == DmlType.UPDATE) {
            sql = buildUpdateSql(Table.getFullyQualifiedTableName(catalogName, schemaName, tableName, identifierQuoteString), keys, columns);
        } else if (type == DmlType.DELETE) {
            sql = buildDeleteSql(Table.getFullyQualifiedTableName(catalogName, schemaName, tableName, identifierQuoteString), keys);
        } else if (type == DmlType.COUNT) {
            sql = buildCountSql(Table.getFullyQualifiedTableName(catalogName, schemaName, tableName, identifierQuoteString), keys);
        } else {
            throw new NotImplementedException("Unimplemented SQL type: " + type);
        }
        dmlType = type;
        types = buildTypes(keys, columns, isDateOverrideToTimestamp);

    }

    protected Column[] removeKeysFromColumns(Column[] keys, Column[] columns) {
        Column[] columnsWithoutKeys = new Column[columns.length - keys.length];
        Set<Column> keySet = new HashSet<Column>();
        CollectionUtils.addAll(keySet, keys);
        int n = 0;
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            if (!keySet.contains(column)) {
                columnsWithoutKeys[n++] = column;
            }
        }
        return columnsWithoutKeys;
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
        }
        return null;

    }

    protected int[] buildTypes(Column[] columns, boolean isDateOverrideToTimestamp) {
        ArrayList<Integer> list = new ArrayList<Integer>(columns.length);
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                list.add(columns[i].getTypeCode());
            }
        }
        int[] types = new int[list.size()];
        int index = 0;
        for (Integer type : list) {
            if (type == Types.DATE && isDateOverrideToTimestamp) {
                type = Types.TIMESTAMP;
            } else if (type == Types.FLOAT || type == Types.DOUBLE) {
                type = Types.DECIMAL;
            }
            types[index++] = type;
        }
        return types;
    }

    public String buildInsertSql(String tableName, String[] columnNames) {
        StringBuilder sql = new StringBuilder("insert into " + tableName + "(");
        appendColumns(sql, columnNames);
        sql.append(") values (");
        appendColumnQuestions(sql, columnNames.length);
        sql.append(")");
        return sql.toString();
    }

    public String buildInsertSql(String tableName, Column[] keys, Column[] columns) {
        StringBuilder sql = new StringBuilder("insert into " + tableName + "(");
        int columnCount = appendColumns(sql, columns);
        sql.append(") values (");
        appendColumnQuestions(sql, columnCount);
        sql.append(")");
        return sql.toString();
    }

    public String buildUpdateSql(String tableName, String[] keyNames, String[] columnNames) {
        StringBuilder sql = new StringBuilder("update ").append(tableName).append(" set ");
        appendColumnEquals(sql, columnNames, ", ");
        sql.append(" where ");
        appendColumnEquals(sql, keyNames, " and ");
        return sql.toString();
    }

    public String buildUpdateSql(String tableName, Column[] keyColumns, Column[] columns) {
        StringBuilder sql = new StringBuilder("update ").append(tableName).append(" set ");
        appendColumnEquals(sql, columns, ", ");
        sql.append(" where ");
        appendColumnEquals(sql, keyColumns, " and ");
        return sql.toString();
    }

    public String buildDeleteSql(String tableName, String[] keyNames) {
        StringBuilder sql = new StringBuilder("delete from ").append(tableName).append(" where ");
        appendColumnEquals(sql, keyNames, " and ");
        return sql.toString();
    }

    public String buildDeleteSql(String tableName, Column[] keyColumns) {
        StringBuilder sql = new StringBuilder("delete from ").append(tableName).append(" where ");
        appendColumnEquals(sql, keyColumns, " and ");
        return sql.toString();
    }

    public String buildCountSql(String tableName, Column[] keyColumns) {
        StringBuilder sql = new StringBuilder("select count(*) from ").append(tableName).append(
                " where ");
        appendColumnEquals(sql, keyColumns, " and ");
        return sql.toString();
    }

    public void appendColumnEquals(StringBuilder sql, String[] names, String separator) {
        for (int i = 0; i < names.length; i++) {
            sql.append(quote).append(names[i]).append(quote).append(" = ?").append(
                    i + 1 < names.length ? separator : "");
        }
    }

    public void appendColumnEquals(StringBuilder sql, Column[] columns, String separator) {
        int existingCount = 0;
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                if (existingCount++ > 0) {
                    sql.append(separator);
                }
                sql.append(quote).append(columns[i].getName()).append(quote).append(" = ?");
            }
        }
    }

    public void appendColumns(StringBuilder sql, String[] names) {
        for (int i = 0; i < names.length; i++) {
            sql.append(quote).append(names[i]).append(quote)
                    .append(i + 1 < names.length ? "," : "");
        }
    }

    public int appendColumns(StringBuilder sql, Column[] columns) {
        int existingCount = 0;
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                if (existingCount++ > 0) {
                    sql.append(",");
                }
                sql.append(quote).append(columns[i].getName()).append(quote);
            }
        }
        return existingCount;
    }

    public void appendColumnQuestions(StringBuilder sql, int number) {
        for (int i = 0; i < number; i++) {
            sql.append("?").append(i + 1 < number ? "," : "");
        }
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
        }
        return null;
    }

    public Column[] getKeys() {
        return keys;
    }

    public String[] getValueArray(String[] columnValues, String[] keyValues) {
        switch (dmlType) {
        case UPDATE:
            return (String[]) ArrayUtils.addAll(columnValues, keyValues);
        case INSERT:
            return columnValues;
        case DELETE:
            return keyValues;
        }
        return null;
    }
    
    public Object[] buildArgsFrom(Map<String, Object> params) {
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