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
 * under the License. 
 */

package org.jumpmind.symmetric.core.db;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.core.common.CollectionUtils;
import org.jumpmind.symmetric.core.common.NotImplementedException;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Table;

/**
 * Represents and builds a SQL DML statement based on a column defintions
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

    protected Column[] preFilteredColumns;

    public DmlStatement(DmlType type, String catalogName, String schemaName, String tableName, Column[] keys, Column[] columns,
            Column[] preFilteredColumns, boolean isDateOverrideToTimestamp,
            String identifierQuoteString) {
        this.keys = keys;
        this.columns = columns;
        this.preFilteredColumns = preFilteredColumns;
        this.quote = identifierQuoteString == null ? "" : identifierQuoteString;
        if (type == DmlType.INSERT) {
            sql = buildInsertSql(Table.getFullyQualifiedTableName(catalogName, schemaName, tableName, quote), columns);
            types = buildTypes(columns, isDateOverrideToTimestamp);
        } else if (type == DmlType.UPDATE) {
            sql = buildUpdateSql(Table.getFullyQualifiedTableName(catalogName, schemaName, tableName, quote), keys, columns);
            types = buildTypes(keys, columns, isDateOverrideToTimestamp);
        } else if (type == DmlType.DELETE) {
            sql = buildDeleteSql(Table.getFullyQualifiedTableName(catalogName, schemaName, tableName, quote), keys);
            types = buildTypes(keys, isDateOverrideToTimestamp);
        } else if (type == DmlType.COUNT) {
            sql = buildCountSql(Table.getFullyQualifiedTableName(catalogName, schemaName, tableName, quote), keys);
            types = buildTypes(keys, isDateOverrideToTimestamp);
        } else {
            throw new NotImplementedException("Unimplemented SQL type: " + type);
        }
        dmlType = type;
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
        int[] columnTypes = buildTypes(columns, isDateOverrideToTimestamp);
        int[] keyTypes = buildTypes(keys, isDateOverrideToTimestamp);
        return CollectionUtils.addAll(columnTypes, keyTypes);
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

    protected String buildInsertSql(String fullQualifiedTableName, Column[] columns) {
        StringBuilder sql = new StringBuilder("insert into ");
        sql.append(fullQualifiedTableName);
        sql.append(" (");
        appendColumns(sql, columns);
        sql.append(") values (");
        appendColumnQuestions(sql, columns);
        sql.append(")");
        return sql.toString();
    }

    protected String buildUpdateSql(String fullQualifiedTableName, String[] keyNames, String[] columnNames) {
        StringBuilder sql = new StringBuilder("update ").append(fullQualifiedTableName).append(" set ");
        appendColumnEquals(sql, columnNames, ", ");
        if (keyNames != null && keyNames.length > 0) {
            sql.append(" where ");
            appendColumnEquals(sql, keyNames, " and ");
        }
        return sql.toString();
    }

    protected String buildUpdateSql(String fullQualifiedTableName, Column[] keyColumns, Column[] columns) {
        StringBuilder sql = new StringBuilder("update ").append(fullQualifiedTableName).append(" set ");
        appendColumnEquals(sql, columns, ", ");
        if (keyColumns != null && keyColumns.length > 0) {
            sql.append(" where ");
            appendColumnEquals(sql, keyColumns, " and ");
        }
        return sql.toString();
    }

    protected String buildDeleteSql(String fullQualifiedTableName, String[] keyNames) {
        StringBuilder sql = new StringBuilder("delete from ").append(fullQualifiedTableName);
        if (keyNames != null && keyNames.length > 0) {
            sql.append(" where ");
            appendColumnEquals(sql, keyNames, " and ");
        }
        return sql.toString();
    }

    protected String buildDeleteSql(String fullQualifiedTableName, Column[] keyColumns) {
        StringBuilder sql = new StringBuilder("delete from ").append(fullQualifiedTableName);
        if (keyColumns != null && keyColumns.length > 0) {
            sql.append(" where ");
            appendColumnEquals(sql, keyColumns, " and ");
        }
        return sql.toString();
    }

    protected String buildCountSql(String fullQualifiedTableName, Column[] keyColumns) {
        StringBuilder sql = new StringBuilder("select count(*) from ").append(fullQualifiedTableName).append(
                " where ");
        appendColumnEquals(sql, keyColumns, " and ");
        return sql.toString();
    }

    protected void appendColumnEquals(StringBuilder sql, String[] names, String separator) {
        for (int i = 0; i < names.length; i++) {
            sql.append(quote).append(names[i]).append(quote).append(" = ?")
                    .append(i + 1 < names.length ? separator : "");
        }
    }

    protected void appendColumnEquals(StringBuilder sql, Column[] columns, String separator) {
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

    protected void appendColumns(StringBuilder sql, String[] names) {
        for (int i = 0; i < names.length; i++) {
            sql.append(quote).append(names[i]).append(quote)
                    .append(i + 1 < names.length ? "," : "");
        }
    }

    protected int appendColumns(StringBuilder sql, Column[] columns) {
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

    protected void appendColumnQuestions(StringBuilder sql, Column[] columns) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                sql.append("?").append(",");
            }
        }
        
        sql.replace(sql.length()-1, sql.length(), "");
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

    public Column[] getColumnKeyMetaData(boolean prefiltered) {
        if (prefiltered) {
            return (Column[]) CollectionUtils.addAll(preFilteredColumns, keys);
        } else {
            return (Column[]) CollectionUtils.addAll(columns, keys);
        }
    }

    public Column[] getMetaData(boolean prefiltered) {
        switch (dmlType) {
        case UPDATE:
            return getColumnKeyMetaData(prefiltered);
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

    public Column[] getPreFilteredColumns() {
        return preFilteredColumns;
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