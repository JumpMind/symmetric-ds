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

package org.jumpmind.symmetric.load;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
import org.jumpmind.symmetric.ddl.model.Column;

/**
 * Builds a SQL DML statement
 */
public class StatementBuilder {

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
    
    protected String tablePrefix;

    public StatementBuilder(DmlType type, String tableName, Column[] keys, Column[] columns,
            Column[] preFilteredColumns, boolean isDateOverrideToTimestamp,
            String identifierQuoteString, String tablePrefix) {
        this.keys = keys;
        this.columns = columns;
        this.preFilteredColumns = preFilteredColumns;
        this.tablePrefix = tablePrefix;
        quote = identifierQuoteString == null ? "" : identifierQuoteString;
        if (type == DmlType.INSERT) {
            sql = buildInsertSql(tableName, keys, columns);
        } else if (type == DmlType.UPDATE) {
            sql = buildUpdateSql(tableName, keys, columns);
        } else if (type == DmlType.DELETE) {
            sql = buildDeleteSql(tableName, keys);
        } else if (type == DmlType.COUNT) {
            sql = buildCountSql(tableName, keys);
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

    public String buildInsertSql(String tableName, Column[] keys, Column[] columns) {
        StringBuilder sql = new StringBuilder("insert into " + tableName + "(");
        appendColumns(sql, columns);
        sql.append(") values (");
        appendColumnQuestions(sql, columns);
        sql.append(")");
        return sql.toString();
    }

    public String buildUpdateSql(String tableName, Column[] keyColumns, Column[] columns) {
        StringBuilder sql = new StringBuilder("update ").append(tableName).append(" set ");
        appendColumnEquals(sql, columns, ", ");
        sql.append(" where ");
        appendColumnEquals(sql, keyColumns, " and ");
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

    public void appendColumnEquals(StringBuilder sql, Column[] columns, String separator) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                sql.append(quote).append(columns[i].getName()).append(quote).append(" = ?").append(separator);
            }
        }
        
        if (columns.length > 0) {
            sql.replace(sql.length()-separator.length(), sql.length(), "");
        }
    }

    public void appendColumns(StringBuilder sql, Column[] columns) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                sql.append(quote).append(columns[i].getName()).append(quote);
                sql.append(",");
            }
        }
        
        if (columns.length > 0) {
            sql.replace(sql.length()-1, sql.length(), "");
        }
    }

    public void appendColumnQuestions(StringBuilder sql, Column[] columns) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                sql.append("?").append(",");
            }
        }
        
        if (columns.length > 0) {
            sql.replace(sql.length()-1, sql.length(), "");
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

    public Column[] getColumnKeyMetaData(boolean prefiltered) {
        if (prefiltered) {
            return (Column[]) ArrayUtils.addAll(preFilteredColumns, keys);
        } else {
            return (Column[]) ArrayUtils.addAll(columns, keys);
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
}