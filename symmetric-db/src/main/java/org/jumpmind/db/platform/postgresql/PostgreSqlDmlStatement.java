/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.db.platform.postgresql;

import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.DmlStatement;

public class PostgreSqlDmlStatement extends DmlStatement {

    public PostgreSqlDmlStatement(DmlType type, String catalogName, String schemaName, String tableName,
            Column[] keysColumns, Column[] columns, boolean[] nullKeyValues, 
            DatabaseInfo databaseInfo, boolean useQuotedIdentifiers, String textColumnExpression) {
        super(type, catalogName, schemaName, tableName, keysColumns, columns, 
                nullKeyValues, databaseInfo, useQuotedIdentifiers, textColumnExpression);
    }

    @Override
    public String buildInsertSql(String tableName, Column[] keyColumns, Column[] columns) {
        if (keyColumns != null && keyColumns.length > 0 && keyColumns[0] != null) {
            StringBuilder sql = new StringBuilder("insert into ");
            sql.append(tableName);
            sql.append("(");
            appendColumns(sql, columns, false);
            sql.append(") (select ");
            appendColumnParameters(sql, columns);
            sql.append(" where (select distinct 1 from ");
            sql.append(tableName);
            sql.append(" where  ");
            if (keyColumns == null || keyColumns.length == 0) {
                sql.append("1 != 1");
            } else {
                appendColumnsEquals(sql, keyColumns, " and ");
            }
            sql.append(") is null)");
            return sql.toString();
        } else {
            return super.buildInsertSql(tableName, keyColumns, columns);
        }
    }

    @Override
    public Column[] getMetaData() {
        if (dmlType == DmlType.INSERT) {
            return getColumnKeyMetaData();
        } else {
            return super.getMetaData();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T[] getValueArray(T[] columnValues, T[] keyValues) {
        if (dmlType == DmlType.INSERT) {
            return (T[]) ArrayUtils.addAll(columnValues, keyValues);
        } else {
            return super.getValueArray(columnValues, keyValues);
        }
    }

    @Override
    public Object[] getValueArray(Map<String, Object> params) {
        Object[] args = null;
        int index = 0;
        if (params != null) {
            if (dmlType == DmlType.INSERT) {
                args = new Object[columns.length + keys.length];
                for (Column column : columns) {
                    args[index++] = params.get(column.getName());
                }
                for (Column column : keys) {
                    args[index++] = params.get(column.getName());
                }
            } else {
                args = super.getValueArray(params);
            }
        }
        return args;
    }

    @Override
    protected int[] buildTypes(Column[] keys, Column[] columns, boolean isDateOverrideToTimestamp) {
        if (dmlType == DmlType.INSERT) {
            int[] columnTypes = buildTypes(columns, isDateOverrideToTimestamp);
            int[] keyTypes = buildTypes(keys, isDateOverrideToTimestamp);
            return ArrayUtils.addAll(columnTypes, keyTypes);
        } else {
            return super.buildTypes(keys, columns, isDateOverrideToTimestamp);
        }
    }

    @Override
    protected void appendColumnParameter(StringBuilder sql, Column column) {
        if (column.isTimestampWithTimezone()) {
            sql.append("cast(? as timestamp with time zone)").append(",");
        } else if (column.getJdbcTypeName() != null && column.getJdbcTypeName().toUpperCase().contains(TypeMap.UUID)) {
            sql.append("cast(? as uuid)").append(",");
        } else if (column.getJdbcTypeName() != null && column.getJdbcTypeName().toUpperCase().contains(TypeMap.VARBIT)) {
            sql.append("cast(? as bit varying)").append(",");
        } else if (column.getJdbcTypeName() != null && column.getJdbcTypeName().toUpperCase().contains(TypeMap.INTERVAL)) {
            sql.append("cast(? as interval)").append(",");
        } else if (column.getJdbcTypeName() != null && (
                column.getJdbcTypeName().toUpperCase().contains(TypeMap.GEOMETRY) ||
                column.getJdbcTypeName().toUpperCase().contains(TypeMap.GEOGRAPHY))) {
            sql.append("ST_GEOMFROMTEXT(?)").append(",");
        } else {
            super.appendColumnParameter(sql, column);
        }
    }
    
    @Override
    protected void appendColumnEquals(StringBuilder sql, Column column) {
        if (column.isTimestampWithTimezone()) {
            sql.append(quote).append(column.getName()).append(quote)
                    .append(" = cast(? as timestamp with time zone)");
        } else if (column.getJdbcTypeName().toUpperCase().contains(TypeMap.UUID)) {
            sql.append(quote).append(column.getName()).append(quote)
                    .append(" = cast(? as uuid)");
        } else if (column.getJdbcTypeName().toUpperCase().contains(TypeMap.VARBIT)) {
            sql.append(quote).append(column.getName()).append(quote)
                    .append(" = cast(? as bit varying)");
        } else if (column.getJdbcTypeName().toUpperCase().contains(TypeMap.INTERVAL)) {
            sql.append(quote).append(column.getName()).append(quote)
                  .append(" = cast(? as interval)");
        } else if (column.getJdbcTypeName().toUpperCase().contains(TypeMap.GEOMETRY) ||
                column.getJdbcTypeName().toUpperCase().contains(TypeMap.GEOGRAPHY)) {
            sql.append(quote).append(column.getName()).append(quote)
            .append(" = ST_GEOMFROMTEXT(?)");
        } else {
            super.appendColumnEquals(sql, column);
        }
    }

    @Override
    protected void appendColumnNameForSql(StringBuilder sql, Column column, boolean select) {
        String columnName = column.getName();
        if (select && column.isTimestampWithTimezone()) {
            sql.append(
            "   case                                                                                                                                 " +
            "   when extract(timezone_hour from ").append(quote).append(columnName).append(quote).append(") < 0 then                                 " +
            "     to_char(").append(quote).append(columnName).append(quote).append(", 'YYYY-MM-DD HH24:MI:SS.US ')||'-'||                            " +
            "     lpad(cast(abs(extract(timezone_hour from ").append(quote).append(columnName).append(quote).append(")) as varchar),2,'0')||':'||    " +
            "     lpad(cast(extract(timezone_minute from ").append(quote).append(columnName).append(quote).append(") as varchar), 2, '0')            " +
            "   else                                                                                                                                 " +
            "     to_char(").append(quote).append(columnName).append(quote).append(", 'YYYY-MM-DD HH24:MI:SS.US ')||'+'||                            " +
            "     lpad(cast(extract(timezone_hour from ").append(quote).append(columnName).append(quote).append(") as varchar),2,'0')||':'||         " +
            "     lpad(cast(extract(timezone_minute from ").append(quote).append(columnName).append(quote).append(") as varchar), 2, '0')            " +
            "   end as ").append(columnName);
        } else {
            super.appendColumnNameForSql(sql, column, select);
        }
    }

}
