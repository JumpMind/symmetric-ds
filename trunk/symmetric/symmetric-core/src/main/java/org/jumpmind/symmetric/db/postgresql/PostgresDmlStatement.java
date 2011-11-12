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
package org.jumpmind.symmetric.db.postgresql;

import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.symmetric.db.ddl.model.Column;
import org.jumpmind.symmetric.db.sql.DmlStatement;

public class PostgresDmlStatement extends DmlStatement {

    public PostgresDmlStatement(DmlType type, String catalogName, String schemaName, String tableName, Column[] keys,
            Column[] columns, Column[] preFilteredColumns, boolean isDateOverrideToTimestamp,
            String identifierQuoteString) {
        super(type, catalogName, schemaName, tableName, keys, columns, preFilteredColumns, isDateOverrideToTimestamp,
                identifierQuoteString);
    }

    @Override
    public String buildInsertSql(String tableName, Column[] keyColumns, Column[] columns) {
        if (keyColumns != null && keyColumns.length > 0 && keyColumns[0] != null) {
            StringBuilder sql = new StringBuilder("insert into ");
            sql.append(tableName);
            sql.append("(");
            int columnCount = appendColumns(sql, columns);
            sql.append(") (select ");
            appendColumnQuestions(sql, columnCount);
            sql.append(" where (select 1 from ");
            sql.append(tableName);
            sql.append(" where  ");
            if (keyColumns == null || keyColumns.length == 0) {
                sql.append("1 != 1");
            } else {
                appendColumnEquals(sql, keyColumns, " and ");
            }
            sql.append(") is null)");
            return sql.toString();
        } else {
            return super.buildInsertSql(tableName, keys, columns);
        }
    }

    @Override
    public Column[] getMetaData(boolean prefiltered) {
        if (dmlType == DmlType.INSERT) {
            return getColumnKeyMetaData(prefiltered);
        } else {
            return super.getMetaData(prefiltered);
        }
    }

    @Override
    public String[] getValueArray(String[] columnValues, String[] keyValues) {
        if (dmlType == DmlType.INSERT) {
            return (String[]) ArrayUtils.addAll(columnValues, keyValues);
        } else {
            return super.getValueArray(columnValues, keyValues);
        }
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

}
