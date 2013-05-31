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
package org.jumpmind.db.platform.oracle;

import java.sql.Types;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.sql.DmlStatement;

public class OracleDmlStatement extends DmlStatement {

    public OracleDmlStatement(DmlType type, String catalogName, String schemaName,
            String tableName, Column[] keys, Column[] columns, boolean isDateOverrideToTimestamp,
            String identifierQuoteString, boolean[] nullKeyValues) {
        super(type, catalogName, schemaName, tableName, keys, columns, isDateOverrideToTimestamp,
                identifierQuoteString, nullKeyValues);
    }

    @Override
    public void appendColumnQuestions(StringBuilder sql, Column[] columns) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                if (columns[i].getMappedTypeCode() == -101) {
                    sql.append("TO_TIMESTAMP_TZ(?, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')")
                            .append(",");
                } else if (columns[i].getJdbcTypeName() != null && columns[i].getJdbcTypeName().toUpperCase().contains(TypeMap.GEOMETRY)) {
                    sql.append("SYM_WKT2GEOM(?)").append(",");
                } else {
                    sql.append("?").append(",");
                }
            }
        }

        if (columns.length > 0) {
            sql.replace(sql.length() - 1, sql.length(), "");
        }
    }

    @Override
    public void appendColumnEquals(StringBuilder sql, Column[] columns, boolean[] nullValues, String separator) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                if (nullValues[i]) {
                    sql.append(quote).append(columns[i].getName()).append(quote).append(" is NULL")
                            .append(separator);
                } else if (columns[i].getMappedTypeCode() == -101) {
                    sql.append(quote).append(columns[i].getName()).append(quote)
                            .append(" = TO_TIMESTAMP_TZ(?, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')")
                            .append(separator);
                } else if (columns[i].getJdbcTypeName().toUpperCase().contains(TypeMap.GEOMETRY)) {
                    sql.append(quote).append(columns[i].getName()).append(quote).append(" = ")
                            .append("SYM_WKT2GEOM(?)").append(separator);
                } else {
                    sql.append(quote).append(columns[i].getName()).append(quote).append(" = ?")
                            .append(separator);
                }
            }
        }

        if (columns.length > 0) {
            sql.replace(sql.length() - separator.length(), sql.length(), "");
        }
    }
    
    @Override
    protected int getTypeCode(Column column, boolean isDateOverrideToTimestamp) {
        int typeCode = super.getTypeCode(column, isDateOverrideToTimestamp);
        if (typeCode == Types.LONGVARCHAR) {
            typeCode = Types.CLOB;
        } 
        return typeCode;
    }
    
    @Override
    protected void appendColumnNameForSql(StringBuilder sql, Column column, boolean select) {
        String columnName = column.getName();
        if (select && column.isTimestampWithTimezone()) {
            sql.append("to_char(").append(quote).append(columnName).append(quote).append(", 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM') as ").append(columnName);
        } else {
            super.appendColumnNameForSql(sql, column, select);
        }        
    }


}
