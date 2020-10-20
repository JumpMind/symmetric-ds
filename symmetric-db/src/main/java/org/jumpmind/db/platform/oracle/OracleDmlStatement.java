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

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.DmlStatement;

public class OracleDmlStatement extends DmlStatement {

    public OracleDmlStatement(DmlType type, String catalogName, String schemaName, String tableName,
            Column[] keysColumns, Column[] columns, boolean[] nullKeyValues, 
            DatabaseInfo databaseInfo, boolean useQuotedIdentifiers, String textColumnExpression) {
        super(type, catalogName, schemaName, tableName, keysColumns, columns, 
                nullKeyValues, databaseInfo, useQuotedIdentifiers, textColumnExpression);
    }
   
    @Override
    protected void appendColumnParameter(StringBuilder sql, Column column) {
        if (column.isTimestampWithTimezone()) {
            sql.append("TO_TIMESTAMP_TZ(?, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')")
                    .append(",");
        } else if (isGeometry(column)) {
            sql.append("SYM_WKT2GEOM(?,").append(buildSRIDSelect(column)).append(")").append(",");
        } else if (column.getJdbcTypeName().startsWith("XMLTYPE")) {
            sql.append("XMLTYPE(?)").append(",");
        } else {
            super.appendColumnParameter(sql, column);
        }
    }
    
    @Override
    protected void appendColumnEquals(StringBuilder sql, Column column) {
        if (column.isTimestampWithTimezone()) {
            sql.append(quote).append(column.getName()).append(quote)
                    .append(" = TO_TIMESTAMP_TZ(?, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')");
        } else if (isGeometry(column)) {
            sql.append(quote).append(column.getName()).append(quote).append(" = ")
                    .append("SYM_WKT2GEOM(?,").append(buildSRIDSelect(column)).append(")");
        } else if (column.getJdbcTypeName().startsWith("XMLTYPE")) {
            sql.append(quote).append(column.getName()).append(quote).append(" = ")
                    .append("XMLTYPE(?)");
        } else {
            super.appendColumnEquals(sql, column);
        }        
    }

    @Override
    protected int getTypeCode(Column column, boolean isDateOverrideToTimestamp) {
        int typeCode = super.getTypeCode(column, isDateOverrideToTimestamp);
        if (typeCode == Types.LONGVARCHAR
                || isGeometry(column)
                || column.getJdbcTypeName().startsWith("XMLTYPE")) {
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
    
    protected boolean isGeometry(Column column) {
        String name = column.getJdbcTypeName();
        if (name != null && (
                name.toUpperCase().contains(TypeMap.GEOMETRY) || 
                name.toUpperCase().contains(TypeMap.GEOGRAPHY))) {
            return true;
        } else {
            return false;
        }
    }
    
    protected String buildSRIDSelect(Column column) {
        if (!StringUtils.isEmpty(schemaName)) {
            return String.format("(select SRID from all_sdo_geom_metadata where owner = '%s' and table_name = '%s' and column_name = '%s')", 
                    schemaName.toUpperCase(), tableName.toUpperCase(), column.getName().toUpperCase());
        } else {
            return String.format("(select SRID from user_sdo_geom_metadata where table_name = '%s' and column_name = '%s')", 
                    tableName.toUpperCase(), column.getName().toUpperCase());
        }
    }    
}
