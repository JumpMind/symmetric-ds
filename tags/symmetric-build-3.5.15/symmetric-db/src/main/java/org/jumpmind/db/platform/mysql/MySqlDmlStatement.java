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
package org.jumpmind.db.platform.mysql;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.DmlStatement;

public class MySqlDmlStatement extends DmlStatement {

    public MySqlDmlStatement(DmlType type, String catalogName, String schemaName, String tableName,
            Column[] keysColumns, Column[] columns, boolean[] nullKeyValues, 
            DatabaseInfo databaseInfo, boolean useQuotedIdentifiers) {
        super(type, catalogName, schemaName, tableName, keysColumns, columns, 
                nullKeyValues, databaseInfo, useQuotedIdentifiers);
    }

    @Override
    public void appendColumnQuestions(StringBuilder sql, Column[] columns) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                if (columns[i].getJdbcTypeName() != null && columns[i].getJdbcTypeName().toUpperCase().contains(TypeMap.GEOMETRY)) {
                    sql.append("geomfromtext(?)").append(",");
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
                } else if (columns[i].getJdbcTypeName().toUpperCase().contains(TypeMap.GEOMETRY)) {
                    sql.append(quote).append(columns[i].getName()).append(quote).append(" = ")
                            .append("geomfromtext(?)").append(separator);
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
    
}
