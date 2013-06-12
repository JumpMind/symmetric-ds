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
package org.jumpmind.db.platform;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.mysql.MySqlDmlStatement;
import org.jumpmind.db.platform.oracle.OracleDmlStatement;
import org.jumpmind.db.platform.postgresql.PostgreSqlDmlStatement;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;

final public class DmlStatementFactory {

    private DmlStatementFactory() {
    }

    public static DmlStatement createDmlStatement(String databaseName, DmlType dmlType,
            Table table, boolean useQuotedIdentifiers) {
        return createDmlStatement(databaseName, dmlType, table.getCatalog(), table.getSchema(),
                table.getName(), table.getPrimaryKeyColumns(), table.getColumns(), null,
                useQuotedIdentifiers);
    }

    public static DmlStatement createDmlStatement(String databaseName, DmlType dmlType,
            String catalogName, String schemaName, String tableName, Column[] keys,
            Column[] columns, boolean[] nullKeyValues, boolean useQuotedIdentifiers) {
        IDdlBuilder ddlBuilder = DdlBuilderFactory.createDdlBuilder(databaseName);
        ddlBuilder.setDelimitedIdentifierModeOn(useQuotedIdentifiers);
        return createDmlStatement(databaseName, dmlType, catalogName, schemaName, tableName, keys,
                columns, nullKeyValues, ddlBuilder);
    }

    public static DmlStatement createDmlStatement(String databaseName, DmlType dmlType,
            String catalogName, String schemaName, String tableName, Column[] keys,
            Column[] columns, boolean[] nullKeyValues, IDdlBuilder ddlBuilder) {
        if (DatabaseNamesConstants.ORACLE.equals(databaseName)) {
            return new OracleDmlStatement(dmlType, catalogName, schemaName, tableName, keys,
                    columns, nullKeyValues, ddlBuilder.getDatabaseInfo(),
                    ddlBuilder.isDelimitedIdentifierModeOn());
        } else if (DatabaseNamesConstants.POSTGRESQL.equals(databaseName)) {
            return new PostgreSqlDmlStatement(dmlType, catalogName, schemaName, tableName, keys,
                    columns, nullKeyValues, ddlBuilder.getDatabaseInfo(),
                    ddlBuilder.isDelimitedIdentifierModeOn());
        } else if (DatabaseNamesConstants.MYSQL.equals(databaseName)) {
            return new MySqlDmlStatement(dmlType, catalogName, schemaName, tableName, keys,
                    columns, nullKeyValues, ddlBuilder.getDatabaseInfo(),
                    ddlBuilder.isDelimitedIdentifierModeOn());
        } else {
            return new DmlStatement(dmlType, catalogName, schemaName, tableName, keys, columns,
                    nullKeyValues, ddlBuilder.getDatabaseInfo(),
                    ddlBuilder.isDelimitedIdentifierModeOn());
        }

    }

}
