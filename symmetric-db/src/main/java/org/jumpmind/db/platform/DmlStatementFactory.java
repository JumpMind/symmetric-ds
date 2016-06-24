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
import org.jumpmind.db.platform.db2.Db2zOsDmlStatement;
import org.jumpmind.db.platform.mssql.MsSqlDmlStatement;
import org.jumpmind.db.platform.mysql.MySqlDmlStatement;
import org.jumpmind.db.platform.oracle.OracleDmlStatement;
import org.jumpmind.db.platform.postgresql.PostgreSqlDmlStatement;
import org.jumpmind.db.platform.redshift.RedshiftDmlStatement;
import org.jumpmind.db.platform.sqlanywhere.SqlAnywhereDmlStatement;
import org.jumpmind.db.platform.sqlite.SqliteDmlStatement;
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
        if (ddlBuilder == null) {
            throw new RuntimeException(
                    "Unable to create DML statements for unknown dialect: " + databaseName);
        } else {
        	ddlBuilder.setDelimitedIdentifierModeOn(useQuotedIdentifiers);
            return createDmlStatement(databaseName, dmlType, catalogName, schemaName, tableName, keys,
                    columns, nullKeyValues, ddlBuilder, null);        	
        }
    }

    public static DmlStatement createDmlStatement(String databaseName, DmlType dmlType,
            String catalogName, String schemaName, String tableName, Column[] keys,
            Column[] columns, boolean[] nullKeyValues, IDdlBuilder ddlBuilder, String textColumnExpression) {
        if (DatabaseNamesConstants.ORACLE.equals(databaseName)) {
            return new OracleDmlStatement(dmlType, catalogName, schemaName, tableName, keys,
                    columns, nullKeyValues, ddlBuilder.getDatabaseInfo(),
                    ddlBuilder.isDelimitedIdentifierModeOn(), textColumnExpression);
        } else if (DatabaseNamesConstants.POSTGRESQL.equals(databaseName)) {
            return new PostgreSqlDmlStatement(dmlType, catalogName, schemaName, tableName, keys,
                    columns, nullKeyValues, ddlBuilder.getDatabaseInfo(),
                    ddlBuilder.isDelimitedIdentifierModeOn(), textColumnExpression);
        } else if (DatabaseNamesConstants.REDSHIFT.equals(databaseName)) {
            return new RedshiftDmlStatement(dmlType, catalogName, schemaName, tableName, keys,
                    columns, nullKeyValues, ddlBuilder.getDatabaseInfo(),
                    ddlBuilder.isDelimitedIdentifierModeOn(), textColumnExpression);
        } else if (DatabaseNamesConstants.MYSQL.equals(databaseName)) {
            return new MySqlDmlStatement(dmlType, catalogName, schemaName, tableName, keys,
                    columns, nullKeyValues, ddlBuilder.getDatabaseInfo(),
                    ddlBuilder.isDelimitedIdentifierModeOn(), textColumnExpression);
        } else if (DatabaseNamesConstants.SQLITE.equals(databaseName)) {
            return new SqliteDmlStatement(dmlType, catalogName, schemaName, tableName, keys,
                    columns, nullKeyValues, ddlBuilder.getDatabaseInfo(),
                    ddlBuilder.isDelimitedIdentifierModeOn(), textColumnExpression);
        } else if (DatabaseNamesConstants.SQLANYWHERE.equals(databaseName)) {
            return new SqlAnywhereDmlStatement(dmlType, catalogName, schemaName, tableName, keys, columns,
                    nullKeyValues, ddlBuilder.getDatabaseInfo(),
                    ddlBuilder.isDelimitedIdentifierModeOn(), textColumnExpression);
        } else if (DatabaseNamesConstants.DB2ZOS.equals(databaseName)) {
            return new Db2zOsDmlStatement(dmlType, catalogName, schemaName, tableName, keys, columns,
                    nullKeyValues, ddlBuilder.getDatabaseInfo(),
                    ddlBuilder.isDelimitedIdentifierModeOn(), textColumnExpression);
        } else if (databaseName.startsWith("mssql")) {
            return new MsSqlDmlStatement(dmlType, catalogName, schemaName, tableName, keys, columns,
                    nullKeyValues, ddlBuilder.getDatabaseInfo(),
                    ddlBuilder.isDelimitedIdentifierModeOn(), textColumnExpression);
        } else {
            return new DmlStatement(dmlType, catalogName, schemaName, tableName, keys, columns,
                    nullKeyValues, ddlBuilder.getDatabaseInfo(),
                    ddlBuilder.isDelimitedIdentifierModeOn(), textColumnExpression);
        }
    }
    
    public static DmlStatement createDmlStatement (String databaseName, DmlType dmlType,
            String catalogName, String schemaName, String tableName, Column[] keys,
            Column[] columns, boolean[] nullKeyValues, IDdlBuilder ddlBuilder, String textColumnExpression,
            boolean namedParameters) {
        
        return new DmlStatement(dmlType, catalogName, schemaName, tableName, keys, columns,
                nullKeyValues, ddlBuilder.getDatabaseInfo(),
                ddlBuilder.isDelimitedIdentifierModeOn(), textColumnExpression,
                namedParameters);
        
    }

}
