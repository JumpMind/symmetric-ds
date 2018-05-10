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
package org.jumpmind.db.platform.mssql;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class MsSql2005DdlBuilder extends MsSql2000DdlBuilder {
    
    public MsSql2005DdlBuilder() {
        this.databaseName = DatabaseNamesConstants.MSSQL2005;
    }

    protected void dropDefaultConstraint(String tableName, String columnName, StringBuilder ddl) {         
        println(              "BEGIN                                                                                        ", ddl);
        println(              "DECLARE @sql NVARCHAR(2000)                                                                  ", ddl);        
        println(String.format("SELECT TOP 1 @sql = N'alter table \"%s\" drop constraint ['+dc.NAME+N']'                     ", tableName), ddl);
        println(              "FROM sys.default_constraints dc                                                              ", ddl);
        println(              "JOIN sys.columns c                                                                           ", ddl);
        println(              "    ON c.default_object_id = dc.object_id                                                    ", ddl);
        println(              "WHERE                                                                                        ", ddl);
        println(String.format("    dc.parent_object_id = OBJECT_ID('%s')                                                    ", tableName), ddl);
        println(String.format("AND c.name = N'%s'                                                                           ", columnName), ddl);
        println(              "IF @@ROWCOUNT > 0                                                                            ", ddl);
        println(              "  EXEC (@sql)                                                                                ", ddl);
        println(              "END                                                                                          ", ddl);
        printEndOfStatement(ddl);        
    }
    
    protected void dropColumnChangeDefaults(Table sourceTable, Column sourceColumn, StringBuilder ddl) {
    	// we're dropping the old default
	    String tableName = getTableName(sourceTable.getName());
	    String columnName = getColumnName(sourceColumn);
	    String tableNameVar = "tn" + createUniqueIdentifier();
	    String constraintNameVar = "cn" + createUniqueIdentifier();
	
	    println("BEGIN", ddl);
	    println("  DECLARE @" + tableNameVar + " nvarchar(256), @" + constraintNameVar
	            + " nvarchar(256)", ddl);
	    println("  DECLARE refcursor CURSOR FOR", ddl);
	    println("  SELECT object_name(cons.parent_object_id) tablename, cons.name constraintname FROM sys.default_constraints cons ",
	            ddl);
	    println("    WHERE  cons.parent_column_id = (SELECT colid FROM syscolumns WHERE id = object_id(", ddl);
	    printAlwaysSingleQuotedIdentifier(tableName, ddl);
	    ddl.append(") AND name = ");
	    printAlwaysSingleQuotedIdentifier(columnName, ddl);
	    println(") AND", ddl);
	    ddl.append("          object_name(cons.parent_object_id) = ");
	    printAlwaysSingleQuotedIdentifier(tableName, ddl);
	    println("  OPEN refcursor", ddl);
	    println("  FETCH NEXT FROM refcursor INTO @" + tableNameVar + ", @" + constraintNameVar,
	            ddl);
	    println("  WHILE @@FETCH_STATUS = 0", ddl);
	    println("    BEGIN", ddl);
	    println("      EXEC ('ALTER TABLE '+@" + tableNameVar + "+' DROP CONSTRAINT '+@"
	            + constraintNameVar + ")", ddl);
	    println("      FETCH NEXT FROM refcursor INTO @" + tableNameVar + ", @"
	            + constraintNameVar, ddl);
	    println("    END", ddl);
	    println("  CLOSE refcursor", ddl);
	    println("  DEALLOCATE refcursor", ddl);
	    ddl.append("END");
	    printEndOfStatement(ddl);
    }

}
