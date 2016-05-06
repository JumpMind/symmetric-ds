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
package org.jumpmind.db.platform.voltdb;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.IndexColumn;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractDdlBuilder;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IAlterDatabaseInterceptor;

public class VoltDbDdlBuilder extends AbstractDdlBuilder {

    public VoltDbDdlBuilder() {
        super(DatabaseNamesConstants.VOLTDB);
        
        // this is the default length though it might be changed when building
        // PostgreSQL
        // in file src/include/postgres_ext.h
        databaseInfo.setMaxIdentifierLength(-1);

//        databaseInfo.setRequiresSavePointsInTransaction(true);
//        databaseInfo.setRequiresAutoCommitForDdl(true);
        
        databaseInfo.addNativeTypeMapping(Types.CLOB, "VARCHAR", Types.VARCHAR);
        databaseInfo.addNativeTypeMapping(Types.LONGVARCHAR, "VARCHAR", Types.VARCHAR);
        databaseInfo.addNativeTypeMapping(Types.CHAR, "VARCHAR", Types.VARCHAR);

//        databaseInfo.addNativeTypeMapping(Types.ARRAY, "BYTEA", Types.LONGVARBINARY);
//        databaseInfo.addNativeTypeMapping(Types.BINARY, "BYTEA", Types.LONGVARBINARY);
//        databaseInfo.addNativeTypeMapping(Types.BIT, "BOOLEAN");
//        databaseInfo.addNativeTypeMapping(Types.BLOB, "BYTEA", Types.LONGVARBINARY);
//        databaseInfo.addNativeTypeMapping(Types.CLOB, "TEXT", Types.LONGVARCHAR);
//        databaseInfo.addNativeTypeMapping(Types.DECIMAL, "NUMERIC", Types.NUMERIC);
//        databaseInfo.addNativeTypeMapping(Types.DISTINCT, "BYTEA", Types.LONGVARBINARY);
//        databaseInfo.addNativeTypeMapping(Types.DOUBLE, "DOUBLE PRECISION");
//        databaseInfo.addNativeTypeMapping(Types.FLOAT, "DOUBLE P    RECISION", Types.DOUBLE);
//        databaseInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "BYTEA", Types.LONGVARBINARY);
//        databaseInfo.addNativeTypeMapping(Types.LONGVARBINARY, "BYTEA");
//        databaseInfo.addNativeTypeMapping(Types.LONGVARCHAR, "TEXT", Types.LONGVARCHAR);
//        databaseInfo.addNativeTypeMapping(Types.NULL, "BYTEA", Types.LONGVARBINARY);
//        databaseInfo.addNativeTypeMapping(Types.OTHER, "BYTEA", Types.LONGVARBINARY);
//        databaseInfo.addNativeTypeMapping(Types.REF, "BYTEA", Types.LONGVARBINARY);
//        databaseInfo.addNativeTypeMapping(Types.STRUCT, "BYTEA", Types.LONGVARBINARY);
//        databaseInfo.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);
//        databaseInfo.addNativeTypeMapping(Types.VARBINARY, "BYTEA", Types.LONGVARBINARY);
//        databaseInfo.addNativeTypeMapping("BOOLEAN", "BOOLEAN", "BIT");
//        databaseInfo.addNativeTypeMapping("DATALINK", "BYTEA", "LONGVARBINARY");
//        databaseInfo.addNativeTypeMapping(ColumnTypes.NVARCHAR, "VARCHAR", Types.VARCHAR);
//        databaseInfo.addNativeTypeMapping(ColumnTypes.LONGNVARCHAR, "VARCHAR", Types.VARCHAR);
//        databaseInfo.addNativeTypeMapping(ColumnTypes.NCHAR, "CHAR", Types.CHAR);
        
        databaseInfo.setDefaultSize(Types.CHAR, 254);
        databaseInfo.setDefaultSize(Types.VARCHAR, 254);

//        // no support for specifying the size for these types (because they are
//        // mapped to BYTEA which back-maps to BLOB)
//        databaseInfo.setHasSize(Types.BINARY, false);
//        databaseInfo.setHasSize(Types.VARBINARY, false);

//        databaseInfo.setNonBlankCharColumnSpacePadded(true);
//        databaseInfo.setBlankCharColumnSpacePadded(true);
//        databaseInfo.setCharColumnSpaceTrimmed(false);
//        databaseInfo.setEmptyStringNulled(false);
        // we need to handle the backslash first otherwise the other
        // already escaped sequences would be affected
//        addEscapedCharSequence("\\", "\\\\");
//        addEscapedCharSequence("\b", "\\b");
//        addEscapedCharSequence("\f", "\\f");
//        addEscapedCharSequence("\n", "\\n");
//        addEscapedCharSequence("\r", "\\r");
//        addEscapedCharSequence("\t", "\\t");
    }
    
    @Override
    protected void writeColumnAutoIncrementStmt(Table table, Column column, StringBuilder ddl) {
        // ddl.append("GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1, NO CACHE, ORDER)");
    }
    
    @Override
    protected void createTable(Table table, StringBuilder ddl, boolean temporary, boolean recreate) {
        // VoltDB doesn't support auto increment.
       // if (table.getPrimaryKeyColumnCount() > 0 && table.hasAutoIncrementColumn()) {
            for (Column column : table.getColumns()) {
                column.setAutoIncrement(false);
            } 
    //    }
        super.createTable(table, ddl, temporary, recreate);
    }    
    
    public boolean isAlterDatabase(Database currentModel, Database desiredModel, IAlterDatabaseInterceptor... alterDatabaseInterceptors) {
        alignPrimaryKeys(currentModel, desiredModel);
        alignIndicies(currentModel, desiredModel);
        return super.isAlterDatabase(currentModel, desiredModel, alterDatabaseInterceptors);
    }

    protected void alignPrimaryKeys(Database currentModel, Database desiredModel) {
        for (Table desiredTable : desiredModel.getTables()) {
            if (desiredTable.getPrimaryKeyColumnCount() > 1) {
                Table currentTable = currentModel.findTable(desiredTable.getName());
                if (currentTable != null) {
                    alignPrimaryKeys(currentTable, desiredTable);
                }
            }
        }
    }

    protected void alignPrimaryKeys(Table currentTable, Table desiredTable) {
        List<Column> currentColumns = new ArrayList<Column>(Arrays.asList(currentTable.getColumns()));      
        currentTable.removeAllColumns();
        for (Column desiredPkColumn : desiredTable.getPrimaryKeyColumns()) {
            for (int i = 0; i < currentColumns.size(); i++) {
                Column currentColumn = currentColumns.get(i);
                if (currentColumn.isPrimaryKey() 
                        && currentColumn.getName().equalsIgnoreCase(desiredPkColumn.getName())) {
                    currentTable.addColumn(currentColumn);
                    currentColumns.remove(i);
                    i--;
                }
            }
        }
        
        for (Column currentColumn : currentColumns) {
            currentTable.addColumn(currentColumn);
        }
    }
    
    private void alignIndicies(Database currentModel, Database desiredModel) {
        for (Table desiredTable : desiredModel.getTables()) {
            for (IIndex desiredIndex : desiredTable.getIndices()) {
                if (desiredIndex.getColumnCount() > 1) {
                    Table currentTable = currentModel.findTable(desiredTable.getName());
                    if (currentTable != null) {
                        IIndex currentIndex = currentTable.findIndex(desiredIndex.getName());
                        if (currentIndex != null) {
                            alignIndexColumns(currentIndex, desiredIndex);
                        }
                    }
                }
            }
        }
    }

    protected void alignIndexColumns(IIndex currentIndex, IIndex desiredIndex) {
        List<IndexColumn> currentColumns = new ArrayList<IndexColumn>(Arrays.asList(currentIndex.getColumns()));
        while (currentIndex.getColumnCount() > 0) {
            currentIndex.removeColumn(0);
        }
        
        int oridinalPosition = 1;
        
        for (IndexColumn desiredColumn : desiredIndex.getColumns()) {
            for (int i = 0; i < currentColumns.size(); i++) {
                IndexColumn currentColumn = currentColumns.get(i);
                if (desiredColumn.getName().equalsIgnoreCase(currentColumn.getName())) {
                    currentColumn.setOrdinalPosition(oridinalPosition++);
                    currentIndex.addColumn(currentColumn);
                    currentColumns.remove(i);
                    i--;
                }
            }
        }
        
        for (IndexColumn currentColumn : currentColumns) {
            currentIndex.addColumn(currentColumn);
        }
    }    

}
