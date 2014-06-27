package org.jumpmind.db.platform.firebird;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.sql.Connection;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;

import org.jumpmind.db.alter.AddColumnChange;
import org.jumpmind.db.alter.AddPrimaryKeyChange;
import org.jumpmind.db.alter.ColumnDataTypeChange;
import org.jumpmind.db.alter.RemoveColumnChange;
import org.jumpmind.db.alter.TableChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractDdlBuilder;
import org.jumpmind.db.platform.PlatformUtils;

/*
 * The SQL Builder for the FireBird database.
 */
public class FirebirdDdlBuilder extends AbstractDdlBuilder {

    public FirebirdDdlBuilder() {
        databaseInfo.setMaxIdentifierLength(31);
        databaseInfo.setRequiresAutoCommitForDdl(true);
        databaseInfo.setSystemForeignKeyIndicesAlwaysNonUnique(true);
        databaseInfo.setCommentPrefix("/*");
        databaseInfo.setCommentSuffix("*/");

        databaseInfo.addNativeTypeMapping(Types.ARRAY, "BLOB", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.BINARY, "BLOB", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.BIT, "SMALLINT", Types.SMALLINT);
        databaseInfo.addNativeTypeMapping(Types.CLOB, "BLOB SUB_TYPE TEXT", Types.LONGVARCHAR);
        databaseInfo.addNativeTypeMapping(Types.DISTINCT, "BLOB", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.BLOB, "BLOB", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.DOUBLE, "DOUBLE PRECISION");
        databaseInfo.addNativeTypeMapping(Types.FLOAT, "DOUBLE PRECISION", Types.DOUBLE);
        databaseInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "BLOB", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.LONGVARBINARY, "BLOB", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.LONGVARCHAR, "BLOB SUB_TYPE TEXT");
        databaseInfo.addNativeTypeMapping(Types.NULL, "BLOB", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.OTHER, "BLOB", Types.LONGVARBINARY);
        // This is back-mapped to REAL in the model reader
        databaseInfo.addNativeTypeMapping(Types.REAL, "FLOAT");
        databaseInfo.addNativeTypeMapping(Types.REF, "BLOB", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.STRUCT, "BLOB", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);
        databaseInfo.addNativeTypeMapping(Types.VARBINARY, "BLOB", Types.LONGVARBINARY);

        databaseInfo.addNativeTypeMapping("BOOLEAN", "SMALLINT", "SMALLINT");
        databaseInfo.addNativeTypeMapping("DATALINK", "BLOB", "LONGVARBINARY");

        databaseInfo.setDefaultSize(Types.VARCHAR, 254);
        databaseInfo.setDefaultSize(Types.CHAR, 254);

        databaseInfo.setNonBlankCharColumnSpacePadded(true);
        databaseInfo.setBlankCharColumnSpacePadded(true);
        databaseInfo.setCharColumnSpaceTrimmed(false);
        databaseInfo.setEmptyStringNulled(false);

        databaseInfo.setMinIsolationLevelToPreventPhantomReads(Connection.TRANSACTION_REPEATABLE_READ);
        
        addEscapedCharSequence("'", "''");
    }

    @Override
    protected void createTable(Table table, StringBuilder ddl, boolean temporary, boolean recreate) {
        super.createTable(table, ddl, temporary, recreate);

        if (!temporary) {
            // creating generator and trigger for auto-increment
            Column[] columns = table.getAutoIncrementColumns();

            for (int idx = 0; idx < columns.length; idx++) {
                writeAutoIncrementCreateStmts(table, columns[idx], ddl);
            }
        }
    }

    @Override
    protected void dropTable(Table table, StringBuilder ddl, boolean temporary, boolean recreate) {
        // dropping generators for auto-increment
        Column[] columns = table.getAutoIncrementColumns();

        if (!temporary && !recreate) {
            for (int idx = 0; idx < columns.length; idx++) {
                writeAutoIncrementDropStmts(table, columns[idx], ddl);
            }
        }
        super.dropTable(table, ddl, temporary, recreate);
    }

    /*
     * Writes the creation statements to make the given column an auto-increment
     * column.
     */
    private void writeAutoIncrementCreateStmts(Table table, Column column, StringBuilder ddl) {
        ddl.append("CREATE GENERATOR ");
        printIdentifier(getGeneratorName(table, column), ddl);
        printEndOfStatement(ddl);

        ddl.append("CREATE TRIGGER ");
        printIdentifier(getTriggerName(table, column), ddl);
        ddl.append(" FOR ");
        printlnIdentifier(getTableName(table.getName()), ddl);
        println("ACTIVE BEFORE INSERT POSITION 0 AS", ddl);
        ddl.append("BEGIN IF (NEW.");
        printIdentifier(getColumnName(column), ddl);
        ddl.append(" IS NULL) THEN NEW.");
        printIdentifier(getColumnName(column), ddl);
        ddl.append(" = GEN_ID(");
        printIdentifier(getGeneratorName(table, column), ddl);
        ddl.append(", 1); END");
        printEndOfStatement(ddl);
    }

    /*
     * Writes the statements to drop the auto-increment status for the given
     * column.
     * 
     * @param table The table
     * 
     * @param column The column to remove the auto-increment status for
     */
    private void writeAutoIncrementDropStmts(Table table, Column column, StringBuilder ddl) {
        ddl.append("DROP TRIGGER ");
        printIdentifier(getTriggerName(table, column), ddl);
        printEndOfStatement(ddl);

        ddl.append("DROP GENERATOR ");
        printIdentifier(getGeneratorName(table, column), ddl);
        printEndOfStatement(ddl);
    }

    /*
     * Determines the name of the trigger for an auto-increment column.
     * 
     * @param table The table
     * 
     * @param column The auto-increment column
     * 
     * @return The trigger name
     */
    protected String getTriggerName(Table table, Column column) {
        String secondPart = column.getName();
        // make sure a backup table gets a different name than the original
        if (table.getName().endsWith("_")) {
            secondPart += "_";
        }
        return getConstraintName("TRG", table, secondPart, null);
    }

    /*
     * Determines the name of the generator for an auto-increment column.
     * 
     * @param table The table
     * 
     * @param column The auto-increment column
     * 
     * @return The generator name
     */
    protected String getGeneratorName(Table table, Column column) {
        String secondPart = column.getName();
        // make sure a backup table gets a different name than the original
        if (table.getName().endsWith("_")) {
            secondPart += "_";
        }
        return getConstraintName("GEN", table, secondPart, null);
    }

    @Override
    protected void writeColumnAutoIncrementStmt(Table table, Column column, StringBuilder ddl) {
        // we're using a generator
    }

    @Override
    public String getSelectLastIdentityValues(Table table) {
        Column[] columns = table.getAutoIncrementColumns();

        if (columns.length == 0) {
            return null;
        } else {
            StringBuffer result = new StringBuffer();

            result.append("SELECT ");
            for (int idx = 0; idx < columns.length; idx++) {
                result.append("GEN_ID(");
                result.append(getDelimitedIdentifier(getGeneratorName(table, columns[idx])));
                result.append(", 0)");
            }
            result.append(" FROM RDB$DATABASE");
            return result.toString();
        }
    }

    @Override
    protected String getNativeDefaultValue(Column column) {
        if ((column.getMappedTypeCode() == Types.BIT)
                || (PlatformUtils.supportsJava14JdbcTypes() && (column.getMappedTypeCode() == PlatformUtils
                        .determineBooleanTypeCode()))) {
            return getDefaultValueHelper().convert(column.getDefaultValue(), column.getMappedTypeCode(),
                    Types.SMALLINT).toString();
        } else {
            return super.getNativeDefaultValue(column);
        }
    }

    @Override
    public void createExternalForeignKeys(Database database, StringBuilder ddl) {
        for (int idx = 0; idx < database.getTableCount(); idx++) {
            createExternalForeignKeys(database, database.getTable(idx), ddl);
        }
    }

    @Override
    public void writeExternalIndexDropStmt(Table table, IIndex index, StringBuilder ddl) {
        // Index names in Firebird are unique to a schema and hence Firebird
        // does not
        // use the ON <tablename> clause
        ddl.append("DROP INDEX ");
        printIdentifier(getIndexName(index), ddl);
        printEndOfStatement(ddl);
    }

    @Override
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            Table sourceTable, Table targetTable, List<TableChange> changes, StringBuilder ddl) {
        // TODO: Dropping of primary keys is currently not supported because we
        // cannot determine the pk constraint names and drop them in one go
        // (We could used a stored procedure if Firebird would allow them to use
        // DDL) This will be easier once named primary keys are supported
        boolean pkColumnAdded = false;

        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();

            if (change instanceof AddColumnChange) {
                AddColumnChange addColumnChange = (AddColumnChange) change;

                // TODO: we cannot add columns to the primary key this way
                // because we would have to drop the pk first and then
                // add a new one afterwards which is not supported yet
                if (addColumnChange.getNewColumn().isPrimaryKey()) {
                    pkColumnAdded = true;
                } else {
                    processChange(currentModel, desiredModel, addColumnChange, ddl);
                    changeIt.remove();
                }
            } else if (change instanceof RemoveColumnChange) {
                RemoveColumnChange removeColumnChange = (RemoveColumnChange) change;

                // TODO: we cannot drop primary key columns this way
                // because we would have to drop the pk first and then
                // add a new one afterwards which is not supported yet
                if (!removeColumnChange.getColumn().isPrimaryKey()) {
                    processChange(currentModel, desiredModel, removeColumnChange, ddl);
                    changeIt.remove();
                }
            }
        }
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();

            // we can only add a primary key if all columns are present in the
            // table
            // i.e. none was added during this alteration
            if ((change instanceof AddPrimaryKeyChange) && !pkColumnAdded) {
                processChange(currentModel, desiredModel, (AddPrimaryKeyChange) change, ddl);
                changeIt.remove();
            }
        }
        
        super.processTableStructureChanges(currentModel, desiredModel,
                sourceTable, targetTable, changes, ddl);
    }
    
    @Override
    protected boolean writeAlterColumnDataType(ColumnDataTypeChange change, StringBuilder ddl) {
        Table table = change.getChangedTable();
        Column column = change.getChangedColumn();
        if (column.isPrimaryKey()) {
            writeTableAlterStmt(change.getChangedTable(), ddl);
            ddl.append(" DROP CONSTRAINT ");
            ddl.append(table.getPrimaryKeyConstraintName());
            printEndOfStatement(ddl);
        }
        
        writeTableAlterStmt(change.getChangedTable(), ddl);
        ddl.append(" ALTER COLUMN ");  
        column.setTypeCode(change.getNewTypeCode());
        printIdentifier(getColumnName(column), ddl);
        ddl.append(" TYPE ");
        ddl.append(getSqlType(column));
        printEndOfStatement(ddl);
        
        if (column.isPrimaryKey()) {
            writeTableAlterStmt(change.getChangedTable(), ddl);
            ddl.append(" ADD ");
            writePrimaryKeyStmt(table, table.getPrimaryKeyColumns(), ddl);
            printEndOfStatement(ddl);
        }
        
        return true;
    }    

    /*
     * Processes the addition of a column to a table.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            AddColumnChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
        printIndent(ddl);
        ddl.append("ADD ");
        writeColumn(change.getChangedTable(), change.getNewColumn(), ddl);
        printEndOfStatement(ddl);

        Table curTable = currentModel.findTable(change.getChangedTable().getName(),
                delimitedIdentifierModeOn);

        if (!change.isAtEnd()) {
            Column prevColumn = change.getPreviousColumn();

            if (prevColumn != null) {
                // we need the corresponding column object from the current
                // table
                prevColumn = curTable.findColumn(prevColumn.getName(), delimitedIdentifierModeOn);
            }
            // Even though Firebird can only add columns, we can move them later
            // on
            ddl.append("ALTER TABLE ");
            printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
            printIndent(ddl);
            ddl.append("ALTER ");
            printIdentifier(getColumnName(change.getNewColumn()), ddl);
            ddl.append(" POSITION ");
            // column positions start at 1 in Firebird
            ddl.append(prevColumn == null ? "1" : String.valueOf(curTable
                    .getColumnIndex(prevColumn) + 2));
            printEndOfStatement(ddl);
        }
        if (change.getNewColumn().isAutoIncrement()) {
            writeAutoIncrementCreateStmts(curTable, change.getNewColumn(), ddl);
        }
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    /*
     * Processes the removal of a column from a table.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            RemoveColumnChange change, StringBuilder ddl) {
        if (change.getColumn().isAutoIncrement()) {
            writeAutoIncrementDropStmts(change.getChangedTable(), change.getColumn(), ddl);
        }
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
        printIndent(ddl);
        ddl.append("DROP ");
        printIdentifier(getColumnName(change.getColumn()), ddl);
        printEndOfStatement(ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }
}
