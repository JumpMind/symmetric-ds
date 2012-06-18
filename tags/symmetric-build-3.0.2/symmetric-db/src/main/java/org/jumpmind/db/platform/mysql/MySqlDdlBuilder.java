package org.jumpmind.db.platform.mysql;

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

import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.set.ListOrderedSet;
import org.jumpmind.db.alter.AddColumnChange;
import org.jumpmind.db.alter.AddPrimaryKeyChange;
import org.jumpmind.db.alter.ColumnChange;
import org.jumpmind.db.alter.PrimaryKeyChange;
import org.jumpmind.db.alter.RemoveColumnChange;
import org.jumpmind.db.alter.RemovePrimaryKeyChange;
import org.jumpmind.db.alter.TableChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractDdlBuilder;

/*
 * The SQL Builder for MySQL.
 */
public class MySqlDdlBuilder extends AbstractDdlBuilder {

    public MySqlDdlBuilder() {
        
        databaseInfo.setSystemForeignKeyIndicesAlwaysNonUnique(true);
        databaseInfo.setMaxIdentifierLength(64);
        databaseInfo.setNullAsDefaultValueRequired(true);
        databaseInfo.setDefaultValuesForLongTypesSupported(false);
        // see
        // http://dev.mysql.com/doc/refman/4.1/en/example-auto-increment.html
        databaseInfo.setNonPKIdentityColumnsSupported(false);
        // MySql returns synthetic default values for pk columns
        databaseInfo.setSyntheticDefaultValueForRequiredReturned(true);
        databaseInfo.setCommentPrefix("#");
        // Double quotes are only allowed for delimiting identifiers if the
        // server SQL mode includes ANSI_QUOTES
        databaseInfo.setDelimiterToken("`");

        databaseInfo.addNativeTypeMapping(Types.ARRAY, "LONGBLOB", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.BIT, "TINYINT(1)");
        databaseInfo.addNativeTypeMapping(Types.BLOB, "LONGBLOB", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.CLOB, "LONGTEXT", Types.LONGVARCHAR);
        databaseInfo.addNativeTypeMapping(Types.DISTINCT, "LONGBLOB", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.FLOAT, "DOUBLE", Types.DOUBLE);
        databaseInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "LONGBLOB", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.LONGVARBINARY, "MEDIUMBLOB");
        databaseInfo.addNativeTypeMapping(Types.LONGVARCHAR, "MEDIUMTEXT");
        databaseInfo.addNativeTypeMapping(Types.NULL, "MEDIUMBLOB", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.NUMERIC, "DECIMAL", Types.DECIMAL);
        databaseInfo.addNativeTypeMapping(Types.OTHER, "LONGBLOB", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.REAL, "FLOAT");
        databaseInfo.addNativeTypeMapping(Types.REF, "MEDIUMBLOB", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.STRUCT, "LONGBLOB", Types.LONGVARBINARY);
        // Since TIMESTAMP is not a stable datatype yet, and does not support a
        // higher precision
        // than DATETIME (year to seconds) as of MySQL 5, we map the JDBC type
        // here to DATETIME
        // TODO: Make this configurable
        databaseInfo.addNativeTypeMapping(Types.TIMESTAMP, "DATETIME");
        // In MySql, TINYINT has only a range of -128 to 127
        databaseInfo.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);
        databaseInfo.addNativeTypeMapping("BOOLEAN", "TINYINT(1)", "BIT");
        databaseInfo.addNativeTypeMapping("DATALINK", "MEDIUMBLOB", "LONGVARBINARY");

        databaseInfo.setDefaultSize(Types.CHAR, 254);
        databaseInfo.setDefaultSize(Types.VARCHAR, 254);
        databaseInfo.setDefaultSize(Types.BINARY, 254);
        databaseInfo.setDefaultSize(Types.VARBINARY, 254);

        databaseInfo.setNonBlankCharColumnSpacePadded(false);
        databaseInfo.setBlankCharColumnSpacePadded(false);
        databaseInfo.setCharColumnSpaceTrimmed(true);
        databaseInfo.setEmptyStringNulled(false);

        // MySql 5.0 returns an empty string for default values for pk columns
        // which is different from the MySql 4 behaviour
        databaseInfo.setSyntheticDefaultValueForRequiredReturned(false);       

        // we need to handle the backslash first otherwise the other
        // already escaped sequences would be affected
        addEscapedCharSequence("\\", "\\\\");
        addEscapedCharSequence("\0", "\\0");
        addEscapedCharSequence("'", "\\'");
        addEscapedCharSequence("\"", "\\\"");
        addEscapedCharSequence("\b", "\\b");
        addEscapedCharSequence("\n", "\\n");
        addEscapedCharSequence("\r", "\\r");
        addEscapedCharSequence("\t", "\\t");
        addEscapedCharSequence("\u001A", "\\Z");
        addEscapedCharSequence("%", "\\%");
        addEscapedCharSequence("_", "\\_");
    }

    @Override
    public void dropTable(Table table, StringBuilder ddl) {
        ddl.append("DROP TABLE IF EXISTS ");
        printIdentifier(getTableName(table.getName()), ddl);
        printEndOfStatement(ddl);
    }

    @Override
    protected void writeColumnAutoIncrementStmt(Table table, Column column, StringBuilder ddl) {
        ddl.append("AUTO_INCREMENT");
    }

    @Override
    protected boolean shouldGeneratePrimaryKeys(Column[] primaryKeyColumns) {
        // mySQL requires primary key indication for autoincrement key columns
        // I'm not sure why the default skips the pk statement if all are
        // identity
        return true;
    }

    /*
     * Normally mysql will return the LAST_INSERT_ID as the column name for the
     * inserted id. Since ddlutils expects the real column name of the field
     * that is autoincrementing, the column has an alias of that column name.
     */
    @Override
    public String getSelectLastIdentityValues(Table table) {
        String autoIncrementKeyName = "";
        if (table.getAutoIncrementColumns().length > 0) {
            autoIncrementKeyName = table.getAutoIncrementColumns()[0].getName();
        }
        return "SELECT LAST_INSERT_ID() " + autoIncrementKeyName;
    }

    @Override
    protected void writeExternalForeignKeyDropStmt(Table table, ForeignKey foreignKey,
            StringBuilder ddl) {
        writeTableAlterStmt(table, ddl);
        ddl.append("DROP FOREIGN KEY ");
        printIdentifier(getForeignKeyName(table, foreignKey), ddl);
        printEndOfStatement(ddl);

        if (foreignKey.isAutoIndexPresent()) {
            writeTableAlterStmt(table, ddl);
            ddl.append("DROP INDEX ");
            printIdentifier(getForeignKeyName(table, foreignKey), ddl);
            printEndOfStatement(ddl);
        }
    }

    @Override
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            Table sourceTable, Table targetTable, List<TableChange> changes, StringBuilder ddl) {
        // in order to utilize the ALTER TABLE ADD COLUMN AFTER statement
        // we have to apply the add column changes in the correct order
        // thus we first gather all add column changes and then execute them
        ArrayList<AddColumnChange> addColumnChanges = new ArrayList<AddColumnChange>();

        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();

            if (change instanceof AddColumnChange) {
                addColumnChanges.add((AddColumnChange) change);
                changeIt.remove();
            }
        }
        for (Iterator<AddColumnChange> changeIt = addColumnChanges.iterator(); changeIt.hasNext();) {
            AddColumnChange addColumnChange = changeIt.next();

            processChange(currentModel, desiredModel, addColumnChange, ddl);
            changeIt.remove();
        }

        ListOrderedSet changedColumns = new ListOrderedSet();

        // we don't have to care about the order because the comparator will
        // have ensured that a add primary key change comes after all necessary
        // columns are present
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();

            if (change instanceof RemoveColumnChange) {
                processChange(currentModel, desiredModel, (RemoveColumnChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof AddPrimaryKeyChange) {
                processChange(currentModel, desiredModel, (AddPrimaryKeyChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof PrimaryKeyChange) {
                processChange(currentModel, desiredModel, (PrimaryKeyChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof RemovePrimaryKeyChange) {
                processChange(currentModel, desiredModel, (RemovePrimaryKeyChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof ColumnChange) {
                // we gather all changed columns because we can use the ALTER
                // TABLE MODIFY COLUMN
                // statement for them
                changedColumns.add(((ColumnChange) change).getChangedColumn());
                changeIt.remove();
            }
        }
        for (Iterator<Column> columnIt = changedColumns.iterator(); columnIt.hasNext();) {
            Column sourceColumn = columnIt.next();
            Column targetColumn = targetTable.findColumn(sourceColumn.getName(),
                    delimitedIdentifierModeOn);

            processColumnChange(sourceTable, targetTable, sourceColumn, targetColumn, ddl);
        }
    }

    /*
     * Processes the addition of a column to a table.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            AddColumnChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
        printIndent(ddl);
        ddl.append("ADD COLUMN ");
        writeColumn(change.getChangedTable(), change.getNewColumn(), ddl);
        if (change.getPreviousColumn() != null) {
            ddl.append(" AFTER ");
            printIdentifier(getColumnName(change.getPreviousColumn()), ddl);
        } else {
            ddl.append(" FIRST");
        }
        printEndOfStatement(ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    /*
     * Processes the removal of a column from a table.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            RemoveColumnChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
        printIndent(ddl);
        ddl.append("DROP COLUMN ");
        printIdentifier(getColumnName(change.getColumn()), ddl);
        printEndOfStatement(ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    /*
     * Processes the removal of a primary key from a table.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            RemovePrimaryKeyChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
        printIndent(ddl);
        ddl.append("DROP PRIMARY KEY");
        printEndOfStatement(ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    /*
     * Processes the change of the primary key of a table.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            PrimaryKeyChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
        printIndent(ddl);
        ddl.append("DROP PRIMARY KEY");
        printEndOfStatement(ddl);
        writeExternalPrimaryKeysCreateStmt(change.getChangedTable(),
                change.getNewPrimaryKeyColumns(), ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    /*
     * Processes a change to a column.
     */
    protected void processColumnChange(Table sourceTable, Table targetTable, Column sourceColumn,
            Column targetColumn, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(sourceTable.getName()), ddl);
        printIndent(ddl);
        ddl.append("MODIFY COLUMN ");
        writeColumn(targetTable, targetColumn, ddl);
        printEndOfStatement(ddl);
    }
}
