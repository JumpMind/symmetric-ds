package org.jumpmind.db.platform.sybase;

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

import java.io.Writer;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.IDatabasePlatform;
import org.jumpmind.db.alter.AddColumnChange;
import org.jumpmind.db.alter.AddPrimaryKeyChange;
import org.jumpmind.db.alter.ColumnAutoIncrementChange;
import org.jumpmind.db.alter.ColumnChange;
import org.jumpmind.db.alter.ColumnDefaultValueChange;
import org.jumpmind.db.alter.IModelChange;
import org.jumpmind.db.alter.PrimaryKeyChange;
import org.jumpmind.db.alter.RemoveColumnChange;
import org.jumpmind.db.alter.RemovePrimaryKeyChange;
import org.jumpmind.db.alter.TableChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.Index;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.SqlBuilder;
import org.jumpmind.db.util.Jdbc3Utils;
import org.jumpmind.util.Log;

/*
 * The SQL Builder for Sybase.
 */
public class SybaseBuilder extends SqlBuilder {

    public SybaseBuilder(Log log, IDatabasePlatform platform, Writer writer) {
        super(log, platform, writer);
        addEscapedCharSequence("'", "''");
    }

    @Override
    public void createTable(Database database, Table table)  {
        writeQuotationOnStatement();
        super.createTable(database, table);
    }

    @Override
    protected void writeColumn(Table table, Column column)  {
        printIdentifier(getColumnName(column));
        print(" ");
        print(getSqlType(column));
        writeColumnDefaultValueStmt(table, column);
        // Sybase does not like NULL/NOT NULL and IDENTITY together
        if (column.isAutoIncrement()) {
            print(" ");
            writeColumnAutoIncrementStmt(table, column);
        } else {
            print(" ");
            if (column.isRequired()) {
                writeColumnNotNullableStmt();
            } else {
                // we'll write a NULL for all columns that are not required
                writeColumnNullableStmt();
            }
        }
    }

    @Override
    protected String getNativeDefaultValue(Column column) {
        if ((column.getTypeCode() == Types.BIT)
                || (Jdbc3Utils.supportsJava14JdbcTypes() && (column.getTypeCode() == Jdbc3Utils
                        .determineBooleanTypeCode()))) {
            return getDefaultValueHelper().convert(column.getDefaultValue(), column.getTypeCode(),
                    Types.SMALLINT).toString();
        } else {
            return super.getNativeDefaultValue(column);
        }
    }

    @Override
    public void dropTable(Table table)  {
        writeQuotationOnStatement();
        print("IF EXISTS (SELECT 1 FROM sysobjects WHERE type = 'U' AND name = ");
        printAlwaysSingleQuotedIdentifier(getTableName(table));
        println(")");
        println("BEGIN");
        printIndent();
        print("DROP TABLE ");
        printlnIdentifier(getTableName(table));
        print("END");
        printEndOfStatement();
    }

    @Override
    protected void writeExternalForeignKeyDropStmt(Table table, ForeignKey foreignKey)
             {
        String constraintName = getForeignKeyName(table, foreignKey);

        print("IF EXISTS (SELECT 1 FROM sysobjects WHERE type = 'RI' AND name = ");
        printAlwaysSingleQuotedIdentifier(constraintName);
        println(")");
        printIndent();
        print("ALTER TABLE ");
        printIdentifier(getTableName(table));
        print(" DROP CONSTRAINT ");
        printIdentifier(constraintName);
        printEndOfStatement();
    }

    @Override
    public void writeExternalIndexDropStmt(Table table, Index index)  {
        print("DROP INDEX ");
        printIdentifier(getTableName(table));
        print(".");
        printIdentifier(getIndexName(index));
        printEndOfStatement();
    }

    @Override
    public void dropExternalForeignKeys(Table table)  {
        writeQuotationOnStatement();
        super.dropExternalForeignKeys(table);
    }

    @Override
    public String getSelectLastIdentityValues(Table table) {
        return "SELECT @@IDENTITY";
    }

    /*
     * Returns the SQL to enable identity override mode.
     * 
     * @param table The table to enable the mode for
     * 
     * @return The SQL
     */
    protected String getEnableIdentityOverrideSql(Table table) {
        StringBuffer result = new StringBuffer();

        result.append("SET IDENTITY_INSERT ");
        result.append(getDelimitedIdentifier(getTableName(table)));
        result.append(" ON");

        return result.toString();
    }

    /*
     * Returns the SQL to disable identity override mode.
     * 
     * @param table The table to disable the mode for
     * 
     * @return The SQL
     */
    protected String getDisableIdentityOverrideSql(Table table) {
        StringBuffer result = new StringBuffer();

        result.append("SET IDENTITY_INSERT ");
        result.append(getDelimitedIdentifier(getTableName(table)));
        result.append(" OFF");

        return result.toString();
    }

    /*
     * Returns the statement that turns on the ability to write delimited
     * identifiers.
     * 
     * @return The quotation-on statement
     */
    protected String getQuotationOnStatement() {
        if (platform.isDelimitedIdentifierModeOn()) {
            return "SET quoted_identifier on";
        } else {
            return "";
        }
    }

    /*
     * Writes the statement that turns on the ability to write delimited
     * identifiers.
     */
    private void writeQuotationOnStatement()  {
        print(getQuotationOnStatement());
        printEndOfStatement();
    }

    /*
     * Prints the given identifier with enforced single quotes around it
     * regardless of whether delimited identifiers are turned on or not.
     * 
     * @param identifier The identifier
     */
    private void printAlwaysSingleQuotedIdentifier(String identifier)  {
        print("'");
        print(identifier);
        print("'");
    }

    /*
     * {@inheritDoc}
     */
    public void writeCopyDataStatement(Table sourceTable, Table targetTable)  {
        boolean hasIdentity = targetTable.getAutoIncrementColumns().length > 0;

        if (hasIdentity) {
            print("SET IDENTITY_INSERT ");
            printIdentifier(getTableName(targetTable));
            print(" ON");
            printEndOfStatement();
        }
        super.writeCopyDataStatement(sourceTable, targetTable);
        if (hasIdentity) {
            print("SET IDENTITY_INSERT ");
            printIdentifier(getTableName(targetTable));
            print(" OFF");
            printEndOfStatement();
        }
    }

    /*
     * {@inheritDoc}
     */
    protected void writeCastExpression(Column sourceColumn, Column targetColumn)  {
        String sourceNativeType = getBareNativeType(sourceColumn);
        String targetNativeType = getBareNativeType(targetColumn);

        if (sourceNativeType.equals(targetNativeType)) {
            printIdentifier(getColumnName(sourceColumn));
        } else {
            print("CONVERT(");
            print(getNativeType(targetColumn));
            print(",");
            printIdentifier(getColumnName(sourceColumn));
            print(")");
        }
    }

    @Override
    protected void processChanges(Database currentModel, Database desiredModel,
            List<IModelChange> changes)  {
        if (!changes.isEmpty()) {
            writeQuotationOnStatement();
        }
        super.processChanges(currentModel, desiredModel, changes);
    }

    @Override
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            Table sourceTable, Table targetTable, List<TableChange> changes)  {
        // First we drop primary keys as necessary
        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = (TableChange) changeIt.next();

            if (change instanceof RemovePrimaryKeyChange) {
                processChange(currentModel, desiredModel, (RemovePrimaryKeyChange) change);
                changeIt.remove();
            } else if (change instanceof PrimaryKeyChange) {
                PrimaryKeyChange pkChange = (PrimaryKeyChange) change;
                RemovePrimaryKeyChange removePkChange = new RemovePrimaryKeyChange(
                        pkChange.getChangedTable(), pkChange.getOldPrimaryKeyColumns());

                processChange(currentModel, desiredModel, removePkChange);
            }
        }

        HashMap columnChanges = new HashMap();

        // Next we add/remove columns
        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = (TableChange) changeIt.next();

            if (change instanceof AddColumnChange) {
                AddColumnChange addColumnChange = (AddColumnChange) change;

                // Sybase can only add not insert columns
                if (addColumnChange.isAtEnd()) {
                    processChange(currentModel, desiredModel, addColumnChange);
                    changeIt.remove();
                }
            } else if (change instanceof RemoveColumnChange) {
                processChange(currentModel, desiredModel, (RemoveColumnChange) change);
                changeIt.remove();
            } else if (change instanceof ColumnAutoIncrementChange) {
                // Sybase has no way of adding or removing an IDENTITY
                // constraint
                // Thus we have to rebuild the table anyway and can ignore all
                // the other
                // column changes
                columnChanges = null;
            } else if ((change instanceof ColumnChange) && (columnChanges != null)) {
                // we gather all changed columns because we can use the ALTER
                // TABLE ALTER COLUMN
                // statement for them
                ColumnChange columnChange = (ColumnChange) change;
                ArrayList changesPerColumn = (ArrayList) columnChanges.get(columnChange
                        .getChangedColumn());

                if (changesPerColumn == null) {
                    changesPerColumn = new ArrayList();
                    columnChanges.put(columnChange.getChangedColumn(), changesPerColumn);
                }
                changesPerColumn.add(change);
            }
        }
        if (columnChanges != null) {
            for (Iterator changesPerColumnIt = columnChanges.entrySet().iterator(); changesPerColumnIt
                    .hasNext();) {
                Map.Entry entry = (Map.Entry) changesPerColumnIt.next();
                Column sourceColumn = (Column) entry.getKey();
                ArrayList changesPerColumn = (ArrayList) entry.getValue();

                // Sybase does not like us to use the ALTER TABLE ALTER
                // statement if we don't actually
                // change the datatype or the required constraint but only the
                // default value
                // Thus, if we only have to change the default, we use a
                // different handler
                if ((changesPerColumn.size() == 1)
                        && (changesPerColumn.get(0) instanceof ColumnDefaultValueChange)) {
                    processChange(currentModel, desiredModel,
                            (ColumnDefaultValueChange) changesPerColumn.get(0));
                } else {
                    Column targetColumn = targetTable.findColumn(sourceColumn.getName(),
                            platform.isDelimitedIdentifierModeOn());

                    processColumnChange(sourceTable, targetTable, sourceColumn, targetColumn);
                }
                for (Iterator changeIt = changesPerColumn.iterator(); changeIt.hasNext();) {
                    ((ColumnChange) changeIt.next()).apply(currentModel,
                            platform.isDelimitedIdentifierModeOn());
                }
            }
        }
        // Finally we add primary keys
        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = (TableChange) changeIt.next();

            if (change instanceof AddPrimaryKeyChange) {
                processChange(currentModel, desiredModel, (AddPrimaryKeyChange) change);
                changeIt.remove();
            } else if (change instanceof PrimaryKeyChange) {
                PrimaryKeyChange pkChange = (PrimaryKeyChange) change;
                AddPrimaryKeyChange addPkChange = new AddPrimaryKeyChange(
                        pkChange.getChangedTable(), pkChange.getNewPrimaryKeyColumns());

                processChange(currentModel, desiredModel, addPkChange);
                changeIt.remove();
            }
        }
    }

    /*
     * Processes the addition of a column to a table.
     * 
     * @param currentModel The current database schema
     * 
     * @param desiredModel The desired database schema
     * 
     * @param change The change object
     */
    protected void processChange(Database currentModel, Database desiredModel,
            AddColumnChange change)  {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("ADD ");
        writeColumn(change.getChangedTable(), change.getNewColumn());
        printEndOfStatement();
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }

    /*
     * Processes the removal of a column from a table.
     * 
     * @param currentModel The current database schema
     * 
     * @param desiredModel The desired database schema
     * 
     * @param change The change object
     */
    protected void processChange(Database currentModel, Database desiredModel,
            RemoveColumnChange change)  {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("DROP ");
        printIdentifier(getColumnName(change.getColumn()));
        printEndOfStatement();
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }

    /*
     * Processes the removal of a primary key from a table.
     * 
     * @param currentModel The current database schema
     * 
     * @param desiredModel The desired database schema
     * 
     * @param change The change object
     */
    protected void processChange(Database currentModel, Database desiredModel,
            RemovePrimaryKeyChange change)  {
        // TODO: this would be easier when named primary keys are supported
        // because then we can use ALTER TABLE DROP
        String tableName = getTableName(change.getChangedTable());
        String tableNameVar = "tn" + createUniqueIdentifier();
        String constraintNameVar = "cn" + createUniqueIdentifier();

        println("BEGIN");
        println("  DECLARE @" + tableNameVar + " nvarchar(60), @" + constraintNameVar
                + " nvarchar(60)");
        println("  WHILE EXISTS(SELECT sysindexes.name");
        println("                 FROM sysindexes, sysobjects");
        print("                 WHERE sysobjects.name = ");
        printAlwaysSingleQuotedIdentifier(tableName);
        println(" AND sysobjects.id = sysindexes.id AND (sysindexes.status & 2048) > 0)");
        println("  BEGIN");
        println("    SELECT @" + tableNameVar + " = sysobjects.name, @" + constraintNameVar
                + " = sysindexes.name");
        println("      FROM sysindexes, sysobjects");
        print("      WHERE sysobjects.name = ");
        printAlwaysSingleQuotedIdentifier(tableName);
        print(" AND sysobjects.id = sysindexes.id AND (sysindexes.status & 2048) > 0");
        println("    EXEC ('ALTER TABLE '+@" + tableNameVar + "+' DROP CONSTRAINT '+@"
                + constraintNameVar + ")");
        println("  END");
        print("END");
        printEndOfStatement();
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }

    /*
     * Processes the change of the default value of a column. Note that this
     * method is only used if it is the only change to that column.
     * 
     * @param currentModel The current database schema
     * 
     * @param desiredModel The desired database schema
     * 
     * @param change The change object
     */
    protected void processChange(Database currentModel, Database desiredModel,
            ColumnDefaultValueChange change)  {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("REPLACE ");
        printIdentifier(getColumnName(change.getChangedColumn()));

        Table curTable = currentModel.findTable(change.getChangedTable().getName(),
                platform.isDelimitedIdentifierModeOn());
        Column curColumn = curTable.findColumn(change.getChangedColumn().getName(),
                platform.isDelimitedIdentifierModeOn());

        print(" DEFAULT ");
        if (isValidDefaultValue(change.getNewDefaultValue(), curColumn.getTypeCode())) {
            printDefaultValue(change.getNewDefaultValue(), curColumn.getTypeCode());
        } else {
            print("NULL");
        }
        printEndOfStatement();
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }

    /*
     * Processes a change to a column.
     * 
     * @param sourceTable The current table
     * 
     * @param targetTable The desired table
     * 
     * @param sourceColumn The current column
     * 
     * @param targetColumn The desired column
     */
    protected void processColumnChange(Table sourceTable, Table targetTable, Column sourceColumn,
            Column targetColumn)  {
        Object oldParsedDefault = sourceColumn.getParsedDefaultValue();
        Object newParsedDefault = targetColumn.getParsedDefaultValue();
        String newDefault = targetColumn.getDefaultValue();
        boolean defaultChanges = ((oldParsedDefault == null) && (newParsedDefault != null))
                || ((oldParsedDefault != null) && !oldParsedDefault.equals(newParsedDefault));

        // Sybase does not like it if there is a default spec in the ALTER TABLE
        // ALTER
        // statement; thus we have to change the default afterwards
        if (newDefault != null) {
            targetColumn.setDefaultValue(null);
        }
        if (defaultChanges) {
            // we're first removing the default as it might make problems when
            // the
            // datatype changes
            print("ALTER TABLE ");
            printlnIdentifier(getTableName(sourceTable));
            printIndent();
            print("REPLACE ");
            printIdentifier(getColumnName(sourceColumn));
            print(" DEFAULT NULL");
            printEndOfStatement();
        }
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(sourceTable));
        printIndent();
        print("MODIFY ");
        writeColumn(sourceTable, targetColumn);
        printEndOfStatement();
        if (defaultChanges) {
            print("ALTER TABLE ");
            printlnIdentifier(getTableName(sourceTable));
            printIndent();
            print("REPLACE ");
            printIdentifier(getColumnName(sourceColumn));
            if (newDefault != null) {
                writeColumnDefaultValueStmt(sourceTable, targetColumn);
            } else {
                print(" DEFAULT NULL");
            }
            printEndOfStatement();
        }
    }
}
