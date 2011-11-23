package org.jumpmind.db.platform.mssql;

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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.IDatabasePlatform;
import org.jumpmind.db.alter.AddColumnChange;
import org.jumpmind.db.alter.AddForeignKeyChange;
import org.jumpmind.db.alter.AddIndexChange;
import org.jumpmind.db.alter.AddPrimaryKeyChange;
import org.jumpmind.db.alter.ColumnAutoIncrementChange;
import org.jumpmind.db.alter.ColumnChange;
import org.jumpmind.db.alter.ColumnDataTypeChange;
import org.jumpmind.db.alter.ColumnSizeChange;
import org.jumpmind.db.alter.IModelChange;
import org.jumpmind.db.alter.PrimaryKeyChange;
import org.jumpmind.db.alter.RemoveColumnChange;
import org.jumpmind.db.alter.RemoveForeignKeyChange;
import org.jumpmind.db.alter.RemoveIndexChange;
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
 * The SQL Builder for the Microsoft SQL Server.
 */
public class MSSqlBuilder extends SqlBuilder {

    /* We use a generic date format. */
    private DateFormat _genericDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    /* We use a generic date format. */
    private DateFormat _genericTimeFormat = new SimpleDateFormat("HH:mm:ss");

    public MSSqlBuilder(Log log, IDatabasePlatform platform) {
        super(log, platform);
        addEscapedCharSequence("'", "''");
    }

    @Override
    public void createTable(Database database, Table table, StringBuilder ddl) {
        writeQuotationOnStatement(ddl);
        super.createTable(database, table, ddl);
    }

    @Override
    public void dropTable(Table table, StringBuilder ddl) {
        String tableName = getTableName(table);
        String tableNameVar = "tn" + createUniqueIdentifier();
        String constraintNameVar = "cn" + createUniqueIdentifier();

        writeQuotationOnStatement(ddl);
        ddl.append("IF EXISTS (SELECT 1 FROM sysobjects WHERE type = 'U' AND name = ");
        printAlwaysSingleQuotedIdentifier(tableName, ddl);
        println(")", ddl);
        println("BEGIN", ddl);
        println("  DECLARE @" + tableNameVar + " nvarchar(256), @" + constraintNameVar
                + " nvarchar(256)", ddl);
        println("  DECLARE refcursor CURSOR FOR", ddl);
        println("  SELECT object_name(objs.parent_obj) tablename, objs.name constraintname", ddl);
        println("    FROM sysobjects objs JOIN sysconstraints cons ON objs.id = cons.constid", ddl);
        ddl.append("    WHERE objs.xtype != 'PK' AND object_name(objs.parent_obj) = ");
        printAlwaysSingleQuotedIdentifier(tableName, ddl);
        println("  OPEN refcursor", ddl);
        println("  FETCH NEXT FROM refcursor INTO @" + tableNameVar + ", @" + constraintNameVar,
                ddl);
        println("  WHILE @@FETCH_STATUS = 0", ddl);
        println("    BEGIN", ddl);
        println("      EXEC ('ALTER TABLE '+@" + tableNameVar + "+' DROP CONSTRAINT '+@"
                + constraintNameVar + ")", ddl);
        println("      FETCH NEXT FROM refcursor INTO @" + tableNameVar + ", @" + constraintNameVar,
                ddl);
        println("    END", ddl);
        println("  CLOSE refcursor", ddl);
        println("  DEALLOCATE refcursor", ddl);
        ddl.append("  DROP TABLE ");
        printlnIdentifier(tableName, ddl);
        ddl.append("END");
        printEndOfStatement(ddl);
    }

    @Override
    public void dropExternalForeignKeys(Table table, StringBuilder ddl) {
        writeQuotationOnStatement(ddl);
        super.dropExternalForeignKeys(table, ddl);
    }

    @Override
    protected DateFormat getValueDateFormat() {
        return _genericDateFormat;
    }

    @Override
    protected DateFormat getValueTimeFormat() {
        return _genericTimeFormat;
    }

    @Override
    protected String getValueAsString(Column column, Object value) {
        if (value == null) {
            return "NULL";
        }

        StringBuffer result = new StringBuffer();

        switch (column.getTypeCode()) {
        case Types.REAL:
        case Types.NUMERIC:
        case Types.FLOAT:
        case Types.DOUBLE:
        case Types.DECIMAL:
            // SQL Server does not want quotes around the value
            if (!(value instanceof String) && (getValueNumberFormat() != null)) {
                result.append(getValueNumberFormat().format(value));
            } else {
                result.append(value.toString());
            }
            break;
        case Types.DATE:
            result.append("CAST(");
            result.append(platform.getPlatformInfo().getValueQuoteToken());
            result.append(value instanceof String ? (String) value : getValueDateFormat().format(
                    value));
            result.append(platform.getPlatformInfo().getValueQuoteToken());
            result.append(" AS datetime)");
            break;
        case Types.TIME:
            result.append("CAST(");
            result.append(platform.getPlatformInfo().getValueQuoteToken());
            result.append(value instanceof String ? (String) value : getValueTimeFormat().format(
                    value));
            result.append(platform.getPlatformInfo().getValueQuoteToken());
            result.append(" AS datetime)");
            break;
        case Types.TIMESTAMP:
            result.append("CAST(");
            result.append(platform.getPlatformInfo().getValueQuoteToken());
            result.append(value.toString());
            result.append(platform.getPlatformInfo().getValueQuoteToken());
            result.append(" AS datetime)");
            break;
        }
        return super.getValueAsString(column, value);
    }

    @Override
    protected String getNativeDefaultValue(Column column) {
        // Sql Server wants BIT default values as 0 or 1
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
    protected void writeColumnAutoIncrementStmt(Table table, Column column, StringBuilder ddl) {
        ddl.append("IDENTITY (1,1) ");
    }

    @Override
    public void writeExternalIndexDropStmt(Table table, Index index, StringBuilder ddl) {
        ddl.append("DROP INDEX ");
        printIdentifier(getTableName(table), ddl);
        ddl.append(".");
        printIdentifier(getIndexName(index), ddl);
        printEndOfStatement(ddl);
    }

    @Override
    protected void writeExternalForeignKeyDropStmt(Table table, ForeignKey foreignKey,
            StringBuilder ddl) {
        String constraintName = getForeignKeyName(table, foreignKey);

        ddl.append("IF EXISTS (SELECT 1 FROM sysobjects WHERE type = 'F' AND name = ");
        printAlwaysSingleQuotedIdentifier(constraintName, ddl);
        println(")", ddl);
        printIndent(ddl);
        ddl.append("ALTER TABLE ");
        printIdentifier(getTableName(table), ddl);
        ddl.append(" DROP CONSTRAINT ");
        printIdentifier(constraintName, ddl);
        printEndOfStatement(ddl);
    }

    /*
     * Returns the statement that turns on the ability to write delimited
     * identifiers.
     * 
     * @return The quotation-on statement
     */
    private String getQuotationOnStatement() {
        if (platform.isDelimitedIdentifierModeOn()) {
            return "SET quoted_identifier on" + platform.getPlatformInfo().getSqlCommandDelimiter()
                    + "\n";
        } else {
            return "";
        }
    }

    /*
     * Writes the statement that turns on the ability to write delimited
     * identifiers.
     */
    private void writeQuotationOnStatement(StringBuilder ddl) {
        ddl.append(getQuotationOnStatement());
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

        result.append(getQuotationOnStatement());
        result.append("SET IDENTITY_INSERT ");
        result.append(getDelimitedIdentifier(getTableName(table)));
        result.append(" ON");
        result.append(platform.getPlatformInfo().getSqlCommandDelimiter());

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

        result.append(getQuotationOnStatement());
        result.append("SET IDENTITY_INSERT ");
        result.append(getDelimitedIdentifier(getTableName(table)));
        result.append(" OFF");
        result.append(platform.getPlatformInfo().getSqlCommandDelimiter());

        return result.toString();
    }

    @Override
    public String getDeleteSql(Table table, Map<String, Object> pkValues, boolean genPlaceholders) {
        return getQuotationOnStatement() + super.getDeleteSql(table, pkValues, genPlaceholders);
    }

    @Override
    public String getInsertSql(Table table, Map<String, Object> columnValues,
            boolean genPlaceholders) {
        return getQuotationOnStatement() + super.getInsertSql(table, columnValues, genPlaceholders);
    }

    @Override
    public String getUpdateSql(Table table, Map<String, Object> columnValues,
            boolean genPlaceholders) {
        return getQuotationOnStatement() + super.getUpdateSql(table, columnValues, genPlaceholders);
    }

    /*
     * Prints the given identifier with enforced single quotes around it
     * regardless of whether delimited identifiers are turned on or not.
     * 
     * @param identifier The identifier
     */
    private void printAlwaysSingleQuotedIdentifier(String identifier, StringBuilder ddl) {
        ddl.append("'");
        ddl.append(identifier);
        ddl.append("'");
    }

    @Override
    public void writeCopyDataStatement(Table sourceTable, Table targetTable, StringBuilder ddl) {
        // Sql Server per default does not allow us to insert values explicitly
        // into
        // identity columns. However, we can change this behavior
        boolean hasIdentityColumns = targetTable.getAutoIncrementColumns().length > 0;

        if (hasIdentityColumns) {
            ddl.append("SET IDENTITY_INSERT ");
            printIdentifier(getTableName(targetTable), ddl);
            ddl.append(" ON");
            printEndOfStatement(ddl);
        }
        super.writeCopyDataStatement(sourceTable, targetTable, ddl);
        // We have to turn it off ASAP because it can be on only for one table
        // per session
        if (hasIdentityColumns) {
            ddl.append("SET IDENTITY_INSERT ");
            printIdentifier(getTableName(targetTable), ddl);
            ddl.append(" OFF");
            printEndOfStatement(ddl);
        }
    }

    @Override
    protected void processChanges(Database currentModel, Database desiredModel,
            List<IModelChange> changes, StringBuilder ddl) {
        if (!changes.isEmpty()) {
            writeQuotationOnStatement(ddl);
        }
        // For column data type and size changes, we need to drop and then
        // re-create indexes
        // and foreign keys using the column, as well as any primary keys
        // containg
        // these columns
        // However, if the index/foreign key/primary key is already slated for
        // removal or
        // change, then we don't want to generate change duplication
        HashSet removedIndexes = new HashSet();
        HashSet removedForeignKeys = new HashSet();
        HashSet removedPKs = new HashSet();

        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
            Object change = changeIt.next();

            if (change instanceof RemoveIndexChange) {
                removedIndexes.add(((RemoveIndexChange) change).getIndex());
            } else if (change instanceof RemoveForeignKeyChange) {
                removedForeignKeys.add(((RemoveForeignKeyChange) change).getForeignKey());
            } else if (change instanceof RemovePrimaryKeyChange) {
                removedPKs.add(((RemovePrimaryKeyChange) change).getChangedTable());
            }
        }

        ArrayList additionalChanges = new ArrayList();

        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
            Object change = changeIt.next();

            if ((change instanceof ColumnDataTypeChange) || (change instanceof ColumnSizeChange)) {
                Column column = ((ColumnChange) change).getChangedColumn();
                Table table = ((ColumnChange) change).getChangedTable();

                if (column.isPrimaryKey() && !removedPKs.contains(table)) {
                    Column[] pk = table.getPrimaryKeyColumns();

                    additionalChanges.add(new RemovePrimaryKeyChange(table, pk));
                    additionalChanges.add(new AddPrimaryKeyChange(table, pk));
                    removedPKs.add(table);
                }
                for (int idx = 0; idx < table.getIndexCount(); idx++) {
                    Index index = table.getIndex(idx);

                    if (index.hasColumn(column) && !removedIndexes.contains(index)) {
                        additionalChanges.add(new RemoveIndexChange(table, index));
                        additionalChanges.add(new AddIndexChange(table, index));
                        removedIndexes.add(index);
                    }
                }
                for (int tableIdx = 0; tableIdx < currentModel.getTableCount(); tableIdx++) {
                    Table curTable = currentModel.getTable(tableIdx);

                    for (int fkIdx = 0; fkIdx < curTable.getForeignKeyCount(); fkIdx++) {
                        ForeignKey curFk = curTable.getForeignKey(fkIdx);

                        if ((curFk.hasLocalColumn(column) || curFk.hasForeignColumn(column))
                                && !removedForeignKeys.contains(curFk)) {
                            additionalChanges.add(new RemoveForeignKeyChange(curTable, curFk));
                            additionalChanges.add(new AddForeignKeyChange(curTable, curFk));
                            removedForeignKeys.add(curFk);
                        }
                    }
                }
            }
        }
        changes.addAll(additionalChanges);
        super.processChanges(currentModel, desiredModel, changes, ddl);
    }

    @Override
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            Table sourceTable, Table targetTable, List<TableChange> changes, StringBuilder ddl) {
        // First we drop primary keys as necessary
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();

            if (change instanceof RemovePrimaryKeyChange) {
                processChange(currentModel, desiredModel, (RemovePrimaryKeyChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof PrimaryKeyChange) {
                PrimaryKeyChange pkChange = (PrimaryKeyChange) change;
                RemovePrimaryKeyChange removePkChange = new RemovePrimaryKeyChange(
                        pkChange.getChangedTable(), pkChange.getOldPrimaryKeyColumns());

                processChange(currentModel, desiredModel, removePkChange, ddl);
            }
        }

        ArrayList columnChanges = new ArrayList();

        // Next we add/remove columns
        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = (TableChange) changeIt.next();

            if (change instanceof AddColumnChange) {
                AddColumnChange addColumnChange = (AddColumnChange) change;

                // Sql Server can only add not insert columns
                if (addColumnChange.isAtEnd()) {
                    processChange(currentModel, desiredModel, addColumnChange, ddl);
                    changeIt.remove();
                }
            } else if (change instanceof RemoveColumnChange) {
                processChange(currentModel, desiredModel, (RemoveColumnChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof ColumnAutoIncrementChange) {
                // Sql Server has no way of adding or removing an IDENTITY
                // constraint
                // Thus we have to rebuild the table anyway and can ignore all
                // the other
                // column changes
                columnChanges = null;
            } else if ((change instanceof ColumnChange) && (columnChanges != null)) {
                // we gather all changed columns because we can use the ALTER
                // TABLE ALTER COLUMN
                // statement for them
                columnChanges.add(change);
            }
        }
        if (columnChanges != null) {
            HashSet processedColumns = new HashSet();

            for (Iterator changeIt = columnChanges.iterator(); changeIt.hasNext();) {
                ColumnChange change = (ColumnChange) changeIt.next();
                Column sourceColumn = change.getChangedColumn();
                Column targetColumn = targetTable.findColumn(sourceColumn.getName(),
                        platform.isDelimitedIdentifierModeOn());

                if (!processedColumns.contains(targetColumn)) {
                    processColumnChange(sourceTable, targetTable, sourceColumn, targetColumn,
                            (change instanceof ColumnDataTypeChange)
                                    || (change instanceof ColumnSizeChange), ddl);
                    processedColumns.add(targetColumn);
                }
                changes.remove(change);
                change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
            }
        }
        // Finally we add primary keys
        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = (TableChange) changeIt.next();

            if (change instanceof AddPrimaryKeyChange) {
                processChange(currentModel, desiredModel, (AddPrimaryKeyChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof PrimaryKeyChange) {
                PrimaryKeyChange pkChange = (PrimaryKeyChange) change;
                AddPrimaryKeyChange addPkChange = new AddPrimaryKeyChange(
                        pkChange.getChangedTable(), pkChange.getNewPrimaryKeyColumns());

                processChange(currentModel, desiredModel, addPkChange, ddl);
                changeIt.remove();
            }
        }
    }

    /*
     * Processes the addition of a column to a table.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            AddColumnChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()), ddl);
        printIndent(ddl);
        ddl.append("ADD ");
        writeColumn(change.getChangedTable(), change.getNewColumn(), ddl);
        printEndOfStatement(ddl);
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }

    /*
     * Processes the removal of a column from a table.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            RemoveColumnChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()), ddl);
        printIndent(ddl);
        ddl.append("DROP COLUMN ");
        printIdentifier(getColumnName(change.getColumn()), ddl);
        printEndOfStatement(ddl);
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }

    /*
     * Processes the removal of a primary key from a table.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            RemovePrimaryKeyChange change, StringBuilder ddl) {
        // TODO: this would be easier when named primary keys are supported
        // because then we can use ALTER TABLE DROP
        String tableName = getTableName(change.getChangedTable());
        String tableNameVar = "tn" + createUniqueIdentifier();
        String constraintNameVar = "cn" + createUniqueIdentifier();

        println("BEGIN", ddl);
        println("  DECLARE @" + tableNameVar + " nvarchar(256), @" + constraintNameVar
                + " nvarchar(256)", ddl);
        println("  DECLARE refcursor CURSOR FOR", ddl);
        println("  SELECT object_name(objs.parent_obj) tablename, objs.name constraintname", ddl);
        println("    FROM sysobjects objs JOIN sysconstraints cons ON objs.id = cons.constid", ddl);
        ddl.append("    WHERE objs.xtype = 'PK' AND object_name(objs.parent_obj) = ");
        printAlwaysSingleQuotedIdentifier(tableName, ddl);
        println("  OPEN refcursor", ddl);
        println("  FETCH NEXT FROM refcursor INTO @" + tableNameVar + ", @" + constraintNameVar,
                ddl);
        println("  WHILE @@FETCH_STATUS = 0", ddl);
        println("    BEGIN", ddl);
        println("      EXEC ('ALTER TABLE '+@" + tableNameVar + "+' DROP CONSTRAINT '+@"
                + constraintNameVar + ")", ddl);
        println("      FETCH NEXT FROM refcursor INTO @" + tableNameVar + ", @" + constraintNameVar,
                ddl);
        println("    END", ddl);
        println("  CLOSE refcursor", ddl);
        println("  DEALLOCATE refcursor", ddl);
        ddl.append("END");
        printEndOfStatement(ddl);
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }

    /*
     * Processes a change to a column.
     */
    protected void processColumnChange(Table sourceTable, Table targetTable, Column sourceColumn,
            Column targetColumn, boolean typeChange, StringBuilder ddl) {
        boolean hasDefault = sourceColumn.getParsedDefaultValue() != null;
        boolean shallHaveDefault = targetColumn.getParsedDefaultValue() != null;
        String newDefault = targetColumn.getDefaultValue();

        // Sql Server does not like it if there is a default spec in the ALTER
        // TABLE ALTER COLUMN
        // statement; thus we have to change the default manually
        if (newDefault != null) {
            targetColumn.setDefaultValue(null);
        }
        if (hasDefault) {
            // we're dropping the old default
            String tableName = getTableName(sourceTable);
            String columnName = getColumnName(sourceColumn);
            String tableNameVar = "tn" + createUniqueIdentifier();
            String constraintNameVar = "cn" + createUniqueIdentifier();

            println("BEGIN", ddl);
            println("  DECLARE @" + tableNameVar + " nvarchar(256), @" + constraintNameVar
                    + " nvarchar(256)", ddl);
            println("  DECLARE refcursor CURSOR FOR", ddl);
            println("  SELECT object_name(objs.parent_obj) tablename, objs.name constraintname",
                    ddl);
            println("    FROM sysobjects objs JOIN sysconstraints cons ON objs.id = cons.constid",
                    ddl);
            println("    WHERE objs.xtype = 'D' AND", ddl);
            ddl.append("          cons.colid = (SELECT colid FROM syscolumns WHERE id = object_id(");
            printAlwaysSingleQuotedIdentifier(tableName, ddl);
            ddl.append(") AND name = ");
            printAlwaysSingleQuotedIdentifier(columnName, ddl);
            println(") AND", ddl);
            ddl.append("          object_name(objs.parent_obj) = ");
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

        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(sourceTable), ddl);
        printIndent(ddl);
        ddl.append("ALTER COLUMN ");
        writeColumn(sourceTable, targetColumn, ddl);
        printEndOfStatement(ddl);

        if (shallHaveDefault) {
            targetColumn.setDefaultValue(newDefault);

            // if the column shall have a default, then we have to add it as a
            // constraint
            ddl.append("ALTER TABLE ");
            printlnIdentifier(getTableName(sourceTable), ddl);
            printIndent(ddl);
            ddl.append("ADD CONSTRAINT ");
            printIdentifier(getConstraintName("DF", sourceTable, sourceColumn.getName(), null), ddl);
            writeColumnDefaultValueStmt(sourceTable, targetColumn, ddl);
            ddl.append(" FOR ");
            printIdentifier(getColumnName(sourceColumn), ddl);
            printEndOfStatement(ddl);
        }
    }
}
