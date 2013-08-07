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

import java.rmi.server.UID;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.alter.AddColumnChange;
import org.jumpmind.db.alter.AddForeignKeyChange;
import org.jumpmind.db.alter.AddIndexChange;
import org.jumpmind.db.alter.AddPrimaryKeyChange;
import org.jumpmind.db.alter.ColumnAutoIncrementChange;
import org.jumpmind.db.alter.ColumnChange;
import org.jumpmind.db.alter.ColumnDataTypeChange;
import org.jumpmind.db.alter.ColumnSizeChange;
import org.jumpmind.db.alter.CopyColumnValueChange;
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
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractDdlBuilder;
import org.jumpmind.db.platform.PlatformUtils;

/*
 * The SQL Builder for the Microsoft SQL Server.
 */
public class MsSqlDdlBuilder extends AbstractDdlBuilder {

    /* We use a generic date format. */
    private DateFormat _genericDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    /* We use a generic date format. */
    private DateFormat _genericTimeFormat = new SimpleDateFormat("HH:mm:ss");

    public MsSqlDdlBuilder() {

        databaseInfo.setMaxIdentifierLength(128);
        databaseInfo.setBlobsWorkInWhereClause(false);
        databaseInfo.addNativeTypeMapping(Types.ARRAY, "IMAGE", Types.LONGVARBINARY);
        // BIGINT will be mapped back to BIGINT by the model reader
        databaseInfo.addNativeTypeMapping(Types.BIGINT, "DECIMAL(19,0)");
        databaseInfo.addNativeTypeMapping(Types.BLOB, "IMAGE", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.CLOB, "TEXT", Types.CLOB);
        databaseInfo.addNativeTypeMapping(Types.DATE, "DATETIME", Types.TIMESTAMP);
        databaseInfo.addNativeTypeMapping(Types.DISTINCT, "IMAGE", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.DOUBLE, "FLOAT", Types.FLOAT);
        databaseInfo.addNativeTypeMapping(Types.INTEGER, "INT");
        databaseInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "IMAGE", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.LONGVARBINARY, "IMAGE");
        databaseInfo.addNativeTypeMapping(Types.LONGVARCHAR, "TEXT", Types.LONGVARCHAR);
        databaseInfo.addNativeTypeMapping(Types.NULL, "IMAGE", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.OTHER, "IMAGE", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.REF, "IMAGE", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.STRUCT, "IMAGE", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.TIME, "DATETIME", Types.TIMESTAMP);
        databaseInfo.addNativeTypeMapping(Types.TIMESTAMP, "DATETIME");
        databaseInfo.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);
        databaseInfo.addNativeTypeMapping("BOOLEAN", "BIT", "BIT");
        databaseInfo.addNativeTypeMapping("DATALINK", "IMAGE", "LONGVARBINARY");

        databaseInfo.setDefaultSize(Types.CHAR, 254);
        databaseInfo.setDefaultSize(Types.VARCHAR, 254);
        databaseInfo.setDefaultSize(Types.BINARY, 254);
        databaseInfo.setDefaultSize(Types.VARBINARY, 254);

        databaseInfo.setDateOverridesToTimestamp(true);
        databaseInfo.setNonBlankCharColumnSpacePadded(true);
        databaseInfo.setBlankCharColumnSpacePadded(true);
        databaseInfo.setCharColumnSpaceTrimmed(false);
        databaseInfo.setEmptyStringNulled(false);
        databaseInfo.setAutoIncrementUpdateAllowed(false);

        addEscapedCharSequence("'", "''");
    }

    @Override
    protected void createTable(Table table, StringBuilder ddl, boolean temporary, boolean recreate) {
        writeQuotationOnStatement(ddl);
        super.createTable(table, ddl, temporary, recreate);
    }

    @Override
    protected void dropTable(Table table, StringBuilder ddl, boolean temporary, boolean recreate) {
        String tableName = getTableName(table.getName());
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

        switch (column.getMappedTypeCode()) {
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
                result.append(databaseInfo.getValueQuoteToken());
                result.append(value instanceof String ? (String) value : getValueDateFormat()
                        .format(value));
                result.append(databaseInfo.getValueQuoteToken());
                result.append(" AS datetime)");
                break;
            case Types.TIME:
                result.append("CAST(");
                result.append(databaseInfo.getValueQuoteToken());
                result.append(value instanceof String ? (String) value : getValueTimeFormat()
                        .format(value));
                result.append(databaseInfo.getValueQuoteToken());
                result.append(" AS datetime)");
                break;
            case Types.TIMESTAMP:
                result.append("CAST(");
                result.append(databaseInfo.getValueQuoteToken());
                result.append(value.toString());
                result.append(databaseInfo.getValueQuoteToken());
                result.append(" AS datetime)");
                break;
        }
        return super.getValueAsString(column, value);
    }

    @Override
    protected String getNativeDefaultValue(Column column) {
        // Sql Server wants BIT default values as 0 or 1
        if ((column.getMappedTypeCode() == Types.BIT)
                || (PlatformUtils.supportsJava14JdbcTypes() && (column.getMappedTypeCode() == PlatformUtils
                        .determineBooleanTypeCode()))) {
            return getDefaultValueHelper().convert(column.getDefaultValue(),
                    column.getMappedTypeCode(), Types.SMALLINT).toString();
        } else {
            return super.getNativeDefaultValue(column);
        }
    }

    @Override
    protected void writeColumnAutoIncrementStmt(Table table, Column column, StringBuilder ddl) {
        ddl.append("IDENTITY (1,1) ");
    }

    @Override
    public void writeExternalIndexDropStmt(Table table, IIndex index, StringBuilder ddl) {
        ddl.append("DROP INDEX ");
        printIdentifier(getTableName(table.getName()), ddl);
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
        printIdentifier(getTableName(table.getName()), ddl);
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
        if (delimitedIdentifierModeOn) {
            return "SET quoted_identifier on" + databaseInfo.getSqlCommandDelimiter() + "\n";
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
            printIdentifier(getTableName(targetTable.getName()), ddl);
            ddl.append(" ON");
            printEndOfStatement(ddl);
        }
        super.writeCopyDataStatement(sourceTable, targetTable, ddl);
        // We have to turn it off ASAP because it can be on only for one table
        // per session
        if (hasIdentityColumns) {
            ddl.append("SET IDENTITY_INSERT ");
            printIdentifier(getTableName(targetTable.getName()), ddl);
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
        /*
         * For column data type and size changes, we need to drop and then
         * re-create indexes and foreign keys using the column, as well as any
         * primary keys containg these columns However, if the index/foreign
         * key/primary key is already slated for removal or change, then we
         * don't want to generate change duplication
         */
        HashSet<IIndex> removedIndexes = new HashSet<IIndex>();
        HashSet<ForeignKey> removedForeignKeys = new HashSet<ForeignKey>();
        HashSet<Table> removedPKs = new HashSet<Table>();

        for (Iterator<IModelChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            IModelChange change = changeIt.next();

            if (change instanceof RemoveIndexChange) {
                removedIndexes.add(((RemoveIndexChange) change).getIndex());
            } else if (change instanceof RemoveForeignKeyChange) {
                removedForeignKeys.add(((RemoveForeignKeyChange) change).getForeignKey());
            } else if (change instanceof RemovePrimaryKeyChange) {
                removedPKs.add(((RemovePrimaryKeyChange) change).getChangedTable());
            }
        }

        ArrayList<TableChange> additionalChanges = new ArrayList<TableChange>();

        for (Iterator<IModelChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            IModelChange change = changeIt.next();

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
                    IIndex index = table.getIndex(idx);

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
        
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();
            if (change instanceof ColumnAutoIncrementChange) {
            /*
             * Sql Server has no way of adding or removing an IDENTITY
             * constraint thus we have to rebuild the table anyway and can
             * ignore all the other column changes
             */
                return;
            }
            
        }
        
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
        
        ArrayList<ColumnChange> columnChanges = new ArrayList<ColumnChange>();

        // Next we add/remove columns
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();

            if (change instanceof AddColumnChange) {
                AddColumnChange addColumnChange = (AddColumnChange) change;
                processChange(currentModel, desiredModel, addColumnChange, ddl);
                changeIt.remove();
            } else if (change instanceof RemoveColumnChange) {
                processChange(currentModel, desiredModel, (RemoveColumnChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof CopyColumnValueChange) {
                CopyColumnValueChange copyColumnChange = (CopyColumnValueChange)change;
                processChange(currentModel, desiredModel, copyColumnChange, ddl);
                changeIt.remove();                
            } else if ((change instanceof ColumnChange) && (columnChanges != null)) {
                /*
                 * We gather all changed columns because we can use the ALTER
                 * TABLE ALTER COLUMN statement for them
                 */
                columnChanges.add((ColumnChange) change);
            }
        }
        if (columnChanges != null) {
            HashSet<Column> processedColumns = new HashSet<Column>();

            for (Iterator<ColumnChange> changeIt = columnChanges.iterator(); changeIt.hasNext();) {
                ColumnChange change = changeIt.next();
                Column sourceColumn = change.getChangedColumn();
                Column targetColumn = targetTable.findColumn(sourceColumn.getName(),
                        delimitedIdentifierModeOn);

                if (!processedColumns.contains(targetColumn)) {
                    processColumnChange(sourceTable, targetTable, sourceColumn, targetColumn,
                            (change instanceof ColumnDataTypeChange)
                                    || (change instanceof ColumnSizeChange), ddl);
                    processedColumns.add(targetColumn);
                }
                changes.remove(change);
                change.apply(currentModel, delimitedIdentifierModeOn);
            }
        }
        // Finally we add primary keys
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();

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
        printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
        printIndent(ddl);
        ddl.append("ADD ");
        writeColumn(change.getChangedTable(), change.getNewColumn(), ddl);
        printEndOfStatement(ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    /*
     * Processes the removal of a column from a table.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            RemoveColumnChange change, StringBuilder ddl) {
        boolean hasDefault = change.getColumn().getParsedDefaultValue() != null;
        if (hasDefault) {
            dropDefaultConstraint(change.getChangedTable().getName(), change.getColumn().getName(), ddl);
        }
        
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
        // TODO: this would be easier when named primary keys are supported
        // because then we can use ALTER TABLE DROP
        String tableName = getTableName(change.getChangedTable().getName());
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
        change.apply(currentModel, delimitedIdentifierModeOn);
    }
    
    protected void dropDefaultConstraint(String tableName, String columnName, StringBuilder ddl) {         
        println(              "DECLARE @sql NVARCHAR(2000)                                                                  ", ddl);
        println(              "WHILE 1=1                                                                                    ", ddl);
        println(              "BEGIN                                                                                        ", ddl);
        println(String.format("SELECT TOP 1 @sql = N'alter table '%s' drop constraint ['+dc.NAME+N']'                       ", tableName), ddl);
        println(              "from sys.default_constraints dc                                                              ", ddl);
        println(              "JOIN sys.columns c                                                                           ", ddl);
        println(              "    ON c.default_object_id = dc.object_id                                                    ", ddl);
        println(              "WHERE                                                                                        ", ddl);
        println(String.format("    dc.parent_object_id = OBJECT_ID('%s')                                                    ", tableName), ddl);
        println(String.format("AND c.name = N'%s'                                                                           ", columnName), ddl);
        println(              "IF @@ROWCOUNT = 0 BREAK                                                                      ", ddl);
        println(              "EXEC (@sql)                                                                                  ", ddl);
        println(              "END                                                                                          ", ddl);
        printEndOfStatement(ddl);        
    }

    /*
     * Processes a change to a column.
     */
    protected void processColumnChange(Table sourceTable, Table targetTable, Column sourceColumn,
            Column targetColumn, boolean typeChange, StringBuilder ddl) {
        boolean hasDefault = sourceColumn.getParsedDefaultValue() != null;
        boolean shallHaveDefault = targetColumn.getParsedDefaultValue() != null;
        String newDefault = targetColumn.getDefaultValue();

        /*
         * Sql Server does not like it if there is a default spec in the ALTER
         * TABLE ALTER COLUMN statement; thus we have to change the default
         * manually
         */
        if (newDefault != null) {
            targetColumn.setDefaultValue(null);
        }
        if (hasDefault) {
            // we're dropping the old default
            String tableName = getTableName(sourceTable.getName());
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
        printlnIdentifier(getTableName(sourceTable.getName()), ddl);
        printIndent(ddl);
        ddl.append("ALTER COLUMN ");
        writeColumnTypeDefaultRequired(sourceTable, targetColumn, ddl);
        printEndOfStatement(ddl);

        if (shallHaveDefault) {
            targetColumn.setDefaultValue(newDefault);

            /*
             * if the column shall have a default, then we have to add it as a
             * constraint
             */
            ddl.append("ALTER TABLE ");
            printlnIdentifier(getTableName(sourceTable.getName()), ddl);
            printIndent(ddl);
            ddl.append("ADD CONSTRAINT ");
            printIdentifier(getConstraintName("DF", sourceTable, sourceColumn.getName(), null), ddl);
            writeColumnDefaultValueStmt(sourceTable, targetColumn, ddl);
            ddl.append(" FOR ");
            printIdentifier(getColumnName(sourceColumn), ddl);
            printEndOfStatement(ddl);
        }
    }

    /**
     * Creates a reasonably unique identifier only consisting of hexadecimal
     * characters and underscores. It looks like
     * <code>d578271282b42fce__2955b56e_107df3fbc96__8000</code> and is 48
     * characters long.
     * 
     * @return The identifier
     */
    protected String createUniqueIdentifier() {
        return new UID().toString().replace(':', '_').replace('-', '_');
    }
}
