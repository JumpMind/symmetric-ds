package org.jumpmind.symmetric.ddl.platform.mssql;

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

import java.io.IOException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.alteration.AddColumnChange;
import org.jumpmind.symmetric.ddl.alteration.AddForeignKeyChange;
import org.jumpmind.symmetric.ddl.alteration.AddIndexChange;
import org.jumpmind.symmetric.ddl.alteration.AddPrimaryKeyChange;
import org.jumpmind.symmetric.ddl.alteration.ColumnAutoIncrementChange;
import org.jumpmind.symmetric.ddl.alteration.ColumnChange;
import org.jumpmind.symmetric.ddl.alteration.ColumnDataTypeChange;
import org.jumpmind.symmetric.ddl.alteration.ColumnSizeChange;
import org.jumpmind.symmetric.ddl.alteration.PrimaryKeyChange;
import org.jumpmind.symmetric.ddl.alteration.RemoveColumnChange;
import org.jumpmind.symmetric.ddl.alteration.RemoveForeignKeyChange;
import org.jumpmind.symmetric.ddl.alteration.RemoveIndexChange;
import org.jumpmind.symmetric.ddl.alteration.RemovePrimaryKeyChange;
import org.jumpmind.symmetric.ddl.alteration.TableChange;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.ForeignKey;
import org.jumpmind.symmetric.ddl.model.Index;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.platform.CreationParameters;
import org.jumpmind.symmetric.ddl.platform.SqlBuilder;
import org.jumpmind.symmetric.ddl.util.Jdbc3Utils;

/*
 * The SQL Builder for the Microsoft SQL Server.
 * 
 * @version $Revision: 504014 $
 */
public class MSSqlBuilder extends SqlBuilder
{
    /* We use a generic date format. */
    private DateFormat _genericDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    /* We use a generic date format. */
    private DateFormat _genericTimeFormat = new SimpleDateFormat("HH:mm:ss");

    /*
     * Creates a new builder instance.
     * 
     * @param platform The plaftform this builder belongs to
     */
    public MSSqlBuilder(Platform platform)
    {
        super(platform);
        addEscapedCharSequence("'", "''");
    }

    /*
     * {@inheritDoc}
     */
    public void createTable(Database database, Table table, Map parameters) throws IOException
    {
        writeQuotationOnStatement();
        super.createTable(database, table, parameters);
    }

    /*
     * {@inheritDoc}
     */
    public void dropTable(Table table) throws IOException
    {
        String tableName         = getTableName(table);
        String tableNameVar      = "tn" + createUniqueIdentifier();
        String constraintNameVar = "cn" + createUniqueIdentifier();

        writeQuotationOnStatement();
        print("IF EXISTS (SELECT 1 FROM sysobjects WHERE type = 'U' AND name = ");
        printAlwaysSingleQuotedIdentifier(tableName);
        println(")");
        println("BEGIN");
        println("  DECLARE @" + tableNameVar + " nvarchar(256), @" + constraintNameVar + " nvarchar(256)");
        println("  DECLARE refcursor CURSOR FOR");
        println("  SELECT object_name(objs.parent_obj) tablename, objs.name constraintname");
        println("    FROM sysobjects objs JOIN sysconstraints cons ON objs.id = cons.constid");
        print("    WHERE objs.xtype != 'PK' AND object_name(objs.parent_obj) = ");
        printAlwaysSingleQuotedIdentifier(tableName);
        println("  OPEN refcursor");
        println("  FETCH NEXT FROM refcursor INTO @" + tableNameVar + ", @" + constraintNameVar);
        println("  WHILE @@FETCH_STATUS = 0");
        println("    BEGIN");
        println("      EXEC ('ALTER TABLE '+@" + tableNameVar + "+' DROP CONSTRAINT '+@" + constraintNameVar + ")");
        println("      FETCH NEXT FROM refcursor INTO @" + tableNameVar + ", @" + constraintNameVar);
        println("    END");
        println("  CLOSE refcursor");
        println("  DEALLOCATE refcursor");
        print("  DROP TABLE ");
        printlnIdentifier(tableName);
        print("END");
        printEndOfStatement();
    }

    /*
     * {@inheritDoc}
     */
    public void dropExternalForeignKeys(Table table) throws IOException
    {
        writeQuotationOnStatement();
        super.dropExternalForeignKeys(table);
    }

    /*
     * {@inheritDoc}
     */
    protected DateFormat getValueDateFormat()
    {
        return _genericDateFormat;
    }

    /*
     * {@inheritDoc}
     */
    protected DateFormat getValueTimeFormat()
    {
        return _genericTimeFormat;
    }

    /*
     * {@inheritDoc}
     */
    protected String getValueAsString(Column column, Object value)
    {
        if (value == null)
        {
            return "NULL";
        }

        StringBuffer result = new StringBuffer();

        switch (column.getTypeCode())
        {
            case Types.REAL:
            case Types.NUMERIC:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.DECIMAL:
                // SQL Server does not want quotes around the value
                if (!(value instanceof String) && (getValueNumberFormat() != null))
                {
                    result.append(getValueNumberFormat().format(value));
                }
                else
                {
                    result.append(value.toString());
                }
                break;
            case Types.DATE:
                result.append("CAST(");
                result.append(getPlatformInfo().getValueQuoteToken());
                result.append(value instanceof String ? (String)value : getValueDateFormat().format(value));
                result.append(getPlatformInfo().getValueQuoteToken());
                result.append(" AS datetime)");
                break;
            case Types.TIME:
                result.append("CAST(");
                result.append(getPlatformInfo().getValueQuoteToken());
                result.append(value instanceof String ? (String)value : getValueTimeFormat().format(value));
                result.append(getPlatformInfo().getValueQuoteToken());
                result.append(" AS datetime)");
                break;
            case Types.TIMESTAMP:
                result.append("CAST(");
                result.append(getPlatformInfo().getValueQuoteToken());
                result.append(value.toString());
                result.append(getPlatformInfo().getValueQuoteToken());
                result.append(" AS datetime)");
                break;
        }
        return super.getValueAsString(column, value);
    }

    /*
     * {@inheritDoc}
     */
    protected String getNativeDefaultValue(Column column)
    {
    	// Sql Server wants BIT default values as 0 or 1
        if ((column.getTypeCode() == Types.BIT) ||
            (Jdbc3Utils.supportsJava14JdbcTypes() && (column.getTypeCode() == Jdbc3Utils.determineBooleanTypeCode())))
        {
            return getDefaultValueHelper().convert(column.getDefaultValue(), column.getTypeCode(), Types.SMALLINT).toString();
        }
        else
        {
            return super.getNativeDefaultValue(column);
        }
    }

    /*
     * {@inheritDoc}
     */
    protected void writeColumnAutoIncrementStmt(Table table, Column column) throws IOException
    {
        print("IDENTITY (1,1) ");
    }

    /*
     * {@inheritDoc}
     */
    public void writeExternalIndexDropStmt(Table table, Index index) throws IOException
    {
        print("DROP INDEX ");
        printIdentifier(getTableName(table));
        print(".");
        printIdentifier(getIndexName(index));
        printEndOfStatement();
    }

    /*
     * {@inheritDoc}
     */
    protected void writeExternalForeignKeyDropStmt(Table table, ForeignKey foreignKey) throws IOException
    {
        String constraintName = getForeignKeyName(table, foreignKey);

        print("IF EXISTS (SELECT 1 FROM sysobjects WHERE type = 'F' AND name = ");
        printAlwaysSingleQuotedIdentifier(constraintName);
        println(")");
        printIndent();
        print("ALTER TABLE ");
        printIdentifier(getTableName(table));
        print(" DROP CONSTRAINT ");
        printIdentifier(constraintName);
        printEndOfStatement();
    }

    /*
     * Returns the statement that turns on the ability to write delimited identifiers.
     * 
     * @return The quotation-on statement
     */
    private String getQuotationOnStatement()
    {
        if (getPlatform().isDelimitedIdentifierModeOn())
        {
            return "SET quoted_identifier on" + getPlatformInfo().getSqlCommandDelimiter() + "\n";
        }
        else
        {
            return "";
        }
    }

    /*
     * Writes the statement that turns on the ability to write delimited identifiers.
     */
    private void writeQuotationOnStatement() throws IOException
    {
        print(getQuotationOnStatement());
    }

    /*
     * {@inheritDoc}
     */
    public String getSelectLastIdentityValues(Table table)
    {
        return "SELECT @@IDENTITY";
    }

    /*
     * Returns the SQL to enable identity override mode.
     * 
     * @param table The table to enable the mode for
     * @return The SQL
     */
    protected String getEnableIdentityOverrideSql(Table table)
    {
        StringBuffer result = new StringBuffer();

        result.append(getQuotationOnStatement());
        result.append("SET IDENTITY_INSERT ");
        result.append(getDelimitedIdentifier(getTableName(table)));
        result.append(" ON");
        result.append(getPlatformInfo().getSqlCommandDelimiter());

        return result.toString();
    }

    /*
     * Returns the SQL to disable identity override mode.
     * 
     * @param table The table to disable the mode for
     * @return The SQL
     */
    protected String getDisableIdentityOverrideSql(Table table)
    {
        StringBuffer result = new StringBuffer();

        result.append(getQuotationOnStatement());
        result.append("SET IDENTITY_INSERT ");
        result.append(getDelimitedIdentifier(getTableName(table)));
        result.append(" OFF");
        result.append(getPlatformInfo().getSqlCommandDelimiter());

        return result.toString();
    }

    /*
     * {@inheritDoc}
     */
    public String getDeleteSql(Table table, Map pkValues, boolean genPlaceholders)
    {
        return getQuotationOnStatement() + super.getDeleteSql(table, pkValues, genPlaceholders);
    }
    
    /*
     * {@inheritDoc}
     */
    public String getInsertSql(Table table, Map columnValues, boolean genPlaceholders)
    {
        return getQuotationOnStatement() + super.getInsertSql(table, columnValues, genPlaceholders);
    }

    /*
     * {@inheritDoc}
     */
    public String getUpdateSql(Table table, Map columnValues, boolean genPlaceholders)
    {
        return getQuotationOnStatement() + super.getUpdateSql(table, columnValues, genPlaceholders);
    }

    /*
     * Prints the given identifier with enforced single quotes around it regardless of whether 
     * delimited identifiers are turned on or not.
     * 
     * @param identifier The identifier
     */
    private void printAlwaysSingleQuotedIdentifier(String identifier) throws IOException
    {
        print("'");
        print(identifier);
        print("'");
    }

    /*
     * {@inheritDoc}
     */
    public void writeCopyDataStatement(Table sourceTable, Table targetTable) throws IOException
    {
        // Sql Server per default does not allow us to insert values explicitly into
        // identity columns. However, we can change this behavior
        boolean hasIdentityColumns = targetTable.getAutoIncrementColumns().length > 0;

        if (hasIdentityColumns)
        {
            print("SET IDENTITY_INSERT ");
            printIdentifier(getTableName(targetTable));
            print(" ON");
            printEndOfStatement();
        }
        super.writeCopyDataStatement(sourceTable, targetTable);
        // We have to turn it off ASAP because it can be on only for one table per session
        if (hasIdentityColumns)
        {
            print("SET IDENTITY_INSERT ");
            printIdentifier(getTableName(targetTable));
            print(" OFF");
            printEndOfStatement();
        }
    }

    /*
     * {@inheritDoc}
     */
    protected void processChanges(Database currentModel, Database desiredModel, List changes, CreationParameters params) throws IOException
    {
        if (!changes.isEmpty())
        {
            writeQuotationOnStatement();
        }
        // For column data type and size changes, we need to drop and then re-create indexes
        // and foreign keys using the column, as well as any primary keys containg
        // these columns
        // However, if the index/foreign key/primary key is already slated for removal or
        // change, then we don't want to generate change duplication
        HashSet removedIndexes     = new HashSet();
        HashSet removedForeignKeys = new HashSet();
        HashSet removedPKs         = new HashSet();

        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();)
        {
            Object change = changeIt.next();

            if (change instanceof RemoveIndexChange)
            {
                removedIndexes.add(((RemoveIndexChange)change).getIndex());
            }
            else if (change instanceof RemoveForeignKeyChange)
            {
                removedForeignKeys.add(((RemoveForeignKeyChange)change).getForeignKey());
            }
            else if (change instanceof RemovePrimaryKeyChange)
            {
                removedPKs.add(((RemovePrimaryKeyChange)change).getChangedTable());
            }
        }

        ArrayList additionalChanges = new ArrayList();

        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();)
        {
            Object change = changeIt.next();

            if ((change instanceof ColumnDataTypeChange) ||
                (change instanceof ColumnSizeChange))
            {
                Column column = ((ColumnChange)change).getChangedColumn();
                Table  table  = ((ColumnChange)change).getChangedTable();

                if (column.isPrimaryKey() && !removedPKs.contains(table))
                {
                    Column[] pk = table.getPrimaryKeyColumns();

                    additionalChanges.add(new RemovePrimaryKeyChange(table, pk));
                    additionalChanges.add(new AddPrimaryKeyChange(table, pk));
                    removedPKs.add(table);
                }
                for (int idx = 0; idx < table.getIndexCount(); idx++)
                {
                    Index index = table.getIndex(idx);

                    if (index.hasColumn(column) && !removedIndexes.contains(index))
                    {
                        additionalChanges.add(new RemoveIndexChange(table, index));
                        additionalChanges.add(new AddIndexChange(table, index));
                        removedIndexes.add(index);
                    }
                }
                for (int tableIdx = 0; tableIdx < currentModel.getTableCount(); tableIdx++)
                {
                    Table curTable = currentModel.getTable(tableIdx);

                    for (int fkIdx = 0; fkIdx < curTable.getForeignKeyCount(); fkIdx++)
                    {
                        ForeignKey curFk = curTable.getForeignKey(fkIdx);

                        if ((curFk.hasLocalColumn(column) || curFk.hasForeignColumn(column)) &&
                            !removedForeignKeys.contains(curFk))
                        {
                            additionalChanges.add(new RemoveForeignKeyChange(curTable, curFk));
                            additionalChanges.add(new AddForeignKeyChange(curTable, curFk));
                            removedForeignKeys.add(curFk);
                        }
                    }
                }
            }
        }
        changes.addAll(additionalChanges);
        super.processChanges(currentModel, desiredModel, changes, params);
    }

    /*
     * {@inheritDoc}
     */
    protected void processTableStructureChanges(Database currentModel,
                                                Database desiredModel,
                                                Table    sourceTable,
                                                Table    targetTable,
                                                Map      parameters,
                                                List     changes) throws IOException
    {
        // First we drop primary keys as necessary
        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();)
        {
            TableChange change = (TableChange)changeIt.next();

            if (change instanceof RemovePrimaryKeyChange)
            {
                processChange(currentModel, desiredModel, (RemovePrimaryKeyChange)change);
                changeIt.remove();
            }
            else if (change instanceof PrimaryKeyChange)
            {
                PrimaryKeyChange       pkChange       = (PrimaryKeyChange)change;
                RemovePrimaryKeyChange removePkChange = new RemovePrimaryKeyChange(pkChange.getChangedTable(),
                                                                                   pkChange.getOldPrimaryKeyColumns());

                processChange(currentModel, desiredModel, removePkChange);
            }
        }

        ArrayList columnChanges = new ArrayList();

        // Next we add/remove columns
        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();)
        {
            TableChange change = (TableChange)changeIt.next();

            if (change instanceof AddColumnChange)
            {
                AddColumnChange addColumnChange = (AddColumnChange)change;

                // Sql Server can only add not insert columns
                if (addColumnChange.isAtEnd())
                {
                    processChange(currentModel, desiredModel, addColumnChange);
                    changeIt.remove();
                }
            }
            else if (change instanceof RemoveColumnChange)
            {
                processChange(currentModel, desiredModel, (RemoveColumnChange)change);
                changeIt.remove();
            }
            else if (change instanceof ColumnAutoIncrementChange)
            {
                // Sql Server has no way of adding or removing an IDENTITY constraint
                // Thus we have to rebuild the table anyway and can ignore all the other 
                // column changes
                columnChanges = null;
            }
            else if ((change instanceof ColumnChange) && (columnChanges != null))
            {
                // we gather all changed columns because we can use the ALTER TABLE ALTER COLUMN
                // statement for them
                columnChanges.add(change);
            }
        }
        if (columnChanges != null)
        {
            HashSet processedColumns = new HashSet();

            for (Iterator changeIt = columnChanges.iterator(); changeIt.hasNext();)
            {
                ColumnChange change       = (ColumnChange)changeIt.next();
                Column       sourceColumn = change.getChangedColumn();
                Column       targetColumn = targetTable.findColumn(sourceColumn.getName(), getPlatform().isDelimitedIdentifierModeOn());

                if (!processedColumns.contains(targetColumn))
                {
                    processColumnChange(sourceTable,
                                        targetTable,
                                        sourceColumn,
                                        targetColumn,
                                        (change instanceof ColumnDataTypeChange) || (change instanceof ColumnSizeChange));
                    processedColumns.add(targetColumn);
                }
                changes.remove(change);
                change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
            }
        }
        // Finally we add primary keys
        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();)
        {
            TableChange change = (TableChange)changeIt.next();

            if (change instanceof AddPrimaryKeyChange)
            {
                processChange(currentModel, desiredModel, (AddPrimaryKeyChange)change);
                changeIt.remove();
            }
            else if (change instanceof PrimaryKeyChange)
            {
                PrimaryKeyChange    pkChange    = (PrimaryKeyChange)change;
                AddPrimaryKeyChange addPkChange = new AddPrimaryKeyChange(pkChange.getChangedTable(),
                                                                          pkChange.getNewPrimaryKeyColumns());

                processChange(currentModel, desiredModel, addPkChange);
                changeIt.remove();
            }
        }
    }

    /*
     * Processes the addition of a column to a table.
     * 
     * @param currentModel The current database schema
     * @param desiredModel The desired database schema
     * @param change       The change object
     */
    protected void processChange(Database        currentModel,
                                 Database        desiredModel,
                                 AddColumnChange change) throws IOException
    {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("ADD ");
        writeColumn(change.getChangedTable(), change.getNewColumn());
        printEndOfStatement();
        change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    }

    /*
     * Processes the removal of a column from a table.
     * 
     * @param currentModel The current database schema
     * @param desiredModel The desired database schema
     * @param change       The change object
     */
    protected void processChange(Database           currentModel,
                                 Database           desiredModel,
                                 RemoveColumnChange change) throws IOException
    {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("DROP COLUMN ");
        printIdentifier(getColumnName(change.getColumn()));
        printEndOfStatement();
        change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    }

    /*
     * Processes the removal of a primary key from a table.
     * 
     * @param currentModel The current database schema
     * @param desiredModel The desired database schema
     * @param change       The change object
     */
    protected void processChange(Database               currentModel,
                                 Database               desiredModel,
                                 RemovePrimaryKeyChange change) throws IOException
    {
        // TODO: this would be easier when named primary keys are supported
        //       because then we can use ALTER TABLE DROP
        String tableName         = getTableName(change.getChangedTable());
        String tableNameVar      = "tn" + createUniqueIdentifier();
        String constraintNameVar = "cn" + createUniqueIdentifier();

        println("BEGIN");
        println("  DECLARE @" + tableNameVar + " nvarchar(256), @" + constraintNameVar + " nvarchar(256)");
        println("  DECLARE refcursor CURSOR FOR");
        println("  SELECT object_name(objs.parent_obj) tablename, objs.name constraintname");
        println("    FROM sysobjects objs JOIN sysconstraints cons ON objs.id = cons.constid");
        print("    WHERE objs.xtype = 'PK' AND object_name(objs.parent_obj) = ");
        printAlwaysSingleQuotedIdentifier(tableName);
        println("  OPEN refcursor");
        println("  FETCH NEXT FROM refcursor INTO @" + tableNameVar + ", @" + constraintNameVar);
        println("  WHILE @@FETCH_STATUS = 0");
        println("    BEGIN");
        println("      EXEC ('ALTER TABLE '+@" + tableNameVar + "+' DROP CONSTRAINT '+@" + constraintNameVar + ")");
        println("      FETCH NEXT FROM refcursor INTO @" + tableNameVar + ", @" + constraintNameVar);
        println("    END");
        println("  CLOSE refcursor");
        println("  DEALLOCATE refcursor");
        print("END");
        printEndOfStatement();
        change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    }

    /*
     * Processes a change to a column.
     * 
     * @param sourceTable  The current table
     * @param targetTable  The desired table
     * @param sourceColumn The current column
     * @param targetColumn The desired column
     * @param typeChange   Whether this is a type change
     */
    protected void processColumnChange(Table   sourceTable,
                                       Table   targetTable,
                                       Column  sourceColumn,
                                       Column  targetColumn,
                                       boolean typeChange) throws IOException
    {
        boolean hasDefault       = sourceColumn.getParsedDefaultValue() != null;
        boolean shallHaveDefault = targetColumn.getParsedDefaultValue() != null;
        String  newDefault       = targetColumn.getDefaultValue();

        // Sql Server does not like it if there is a default spec in the ALTER TABLE ALTER COLUMN
        // statement; thus we have to change the default manually
        if (newDefault != null)
        {
            targetColumn.setDefaultValue(null);
        }
        if (hasDefault)
        {
            // we're dropping the old default
            String tableName         = getTableName(sourceTable);
            String columnName        = getColumnName(sourceColumn);
            String tableNameVar      = "tn" + createUniqueIdentifier();
            String constraintNameVar = "cn" + createUniqueIdentifier();

            println("BEGIN");
            println("  DECLARE @" + tableNameVar + " nvarchar(256), @" + constraintNameVar + " nvarchar(256)");
            println("  DECLARE refcursor CURSOR FOR");
            println("  SELECT object_name(objs.parent_obj) tablename, objs.name constraintname");
            println("    FROM sysobjects objs JOIN sysconstraints cons ON objs.id = cons.constid");
            println("    WHERE objs.xtype = 'D' AND");
            print("          cons.colid = (SELECT colid FROM syscolumns WHERE id = object_id(");
            printAlwaysSingleQuotedIdentifier(tableName);
            print(") AND name = ");
            printAlwaysSingleQuotedIdentifier(columnName);
            println(") AND");
            print("          object_name(objs.parent_obj) = ");
            printAlwaysSingleQuotedIdentifier(tableName);
            println("  OPEN refcursor");
            println("  FETCH NEXT FROM refcursor INTO @" + tableNameVar + ", @" + constraintNameVar);
            println("  WHILE @@FETCH_STATUS = 0");
            println("    BEGIN");
            println("      EXEC ('ALTER TABLE '+@" + tableNameVar + "+' DROP CONSTRAINT '+@" + constraintNameVar + ")");
            println("      FETCH NEXT FROM refcursor INTO @" + tableNameVar + ", @" + constraintNameVar);
            println("    END");
            println("  CLOSE refcursor");
            println("  DEALLOCATE refcursor");
            print("END");
            printEndOfStatement();
        }

        print("ALTER TABLE ");
        printlnIdentifier(getTableName(sourceTable));
        printIndent();
        print("ALTER COLUMN ");
        writeColumn(sourceTable, targetColumn);
        printEndOfStatement();

        if (shallHaveDefault)
        {
            targetColumn.setDefaultValue(newDefault);

            // if the column shall have a default, then we have to add it as a constraint
            print("ALTER TABLE ");
            printlnIdentifier(getTableName(sourceTable));
            printIndent();
            print("ADD CONSTRAINT ");
            printIdentifier(getConstraintName("DF", sourceTable, sourceColumn.getName(), null));
            writeColumnDefaultValueStmt(sourceTable, targetColumn);
            print(" FOR ");
            printIdentifier(getColumnName(sourceColumn));
            printEndOfStatement();
        }
    }
}
