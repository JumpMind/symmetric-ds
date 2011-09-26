package org.jumpmind.symmetric.ddl.platform.sybase;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.alteration.AddColumnChange;
import org.jumpmind.symmetric.ddl.alteration.AddPrimaryKeyChange;
import org.jumpmind.symmetric.ddl.alteration.ColumnAutoIncrementChange;
import org.jumpmind.symmetric.ddl.alteration.ColumnChange;
import org.jumpmind.symmetric.ddl.alteration.ColumnDefaultValueChange;
import org.jumpmind.symmetric.ddl.alteration.PrimaryKeyChange;
import org.jumpmind.symmetric.ddl.alteration.RemoveColumnChange;
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

/**
 * The SQL Builder for Sybase.
 * 
 * @version $Revision: 532976 $
 */
public class SybaseBuilder extends SqlBuilder
{
    /**
     * Creates a new builder instance.
     * 
     * @param platform The plaftform this builder belongs to
     */
    public SybaseBuilder(Platform platform)
    {
        super(platform);
        addEscapedCharSequence("'", "''");
    }

    /**
     * {@inheritDoc}
     */
    public void createTable(Database database, Table table, Map parameters) throws IOException
    {
        writeQuotationOnStatement();
        super.createTable(database, table, parameters);
    }

    /**
     * {@inheritDoc}
     */
	protected void writeTableCreationStmtEnding(Table table, Map parameters) throws IOException
    {
        if (parameters != null)
        {
            // We support
            // - 'lock'
            // - 'at'
            // - 'external table at'
            // - 'on'
            // - with parameters as name value pairs

            String lockValue            = (String)parameters.get("lock");
            String atValue              = (String)parameters.get("at");
            String externalTableAtValue = (String)parameters.get("external table at");
            String onValue              = (String)parameters.get("on");

            if (lockValue != null)
            {
                print(" lock ");
                print(lockValue);
            }

            boolean writtenWithParameters = false;

            for (Iterator it = parameters.entrySet().iterator(); it.hasNext();)
            {
                Map.Entry entry = (Map.Entry)it.next();
                String    name  = entry.getKey().toString();

                if (!"lock".equals(name) && !"at".equals(name) && !"external table at".equals(name) && !"on".equals(name))
                {
                    if (!writtenWithParameters)
                    {
                        print(" with ");
                        writtenWithParameters = true;
                    }
                    else
                    {
                        print(", ");
                    }
                    print(name);
                    if (entry.getValue() != null)
                    {
                        print("=");
                        print(entry.getValue().toString());
                    }
                }
            }
            if (onValue != null)
            {
                print(" on ");
                print(onValue);
            }
            if (externalTableAtValue != null)
            {
                print(" external table at \"");
                print(externalTableAtValue);
                print("\"");
            }
            else if (atValue != null)
            {
                print(" at \"");
                print(atValue);
                print("\"");
            }
        }
        super.writeTableCreationStmtEnding(table, parameters);
    }

    /**
	 * {@inheritDoc}
	 */
	protected void writeColumn(Table table, Column column) throws IOException
	{
        printIdentifier(getColumnName(column));
        print(" ");
        print(getSqlType(column));
        writeColumnDefaultValueStmt(table, column);
        // Sybase does not like NULL/NOT NULL and IDENTITY together
        if (column.isAutoIncrement())
        {
            print(" ");
            writeColumnAutoIncrementStmt(table, column);
        }
        else
        {
            print(" ");
            if (column.isRequired())
            {
                writeColumnNotNullableStmt();
            }
            else
            {
                // we'll write a NULL for all columns that are not required 
                writeColumnNullableStmt();
            }
        }
	}

	/**
     * {@inheritDoc}
     */
    protected String getNativeDefaultValue(Column column)
    {
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

    /**
     * {@inheritDoc}
     */
    public void dropTable(Table table) throws IOException
    {
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

    /**
     * {@inheritDoc}
     */
    protected void writeExternalForeignKeyDropStmt(Table table, ForeignKey foreignKey) throws IOException
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

    /**
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

    /**
     * {@inheritDoc}
     */
    public void dropExternalForeignKeys(Table table) throws IOException
    {
        writeQuotationOnStatement();
        super.dropExternalForeignKeys(table);
    }

    /**
     * {@inheritDoc}
     */
    public String getSelectLastIdentityValues(Table table)
    {
        return "SELECT @@IDENTITY";
    }

    /**
     * Returns the SQL to enable identity override mode.
     * 
     * @param table The table to enable the mode for
     * @return The SQL
     */
    protected String getEnableIdentityOverrideSql(Table table)
    {
        StringBuffer result = new StringBuffer();

        result.append("SET IDENTITY_INSERT ");
        result.append(getDelimitedIdentifier(getTableName(table)));
        result.append(" ON");

        return result.toString();
    }

    /**
     * Returns the SQL to disable identity override mode.
     * 
     * @param table The table to disable the mode for
     * @return The SQL
     */
    protected String getDisableIdentityOverrideSql(Table table)
    {
        StringBuffer result = new StringBuffer();

        result.append("SET IDENTITY_INSERT ");
        result.append(getDelimitedIdentifier(getTableName(table)));
        result.append(" OFF");

        return result.toString();
    }

    /**
     * Returns the statement that turns on the ability to write delimited identifiers.
     * 
     * @return The quotation-on statement
     */
    protected String getQuotationOnStatement()
    {
        if (getPlatform().isDelimitedIdentifierModeOn())
        {
            return "SET quoted_identifier on";
        }
        else
        {
            return "";
        }
    }

    /**
     * Writes the statement that turns on the ability to write delimited identifiers.
     */
    private void writeQuotationOnStatement() throws IOException
    {
        print(getQuotationOnStatement());
        printEndOfStatement();
    }

    /**
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

    /**
     * {@inheritDoc}
     */
    public void writeCopyDataStatement(Table sourceTable, Table targetTable) throws IOException
    {
        boolean hasIdentity = targetTable.getAutoIncrementColumns().length > 0;

        if (hasIdentity)
        {
            print("SET IDENTITY_INSERT ");
            printIdentifier(getTableName(targetTable));
            print(" ON");
            printEndOfStatement();
        }
        super.writeCopyDataStatement(sourceTable, targetTable);
        if (hasIdentity)
        {
            print("SET IDENTITY_INSERT ");
            printIdentifier(getTableName(targetTable));
            print(" OFF");
            printEndOfStatement();
        }
    }

    /**
     * {@inheritDoc}
     */
    protected void writeCastExpression(Column sourceColumn, Column targetColumn) throws IOException
    {
        String sourceNativeType = getBareNativeType(sourceColumn);
        String targetNativeType = getBareNativeType(targetColumn);

        if (sourceNativeType.equals(targetNativeType))
        {
            printIdentifier(getColumnName(sourceColumn));
        }
        else
        {
            print("CONVERT(");
            print(getNativeType(targetColumn));
            print(",");
            printIdentifier(getColumnName(sourceColumn));
            print(")");
        }
    }

    /**
     * {@inheritDoc}
     */
    protected void processChanges(Database currentModel, Database desiredModel, List changes, CreationParameters params) throws IOException
    {
        if (!changes.isEmpty())
        {
            writeQuotationOnStatement();
        }
        super.processChanges(currentModel, desiredModel, changes, params);
    }

    /**
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


        HashMap columnChanges = new HashMap();

        // Next we add/remove columns
        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();)
        {
            TableChange change = (TableChange)changeIt.next();

            if (change instanceof AddColumnChange)
            {
                AddColumnChange addColumnChange = (AddColumnChange)change;

                // Sybase can only add not insert columns
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
                // Sybase has no way of adding or removing an IDENTITY constraint
                // Thus we have to rebuild the table anyway and can ignore all the other 
                // column changes
                columnChanges = null;
            }
            else if ((change instanceof ColumnChange) && (columnChanges != null))
            {
                // we gather all changed columns because we can use the ALTER TABLE ALTER COLUMN
                // statement for them
                ColumnChange columnChange     = (ColumnChange)change;
                ArrayList    changesPerColumn = (ArrayList)columnChanges.get(columnChange.getChangedColumn());

                if (changesPerColumn == null)
                {
                    changesPerColumn = new ArrayList();
                    columnChanges.put(columnChange.getChangedColumn(), changesPerColumn);
                }
                changesPerColumn.add(change);
            }
        }
        if (columnChanges != null)
        {
            for (Iterator changesPerColumnIt = columnChanges.entrySet().iterator(); changesPerColumnIt.hasNext();)
            {
                Map.Entry entry            = (Map.Entry)changesPerColumnIt.next();
                Column    sourceColumn     = (Column)entry.getKey();
                ArrayList changesPerColumn = (ArrayList)entry.getValue();

                // Sybase does not like us to use the ALTER TABLE ALTER statement if we don't actually
                // change the datatype or the required constraint but only the default value
                // Thus, if we only have to change the default, we use a different handler
                if ((changesPerColumn.size() == 1) && (changesPerColumn.get(0) instanceof ColumnDefaultValueChange))
                {
                    processChange(currentModel,
                                  desiredModel,
                                  (ColumnDefaultValueChange)changesPerColumn.get(0));
                }
                else
                {
                    Column targetColumn = targetTable.findColumn(sourceColumn.getName(),
                                                                 getPlatform().isDelimitedIdentifierModeOn());

                    processColumnChange(sourceTable, targetTable, sourceColumn, targetColumn);
                }
                for (Iterator changeIt = changesPerColumn.iterator(); changeIt.hasNext();)
                {
                    ((ColumnChange)changeIt.next()).apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
                }
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


    /**
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

    /**
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
        print("DROP ");
        printIdentifier(getColumnName(change.getColumn()));
        printEndOfStatement();
        change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    }

    /**
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
        println("  DECLARE @" + tableNameVar + " nvarchar(60), @" + constraintNameVar + " nvarchar(60)");
        println("  WHILE EXISTS(SELECT sysindexes.name");
        println("                 FROM sysindexes, sysobjects");
        print("                 WHERE sysobjects.name = ");
        printAlwaysSingleQuotedIdentifier(tableName);
        println(" AND sysobjects.id = sysindexes.id AND (sysindexes.status & 2048) > 0)");
        println("  BEGIN");
        println("    SELECT @" + tableNameVar + " = sysobjects.name, @" + constraintNameVar + " = sysindexes.name");
        println("      FROM sysindexes, sysobjects");
        print("      WHERE sysobjects.name = ");
        printAlwaysSingleQuotedIdentifier(tableName);
        print(" AND sysobjects.id = sysindexes.id AND (sysindexes.status & 2048) > 0");
        println("    EXEC ('ALTER TABLE '+@" + tableNameVar + "+' DROP CONSTRAINT '+@" + constraintNameVar + ")");
        println("  END");
        print("END");
        printEndOfStatement();
        change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    }

    /**
     * Processes the change of the default value of a column. Note that this method is only
     * used if it is the only change to that column.
     * 
     * @param currentModel The current database schema
     * @param desiredModel The desired database schema
     * @param change       The change object
     */
    protected void processChange(Database                 currentModel,
                                 Database                 desiredModel,
                                 ColumnDefaultValueChange change) throws IOException
    {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("REPLACE ");
        printIdentifier(getColumnName(change.getChangedColumn()));

        Table  curTable  = currentModel.findTable(change.getChangedTable().getName(), getPlatform().isDelimitedIdentifierModeOn());
        Column curColumn = curTable.findColumn(change.getChangedColumn().getName(), getPlatform().isDelimitedIdentifierModeOn());

        print(" DEFAULT ");
        if (isValidDefaultValue(change.getNewDefaultValue(), curColumn.getTypeCode()))
        {
            printDefaultValue(change.getNewDefaultValue(), curColumn.getTypeCode());
        }
        else
        {
            print("NULL");
        }
        printEndOfStatement();
        change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    }

    /**
     * Processes a change to a column.
     * 
     * @param sourceTable  The current table
     * @param targetTable  The desired table
     * @param sourceColumn The current column
     * @param targetColumn The desired column
     */
    protected void processColumnChange(Table  sourceTable,
                                       Table  targetTable,
                                       Column sourceColumn,
                                       Column targetColumn) throws IOException
    {
        Object oldParsedDefault = sourceColumn.getParsedDefaultValue();
        Object newParsedDefault = targetColumn.getParsedDefaultValue();
        String newDefault       = targetColumn.getDefaultValue();
        boolean defaultChanges  = ((oldParsedDefault == null) && (newParsedDefault != null)) ||
                                  ((oldParsedDefault != null) && !oldParsedDefault.equals(newParsedDefault));

        // Sybase does not like it if there is a default spec in the ALTER TABLE ALTER
        // statement; thus we have to change the default afterwards
        if (newDefault != null)
        {
            targetColumn.setDefaultValue(null);
        }
        if (defaultChanges)
        {
            // we're first removing the default as it might make problems when the
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
        if (defaultChanges)
        {
            print("ALTER TABLE ");
            printlnIdentifier(getTableName(sourceTable));
            printIndent();
            print("REPLACE ");
            printIdentifier(getColumnName(sourceColumn));
            if (newDefault != null)
            {
                writeColumnDefaultValueStmt(sourceTable, targetColumn);
            }
            else
            {
                print(" DEFAULT NULL");
            }
            printEndOfStatement();
        }
   }
}
