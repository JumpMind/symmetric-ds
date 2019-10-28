package org.jumpmind.db.platform.sqlanywhere;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.alter.AddColumnChange;
import org.jumpmind.db.alter.AddPrimaryKeyChange;
import org.jumpmind.db.alter.ColumnAutoIncrementChange;
import org.jumpmind.db.alter.ColumnChange;
import org.jumpmind.db.alter.ColumnDefaultValueChange;
import org.jumpmind.db.alter.CopyColumnValueChange;
import org.jumpmind.db.alter.IModelChange;
import org.jumpmind.db.alter.PrimaryKeyChange;
import org.jumpmind.db.alter.RemoveColumnChange;
import org.jumpmind.db.alter.RemovePrimaryKeyChange;
import org.jumpmind.db.alter.TableChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.ForeignKey.ForeignKeyAction;
import org.jumpmind.db.platform.AbstractDdlBuilder;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.PlatformUtils;

/*
 * The SQL Builder for Sybase.
 */
public class SqlAnywhereDdlBuilder extends AbstractDdlBuilder {

    public SqlAnywhereDdlBuilder() {
        super(DatabaseNamesConstants.SQLANYWHERE);
        
        databaseInfo.setMaxIdentifierLength(128);
        databaseInfo.setNullAsDefaultValueRequired(true);
        databaseInfo.setCommentPrefix("/*");
        databaseInfo.setCommentSuffix("*/");
        databaseInfo.setDelimiterToken("\"");

        databaseInfo.addNativeTypeMapping(Types.ARRAY, "IMAGE");
        // we're not using the native BIT type because it is rather limited
        // (cannot be NULL, cannot be indexed)
        databaseInfo.addNativeTypeMapping(Types.BIT, "SMALLINT", Types.SMALLINT);
        databaseInfo.addNativeTypeMapping(Types.BLOB, "IMAGE", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.CLOB, "TEXT", Types.LONGVARCHAR);
        databaseInfo.addNativeTypeMapping(Types.DATE, "DATETIME", Types.TIMESTAMP);
        databaseInfo.addNativeTypeMapping(Types.DISTINCT, "IMAGE", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.DOUBLE, "DOUBLE PRECISION");
        databaseInfo.addNativeTypeMapping(Types.FLOAT, "DOUBLE PRECISION", Types.DOUBLE);
        databaseInfo.addNativeTypeMapping(Types.INTEGER, "INT");
        databaseInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "IMAGE", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.LONGVARBINARY, "IMAGE");
        databaseInfo.addNativeTypeMapping(Types.LONGVARCHAR, "TEXT");
        databaseInfo.addNativeTypeMapping(Types.NULL, "IMAGE", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.OTHER, "IMAGE", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.REF, "IMAGE", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.STRUCT, "IMAGE", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.TIME, "DATETIME", Types.TIMESTAMP);
        databaseInfo.addNativeTypeMapping(Types.TIMESTAMP, "DATETIME", Types.TIMESTAMP);
        databaseInfo.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);
        databaseInfo.addNativeTypeMapping("BOOLEAN", "SMALLINT", "SMALLINT");
        databaseInfo.addNativeTypeMapping("DATALINK", "IMAGE", "LONGVARBINARY");

        databaseInfo.setDefaultSize(Types.BINARY, 254);
        databaseInfo.setDefaultSize(Types.VARBINARY, 254);
        databaseInfo.setDefaultSize(Types.CHAR, 254);
        databaseInfo.setDefaultSize(Types.VARCHAR, 254);

        databaseInfo.setDateOverridesToTimestamp(true);
        databaseInfo.setNonBlankCharColumnSpacePadded(false);
        databaseInfo.setBlankCharColumnSpacePadded(false);
        databaseInfo.setCharColumnSpaceTrimmed(false);
        databaseInfo.setEmptyStringNulled(false);
        databaseInfo.setAutoIncrementUpdateAllowed(false);
        databaseInfo.setRequiresAutoCommitForDdl(true);
        databaseInfo.setRequiredCharColumnEmptyStringSameAsNull(true);
        
    }

    @Override
    protected void createTable(Table table, StringBuilder ddl, boolean temporary, boolean recreate) {
        writeQuotationOnStatement(ddl);
        super.createTable(table, ddl, temporary, recreate);
    }

    @Override
    protected void writeColumn(Table table, Column column, StringBuilder ddl) {
        printIdentifier(getColumnName(column), ddl);
        ddl.append(" ");
        ddl.append(getSqlType(column));
        writeColumnDefaultValueStmt(table, column, ddl);
        // Sybase does not like NULL/NOT NULL and IDENTITY together
        if (column.isAutoIncrement()) {
            ddl.append(" ");
            writeColumnAutoIncrementStmt(table, column, ddl);
        } else {
            ddl.append(" ");
            if (column.isRequired()) {
                writeColumnNotNullableStmt(ddl);
            } else {
                // we'll write a NULL for all columns that are not required
                writeColumnNullableStmt(ddl);
            }
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
    protected void dropTable(Table table, StringBuilder ddl, boolean temporary, boolean recreate) {
        writeQuotationOnStatement(ddl);
        ddl.append("IF EXISTS (SELECT 1 FROM dbo.sysobjects WHERE type = 'U' AND name = ");
        printAlwaysSingleQuotedIdentifier(getTableName(table.getName()), ddl);
        println(")", ddl);
        println("BEGIN", ddl);
        printIndent(ddl);
        ddl.append("DROP TABLE ");
        ddl.append(getFullyQualifiedTableNameShorten(table));
        ddl.append(" END");
        printEndOfStatement(ddl);
    }

    @Override
    protected void writeExternalForeignKeyDropStmt(Table table, ForeignKey foreignKey,
            StringBuilder ddl) {
        String constraintName = getForeignKeyName(table, foreignKey);

        ddl.append("IF EXISTS (SELECT 1 FROM dbo.sysobjects WHERE type = 'RI' AND name = ");
        printAlwaysSingleQuotedIdentifier(constraintName, ddl);
        println(")", ddl);
        printIndent(ddl);
        ddl.append("ALTER TABLE ");
        ddl.append(getFullyQualifiedTableNameShorten(table));
        ddl.append(" DROP CONSTRAINT ");
        printIdentifier(constraintName, ddl);
        printEndOfStatement(ddl);
    }

    @Override
    public void writeExternalIndexDropStmt(Table table, IIndex index, StringBuilder ddl) {
        ddl.append("DROP INDEX ");
        ddl.append(getFullyQualifiedTableNameShorten(table));
        ddl.append(".");
        printIdentifier(getIndexName(index), ddl);
        printEndOfStatement(ddl);
    }

    @Override
    public void dropExternalForeignKeys(Table table, StringBuilder ddl) {
        writeQuotationOnStatement(ddl);
        super.dropExternalForeignKeys(table, ddl);
    }

    @Override
    public String getSelectLastIdentityValues(Table table) {
        return "SELECT @@IDENTITY";
    }

    /*
     * Returns the statement that turns on the ability to write delimited
     * identifiers.
     *
     * @return The quotation-on statement
     */
    protected String getQuotationOnStatement() {
        if (delimitedIdentifierModeOn) {
            return "SET quoted_identifier on";
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
        printEndOfStatement(ddl);
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
    protected void writeCastExpression(Column sourceColumn, Column targetColumn, StringBuilder ddl) {
        String sourceNativeType = getBareNativeType(sourceColumn);
        String targetNativeType = getBareNativeType(targetColumn);

        if (sourceNativeType.equals(targetNativeType)) {
            printIdentifier(getColumnName(sourceColumn), ddl);
        } else {
            ddl.append("CONVERT(");
            ddl.append(getNativeType(targetColumn));
            ddl.append(",");
            printIdentifier(getColumnName(sourceColumn), ddl);
            ddl.append(")");
        }
    }

    @Override
    protected void processChanges(Database currentModel, Database desiredModel,
            List<IModelChange> changes, StringBuilder ddl) {
        if (!changes.isEmpty()) {
            writeQuotationOnStatement(ddl);
        }
        super.processChanges(currentModel, desiredModel, changes, ddl);
    }

    @Override
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            Table sourceTable, Table targetTable, List<TableChange> changes, StringBuilder ddl) {
        // First we drop primary keys as necessary
        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = (TableChange) changeIt.next();

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

        HashMap columnChanges = new HashMap();

        // Next we add/remove columns
        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = (TableChange) changeIt.next();

            if (change instanceof AddColumnChange) {
                AddColumnChange addColumnChange = (AddColumnChange) change;

                // Sybase can only add not insert columns
                if (addColumnChange.isAtEnd()) {
                    processChange(currentModel, desiredModel, addColumnChange, ddl);
                    changeIt.remove();
                }
            } else if (change instanceof RemoveColumnChange) {
                processChange(currentModel, desiredModel, (RemoveColumnChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof CopyColumnValueChange) {
                CopyColumnValueChange copyColumnChange = (CopyColumnValueChange)change;
                processChange(currentModel, desiredModel, copyColumnChange, ddl);
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
                            (ColumnDefaultValueChange) changesPerColumn.get(0), ddl);
                } else {
                    Column targetColumn = targetTable.findColumn(sourceColumn.getName(),
                            delimitedIdentifierModeOn);

                    processColumnChange(sourceTable, targetTable, sourceColumn, targetColumn, ddl);
                }
                for (Iterator changeIt = changesPerColumn.iterator(); changeIt.hasNext();) {
                    ((ColumnChange) changeIt.next()).apply(currentModel,
                            delimitedIdentifierModeOn);
                }
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
        ddl.append(getFullyQualifiedTableNameShorten(change.getChangedTable()));
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
        ddl.append("ALTER TABLE ");
        ddl.append(getFullyQualifiedTableNameShorten(change.getChangedTable()));
        printIndent(ddl);
        ddl.append("DROP ");
        printIdentifier(getColumnName(change.getColumn()), ddl);
        printEndOfStatement(ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
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
            RemovePrimaryKeyChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        ddl.append(getFullyQualifiedTableNameShorten(change.getChangedTable()));
        printIndent(ddl);
        ddl.append("DROP PRIMARY KEY");
        printEndOfStatement(ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    /*
     * Processes the change of the default value of a column. Note that this
     * method is only used if it is the only change to that column.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            ColumnDefaultValueChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        ddl.append(getFullyQualifiedTableNameShorten(change.getChangedTable()));
        printIndent(ddl);
        ddl.append("REPLACE ");
        printIdentifier(getColumnName(change.getChangedColumn()), ddl);

        Table curTable = currentModel.findTable(change.getChangedTable().getName(),
                delimitedIdentifierModeOn);
        Column curColumn = curTable.findColumn(change.getChangedColumn().getName(),
                delimitedIdentifierModeOn);

        ddl.append(" DEFAULT ");
        if (isValidDefaultValue(change.getNewDefaultValue(), curColumn.getMappedTypeCode())) {
            printDefaultValue(change.getNewDefaultValue(), curColumn.getMappedTypeCode(), ddl);
        } else {
            ddl.append("NULL");
        }
        printEndOfStatement(ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    /*
     * Processes a change to a column.
     */
    protected void processColumnChange(Table sourceTable, Table targetTable, Column sourceColumn,
            Column targetColumn, StringBuilder ddl) {
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
            ddl.append("ALTER TABLE ");
            ddl.append(getFullyQualifiedTableNameShorten(sourceTable));
            printIndent(ddl);
            ddl.append("REPLACE ");
            printIdentifier(getColumnName(sourceColumn), ddl);
            ddl.append(" DEFAULT NULL");
            printEndOfStatement(ddl);
        }
        ddl.append("ALTER TABLE ");
        ddl.append(getFullyQualifiedTableNameShorten(sourceTable));
        printIndent(ddl);
        ddl.append("MODIFY ");
        writeColumn(sourceTable, targetColumn, ddl);
        printEndOfStatement(ddl);
        if (defaultChanges) {
            ddl.append("ALTER TABLE ");
            ddl.append(getFullyQualifiedTableNameShorten(sourceTable));
            printIndent(ddl);
            ddl.append("REPLACE ");
            printIdentifier(getColumnName(sourceColumn), ddl);
            if (newDefault != null) {
                targetColumn.setDefaultValue(newDefault);
                writeColumnDefaultValueStmt(sourceTable, targetColumn, ddl);
            } else {
                ddl.append(" DEFAULT NULL");
            }
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
    
    @Override
    protected void writeCascadeAttributesForForeignKeyUpdate(ForeignKey key, StringBuilder ddl) {
        // SQLAnywhere does not support ON UPDATE NO ACTION, but NOACTION is just like RESTRICT
        ForeignKeyAction original = key.getOnUpdateAction();
        if(key.getOnUpdateAction().equals(ForeignKeyAction.NOACTION)) {
            key.setOnUpdateAction(ForeignKeyAction.RESTRICT);
        }
        super.writeCascadeAttributesForForeignKeyUpdate(key, ddl);
        key.setOnUpdateAction(original);
    }
    
    @Override
    protected void writeCascadeAttributesForForeignKeyDelete(ForeignKey key, StringBuilder ddl) {
        // Firebird does not support ON DELETE NO ACTION, but NOACTION is just like RESTRICT
        ForeignKeyAction original = key.getOnDeleteAction();
        if(key.getOnDeleteAction().equals(ForeignKeyAction.NOACTION)) {
            key.setOnDeleteAction(ForeignKeyAction.RESTRICT);
        }
        super.writeCascadeAttributesForForeignKeyDelete(key, ddl);
        key.setOnDeleteAction(original);
    }
}
