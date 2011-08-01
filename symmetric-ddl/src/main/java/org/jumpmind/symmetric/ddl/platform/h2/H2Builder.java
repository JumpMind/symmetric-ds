/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.jumpmind.symmetric.ddl.platform.h2;

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
import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.alteration.AddColumnChange;
import org.jumpmind.symmetric.ddl.alteration.ColumnAutoIncrementChange;
import org.jumpmind.symmetric.ddl.alteration.ColumnChange;
import org.jumpmind.symmetric.ddl.alteration.ColumnDataTypeChange;
import org.jumpmind.symmetric.ddl.alteration.ColumnRequiredChange;
import org.jumpmind.symmetric.ddl.alteration.ColumnSizeChange;
import org.jumpmind.symmetric.ddl.alteration.RemoveColumnChange;
import org.jumpmind.symmetric.ddl.alteration.TableChange;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.Index;
import org.jumpmind.symmetric.ddl.model.ModelException;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.model.TypeMap;
import org.jumpmind.symmetric.ddl.platform.CreationParameters;
import org.jumpmind.symmetric.ddl.platform.SqlBuilder;

/**
 * The SQL Builder for the H2 database. From patch <a
 * href="https://issues.apache.org/jira/browse/DDLUTILS-185"
 * >https://issues.apache.org/jira/browse/DDLUTILS-185</a>
 */
public class H2Builder extends SqlBuilder {

    public H2Builder(Platform platform) {
        super(platform);
        addEscapedCharSequence("'", "''");
    }

    @Override
    public void dropTable(Table table) throws IOException {
        print("DROP TABLE ");
        printIdentifier(getTableName(table));
        print(" IF EXISTS");
        printEndOfStatement();
    }

    @Override
    public String getSelectLastIdentityValues(Table table) {
        return "CALL IDENTITY()";
    }

    @Override
    public void writeExternalIndexDropStmt(Table table, Index index) throws IOException {
        print("DROP INDEX IF EXISTS ");
        printIdentifier(getIndexName(index));
        printEndOfStatement();
    }

    @Override
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            CreationParameters params, Collection<TableChange> changes) throws IOException {

        // Only drop columns that are not part of a primary key
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();
            if ((change instanceof RemoveColumnChange) && ((RemoveColumnChange) change).getColumn().isPrimaryKey()) {
                changeIt.remove();
            }
        }

        // in order to utilize the ALTER TABLE ADD COLUMN BEFORE statement
        // we have to apply the add column changes in the correct order
        // thus we first gather all add column changes and then execute them
        // Since we get them in target table column order, we can simply
        // iterate backwards
        ArrayList<AddColumnChange> addColumnChanges = new ArrayList<AddColumnChange>();

        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();
            if (change instanceof AddColumnChange) {
                addColumnChanges.add((AddColumnChange)change);
                changeIt.remove();
            }
        }

        for (ListIterator<AddColumnChange> changeIt = addColumnChanges.listIterator(addColumnChanges.size()); changeIt.hasPrevious();) {
            AddColumnChange addColumnChange = (AddColumnChange) changeIt.previous();
            processChange(currentModel, desiredModel, addColumnChange);
            changeIt.remove();
        }

        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();
            if (change instanceof RemoveColumnChange) {
                RemoveColumnChange removeColumnChange = (RemoveColumnChange) change;
                processChange(currentModel, desiredModel, removeColumnChange);
                changeIt.remove();
            }
            else if (change instanceof ColumnAutoIncrementChange) {
                processAlterColumn(currentModel, change);              
                changeIt.remove();
            } else if (change instanceof ColumnChange) {
                boolean needsAlter = true;
                if (change instanceof ColumnDataTypeChange) {
                    ColumnDataTypeChange dataTypeChange = (ColumnDataTypeChange) change;
                    if (dataTypeChange.getChangedColumn().getTypeCode() == Types.DECIMAL
                            && dataTypeChange.getNewTypeCode() == Types.NUMERIC) {
                        needsAlter = false;
                    }
                    if (dataTypeChange.getChangedColumn().getTypeCode() == Types.SMALLINT
                            && dataTypeChange.getNewTypeCode() == Types.TINYINT) {
                        needsAlter = false;
                    }
                    if (dataTypeChange.getChangedColumn().getTypeCode() == Types.VARCHAR
                            && dataTypeChange.getNewTypeCode() == Types.LONGVARCHAR) {
                        needsAlter = false;
                    }
                }
                if (change instanceof ColumnSizeChange) {
                    ColumnSizeChange sizeChange = (ColumnSizeChange) change;
                    if (sizeChange.getNewScale() == 0 && sizeChange.getNewSize() == 0) {
                        needsAlter = false;
                    } else if (sizeChange.getNewSize() == sizeChange.getChangedColumn().getSizeAsInt()
                            && sizeChange.getNewScale() == sizeChange.getChangedColumn().getScale()) {
                        needsAlter = false;
                    }
                }
                if (needsAlter) {
                    processAlterColumn(currentModel, change);
                }
                changeIt.remove();
            }
        }

    }
    
    @Override
    protected void writeColumnAutoIncrementStmt(Table table, Column column) throws IOException {
        print("AUTO_INCREMENT");
    }

    protected void processAlterColumn(Database currentModel, TableChange change) throws IOException {
        Column column = null;
        if (change instanceof ColumnChange) {
            column = ((ColumnChange) change).getChangedColumn();
        } else if (change instanceof ColumnAutoIncrementChange) {
            column = ((ColumnAutoIncrementChange) change).getColumn();
        }

        if (column != null) {
            change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
            print("ALTER TABLE ");
            printlnIdentifier(getTableName(change.getChangedTable()));
            printIndent();
            print("ALTER COLUMN ");
            if (change instanceof ColumnRequiredChange) {
                ColumnRequiredChange columnRequiredChange = (ColumnRequiredChange) change;
                printlnIdentifier(getColumnName(column));
                printIndent();
                if (columnRequiredChange.getChangedColumn().isRequired()) {
                    print(" SET NOT NULL ");
                } else {
                    print(" SET NULL ");
                }
            } else {
                writeColumn(change.getChangedTable(), column);
            }
            printEndOfStatement();
        }
    }

    /**
     * Processes the addition of a column to a table.
     * 
     * @param currentModel
     *            The current database schema
     * @param desiredModel
     *            The desired database schema
     * @param change
     *            The change object
     */
    protected void processChange(Database currentModel, Database desiredModel, AddColumnChange change)
            throws IOException {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("ADD COLUMN ");
        writeColumn(change.getChangedTable(), change.getNewColumn());
        if (change.getNextColumn() != null) {
            print(" BEFORE ");
            printIdentifier(getColumnName(change.getNextColumn()));
        }
        printEndOfStatement();
        change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    }

    /**
     * Processes the removal of a column from a table.
     * 
     * @param currentModel
     *            The current database schema
     * @param desiredModel
     *            The desired database schema
     * @param change
     *            The change object
     */
    protected void processChange(Database currentModel, Database desiredModel, RemoveColumnChange change)
            throws IOException {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("DROP COLUMN ");
        printIdentifier(getColumnName(change.getColumn()));
        printEndOfStatement();
        change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    }

    @Override
    protected void writeColumnDefaultValueStmt(Table table, Column column) throws IOException {
        Object parsedDefault = column.getParsedDefaultValue();

        if (parsedDefault != null) {
            if (!getPlatformInfo().isDefaultValuesForLongTypesSupported()
                    && ((column.getTypeCode() == Types.LONGVARBINARY) || (column.getTypeCode() == Types.LONGVARCHAR))) {
                throw new ModelException(
                        "The platform does not support default values for LONGVARCHAR or LONGVARBINARY columns");
            }
            // we write empty default value strings only if the type is not a
            // numeric or date/time type
            if (isValidDefaultValue(column.getDefaultValue(), column.getTypeCode())) {
                print(" DEFAULT ");
                writeColumnDefaultValue(table, column);
            }
        } else if (getPlatformInfo().isDefaultValueUsedForIdentitySpec() && column.isAutoIncrement()) {
            print(" DEFAULT ");
            writeColumnDefaultValue(table, column);
        } else if (!StringUtils.isBlank(column.getDefaultValue())) {
            print(" DEFAULT ");
            writeColumnDefaultValue(table, column);
        }
    }    

    @Override
    protected void printDefaultValue(Object defaultValue, int typeCode) throws IOException {
        if (defaultValue != null) {
            String defaultValueStr = defaultValue.toString();
            boolean shouldUseQuotes = !TypeMap.isNumericType(typeCode) && !defaultValueStr.startsWith("TO_DATE(")
                    && !defaultValue.equals("CURRENT_TIMESTAMP") && !defaultValue.equals("CURRENT_TIME")
                    && !defaultValue.equals("CURRENT_DATE");
            ;

            if (shouldUseQuotes) {
                // characters are only escaped when within a string literal
                print(getPlatformInfo().getValueQuoteToken());
                print(escapeStringValue(defaultValueStr));
                print(getPlatformInfo().getValueQuoteToken());
            } else {
                print(defaultValueStr);
            }
        }
    }
}