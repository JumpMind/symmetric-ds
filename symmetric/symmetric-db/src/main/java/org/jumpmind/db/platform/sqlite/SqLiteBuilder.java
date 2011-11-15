/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.jumpmind.db.platform.sqlite;

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
import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.IDatabasePlatform;
import org.jumpmind.db.alter.AddColumnChange;
import org.jumpmind.db.alter.ColumnChange;
import org.jumpmind.db.alter.ColumnDataTypeChange;
import org.jumpmind.db.alter.ColumnRequiredChange;
import org.jumpmind.db.alter.ColumnSizeChange;
import org.jumpmind.db.alter.RemoveColumnChange;
import org.jumpmind.db.alter.TableChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Index;
import org.jumpmind.db.model.ModelException;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.SqlBuilder;
import org.jumpmind.util.Log;

/*
 * The SQL Builder for the SqlLite database. 
 */
public class SqLiteBuilder extends SqlBuilder {
        
    public SqLiteBuilder(Log log, IDatabasePlatform platform, Writer writer) {
        super(log, platform, writer);
        
        addEscapedCharSequence("'", "''");
    }
    
    @Override
    public void dropTable(Table table)  {
        print("DROP TABLE IF EXISTS ");
        printIdentifier(getTableName(table));
        printEndOfStatement();
    }

    protected void writeColumnAutoIncrementStmt(Table table, Column column) 
    {
        if (!column.isPrimaryKey()){
            print("PRIMARY KEY AUTOINCREMENT");
        }
    }

    @Override
    public String getSelectLastIdentityValues(Table table) {
        return "CALL IDENTITY()";
    }

    @Override
    public void writeExternalIndexDropStmt(Table table, Index index)  {
        print("DROP INDEX IF EXISTS ");
        printIdentifier(getIndexName(index));
        printEndOfStatement();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            Collection<TableChange> changes)  {
        // Only drop columns that are not part of a primary key
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = (TableChange) changeIt.next();

            if ((change instanceof RemoveColumnChange) && ((RemoveColumnChange) change).getColumn().isPrimaryKey()) {
                return;
            }
        }

        // in order to utilize the ALTER TABLE ADD COLUMN BEFORE statement
        // we have to apply the add column changes in the correct order
        // thus we first gather all add column changes and then execute them
        // Since we get them in target table column order, we can simply
        // iterate backwards
        ArrayList addColumnChanges = new ArrayList();

        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = (TableChange) changeIt.next();
            if (change instanceof AddColumnChange) {
                addColumnChanges.add(change);
                changeIt.remove();
            }
        }

        for (ListIterator changeIt = addColumnChanges.listIterator(addColumnChanges.size()); changeIt.hasPrevious();) {
            AddColumnChange addColumnChange = (AddColumnChange) changeIt.previous();
            processChange(currentModel, desiredModel, addColumnChange);
            changeIt.remove();
        }

        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = (TableChange) changeIt.next();
            if (change instanceof RemoveColumnChange) {
                RemoveColumnChange removeColumnChange = (RemoveColumnChange) change;
                processChange(currentModel, desiredModel, removeColumnChange);
                changeIt.remove();
            }
        }

        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = (TableChange) changeIt.next();
            if (change instanceof ColumnChange) {
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
                    processAlterColumn(currentModel, (ColumnChange) change);
                }
                changeIt.remove();
            }
        }

    }

    protected void processAlterColumn(Database currentModel, ColumnChange columnChange)  {
        columnChange.apply(currentModel, platform.isDelimitedIdentifierModeOn());
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(columnChange.getChangedTable()));
        printIndent();
        print("ALTER COLUMN ");
        if (columnChange instanceof ColumnRequiredChange) {
            ColumnRequiredChange columnRequiredChange = (ColumnRequiredChange)columnChange;
            printlnIdentifier(getColumnName(columnChange.getChangedColumn())); 
            printIndent();
            if (columnRequiredChange.getChangedColumn().isRequired()) {
                print(" SET NOT NULL ");
            } else {
                print(" SET NULL ");
            }
        } else {
          writeColumn(columnChange.getChangedTable(), columnChange.getChangedColumn());
        }
        printEndOfStatement();
    }

    /*
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
             {
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
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }

    /*
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
             {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("DROP COLUMN ");
        printIdentifier(getColumnName(change.getColumn()));
        printEndOfStatement();
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }

    @Override
    protected void writeColumnDefaultValueStmt(Table table, Column column)  {
        Object parsedDefault = column.getParsedDefaultValue();

        if (parsedDefault != null) {
            if (!platform.getPlatformInfo().isDefaultValuesForLongTypesSupported()
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
        } else if (platform.getPlatformInfo().isDefaultValueUsedForIdentitySpec() && column.isAutoIncrement()) {
            print(" DEFAULT ");
            writeColumnDefaultValue(table, column);
        } else if (!StringUtils.isBlank(column.getDefaultValue())) {
            print(" DEFAULT ");
            writeColumnDefaultValue(table, column);
        }
    }

    @Override
    protected void printDefaultValue(Object defaultValue, int typeCode)  {
        if (defaultValue != null) {
            String defaultValueStr = defaultValue.toString();
            boolean shouldUseQuotes = !TypeMap.isNumericType(typeCode) && !defaultValueStr.startsWith("TO_DATE(")
                    && !defaultValue.equals("CURRENT_TIMESTAMP") && !defaultValue.equals("CURRENT_TIME")
                    && !defaultValue.equals("CURRENT_DATE");
            ;

            if (shouldUseQuotes) {
                // characters are only escaped when within a string literal
                print(platform.getPlatformInfo().getValueQuoteToken());
                print(escapeStringValue(defaultValueStr));
                print(platform.getPlatformInfo().getValueQuoteToken());
            } else {
                print(defaultValueStr);
            }
        }
    }
}
