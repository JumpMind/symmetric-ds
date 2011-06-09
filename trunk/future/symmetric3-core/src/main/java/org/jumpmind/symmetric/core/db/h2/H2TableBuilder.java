/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.jumpmind.symmetric.core.db.h2;

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
import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;

import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.db.AbstractTableBuilder;
import org.jumpmind.symmetric.core.db.SqlException;
import org.jumpmind.symmetric.core.db.alter.AddColumnChange;
import org.jumpmind.symmetric.core.db.alter.ColumnAutoIncrementChange;
import org.jumpmind.symmetric.core.db.alter.ColumnChange;
import org.jumpmind.symmetric.core.db.alter.ColumnDataTypeChange;
import org.jumpmind.symmetric.core.db.alter.ColumnRequiredChange;
import org.jumpmind.symmetric.core.db.alter.ColumnSizeChange;
import org.jumpmind.symmetric.core.db.alter.RemoveColumnChange;
import org.jumpmind.symmetric.core.db.alter.TableChange;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Database;
import org.jumpmind.symmetric.core.model.Index;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.TypeMap;

/**
 * The SQL Builder for the H2 database. From patch <a
 * href="https://issues.apache.org/jira/browse/DDLUTILS-185"
 * >https://issues.apache.org/jira/browse/DDLUTILS-185</a>
 */
public class H2TableBuilder extends AbstractTableBuilder {

    public H2TableBuilder(IDbDialect platform) {
        super(platform);
        addEscapedCharSequence("'", "''");
    }

    @Override
    public void dropTable(Table table) {
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
    public void writeExternalIndexDropStmt(Table table, Index index) {
        print("DROP INDEX IF EXISTS ");
        printIdentifier(getIndexName(index));
        printEndOfStatement();
    }

    @Override
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            Collection<TableChange> changes) {

        // Only drop columns that are not part of a primary key
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();
            if ((change instanceof RemoveColumnChange)
                    && ((RemoveColumnChange) change).getColumn().isPrimaryKey()) {
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
                addColumnChanges.add((AddColumnChange) change);
                changeIt.remove();
            }
        }

        for (ListIterator<AddColumnChange> changeIt = addColumnChanges
                .listIterator(addColumnChanges.size()); changeIt.hasPrevious();) {
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
            } else if (change instanceof ColumnAutoIncrementChange) {
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
                    } else if (sizeChange.getNewSize() == sizeChange.getChangedColumn()
                            .getSizeAsInt()
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
    protected void writeColumnAutoIncrementStmt(Table table, Column column) {
        print("AUTO_INCREMENT");
    }

    protected void processAlterColumn(Database currentModel, TableChange change) {
        Column column = null;
        if (change instanceof ColumnChange) {
            column = ((ColumnChange) change).getChangedColumn();
        } else if (change instanceof ColumnAutoIncrementChange) {
            column = ((ColumnAutoIncrementChange) change).getColumn();
        }

        if (column != null) {
            change.apply(currentModel, getDbDialectInfo().isDelimitedIdentifierModeOn());
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
    protected void processChange(Database currentModel, Database desiredModel,
            AddColumnChange change) {
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
        change.apply(currentModel, getDbDialectInfo().isDelimitedIdentifierModeOn());
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
    protected void processChange(Database currentModel, Database desiredModel,
            RemoveColumnChange change) {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("DROP COLUMN ");
        printIdentifier(getColumnName(change.getColumn()));
        printEndOfStatement();
        change.apply(currentModel, getDbDialectInfo().isDelimitedIdentifierModeOn());
    }

    @Override
    protected void writeColumnDefaultValueStmt(Table table, Column column) {
        Object parsedDefault = column.getParsedDefaultValue();

        if (parsedDefault != null) {
            if (!getDbDialectInfo().isDefaultValuesForLongTypesSupported()
                    && ((column.getTypeCode() == Types.LONGVARBINARY) || (column.getTypeCode() == Types.LONGVARCHAR))) {
                throw new SqlException(
                        "The platform does not support default values for LONGVARCHAR or LONGVARBINARY columns");
            }
            // we write empty default value strings only if the type is not a
            // numeric or date/time type
            if (isValidDefaultValue(column.getDefaultValue(), column.getTypeCode())) {
                print(" DEFAULT ");
                writeColumnDefaultValue(table, column);
            }
        } else if (getDbDialectInfo().isDefaultValueUsedForIdentitySpec()
                && column.isAutoIncrement()) {
            print(" DEFAULT ");
            writeColumnDefaultValue(table, column);
        } else if (!StringUtils.isBlank(column.getDefaultValue())) {
            print(" DEFAULT ");
            writeColumnDefaultValue(table, column);
        }
    }

    @Override
    protected void printDefaultValue(Object defaultValue, int typeCode) {
        if (defaultValue != null) {
            String defaultValueStr = defaultValue.toString();
            boolean shouldUseQuotes = !TypeMap.isNumericType(typeCode)
                    && !defaultValueStr.startsWith("TO_DATE(")
                    && !defaultValue.equals("CURRENT_TIMESTAMP")
                    && !defaultValue.equals("CURRENT_TIME") && !defaultValue.equals("CURRENT_DATE");
            ;

            if (shouldUseQuotes) {
                // characters are only escaped when within a string literal
                print(getDbDialectInfo().getValueQuoteToken());
                print(escapeStringValue(defaultValueStr));
                print(getDbDialectInfo().getValueQuoteToken());
            } else {
                print(defaultValueStr);
            }
        }
    }
}