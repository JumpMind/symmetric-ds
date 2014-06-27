/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.jumpmind.db.platform.h2;

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
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.alter.AddColumnChange;
import org.jumpmind.db.alter.ColumnAutoIncrementChange;
import org.jumpmind.db.alter.ColumnDataTypeChange;
import org.jumpmind.db.alter.ColumnDefaultValueChange;
import org.jumpmind.db.alter.ColumnRequiredChange;
import org.jumpmind.db.alter.ColumnSizeChange;
import org.jumpmind.db.alter.CopyColumnValueChange;
import org.jumpmind.db.alter.RemoveColumnChange;
import org.jumpmind.db.alter.TableChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.ModelException;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractDdlBuilder;

/*
 * The SQL Builder for the H2 database. 
 */
public class H2DdlBuilder extends AbstractDdlBuilder {

    public H2DdlBuilder() {
        databaseInfo.setNonPKIdentityColumnsSupported(false);
        databaseInfo.setIdentityOverrideAllowed(false);
        databaseInfo.setSystemForeignKeyIndicesAlwaysNonUnique(true);
        databaseInfo.setNullAsDefaultValueRequired(false);
        databaseInfo.addNativeTypeMapping(Types.ARRAY, "BINARY", Types.BINARY);
        databaseInfo.addNativeTypeMapping(Types.DISTINCT, "BINARY", Types.BINARY);
        databaseInfo.addNativeTypeMapping(Types.NULL, "BINARY", Types.BINARY);
        databaseInfo.addNativeTypeMapping(Types.REF, "BINARY", Types.BINARY);
        databaseInfo.addNativeTypeMapping(Types.STRUCT, "BINARY", Types.BINARY);
        databaseInfo.addNativeTypeMapping(Types.DATALINK, "BINARY", Types.BINARY);
        databaseInfo.addNativeTypeMapping(Types.BIT, "BOOLEAN", Types.BIT);
        databaseInfo.addNativeTypeMapping(Types.NUMERIC, "DECIMAL", Types.DECIMAL);
        databaseInfo.addNativeTypeMapping(Types.BINARY, "BINARY", Types.BINARY);
        databaseInfo.addNativeTypeMapping(Types.BLOB, "BLOB", Types.BLOB);
        databaseInfo.addNativeTypeMapping(Types.CLOB, "CLOB", Types.CLOB);
        databaseInfo.addNativeTypeMapping(Types.LONGVARCHAR, "VARCHAR("+Integer.MAX_VALUE+")", Types.VARCHAR);
        databaseInfo.addNativeTypeMapping(Types.FLOAT, "DOUBLE", Types.DOUBLE);
        databaseInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "OTHER");

        databaseInfo.setDefaultSize(Types.CHAR, Integer.MAX_VALUE);
        databaseInfo.setDefaultSize(Types.VARCHAR, Integer.MAX_VALUE);
        databaseInfo.setDefaultSize(Types.BINARY, Integer.MAX_VALUE);
        databaseInfo.setDefaultSize(Types.VARBINARY, Integer.MAX_VALUE);
        
        databaseInfo.setNonBlankCharColumnSpacePadded(false);
        databaseInfo.setBlankCharColumnSpacePadded(false);
        databaseInfo.setCharColumnSpaceTrimmed(true);
        databaseInfo.setEmptyStringNulled(false);                

        addEscapedCharSequence("'", "''");
    }
    
    @Override
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            Table sourceTable, Table targetTable, List<TableChange> changes, StringBuilder ddl) {
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();

            if (change instanceof AddColumnChange) {
                AddColumnChange addColumnChange = (AddColumnChange) change;
                processChange(currentModel, desiredModel, addColumnChange, ddl);
                changeIt.remove();
            } else if (change instanceof CopyColumnValueChange) {
                CopyColumnValueChange copyColumnChange = (CopyColumnValueChange)change;
                processChange(currentModel, desiredModel, copyColumnChange, ddl);
                changeIt.remove();
            } else if (change instanceof RemoveColumnChange) {
                processChange(currentModel, desiredModel, (RemoveColumnChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof ColumnDefaultValueChange) {
                ColumnDefaultValueChange defaultChange = (ColumnDefaultValueChange)change;
                defaultChange.getChangedColumn().setDefaultValue(defaultChange.getNewDefaultValue());
                writeAlterColumn(change.getChangedTable(), defaultChange.getChangedColumn(), ddl);
                changeIt.remove();
            } else if (change instanceof ColumnRequiredChange) {
                ColumnRequiredChange defaultChange = (ColumnRequiredChange)change;
                defaultChange.getChangedColumn().setRequired(!defaultChange.getChangedColumn().isRequired());
                writeAlterColumn(change.getChangedTable(), defaultChange.getChangedColumn(), ddl);
                changeIt.remove();
            } else if (change instanceof ColumnSizeChange) {
                ColumnSizeChange sizeChange = (ColumnSizeChange)change;
                sizeChange.getChangedColumn().setSizeAndScale(sizeChange.getNewSize(), sizeChange.getNewScale());                
                writeAlterColumn(change.getChangedTable(), sizeChange.getChangedColumn(), ddl);
                changeIt.remove();
            } else if (change instanceof ColumnAutoIncrementChange) {
                ColumnAutoIncrementChange defaultChange = (ColumnAutoIncrementChange)change;
                defaultChange.getColumn().setAutoIncrement(!defaultChange.getColumn().isAutoIncrement());
                writeAlterColumn(change.getChangedTable(), defaultChange.getColumn(), ddl);
                changeIt.remove();
            }
        }
        super.processTableStructureChanges(currentModel, desiredModel, sourceTable, targetTable,
                changes, ddl);
    }    

    protected void processChange(Database currentModel, Database desiredModel,
            AddColumnChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
        printIndent(ddl);
        ddl.append("ADD COLUMN ");
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
        printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
        printIndent(ddl);
        ddl.append("DROP COLUMN ");
        printIdentifier(getColumnName(change.getColumn()), ddl);
        printEndOfStatement(ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    @Override
    protected void writeColumnDefaultValueStmt(Table table, Column column, StringBuilder ddl) {
        Object parsedDefault = column.getParsedDefaultValue();

        if (parsedDefault != null) {
            if (!databaseInfo.isDefaultValuesForLongTypesSupported()
                    && ((column.getMappedTypeCode() == Types.LONGVARBINARY) || (column.getMappedTypeCode() == Types.LONGVARCHAR))) {
                throw new ModelException(
                        "The platform does not support default values for LONGVARCHAR or LONGVARBINARY columns");
            }
            // we write empty default value strings only if the type is not a
            // numeric or date/time type
            if (isValidDefaultValue(column.getDefaultValue(), column.getMappedTypeCode())) {
                ddl.append(" DEFAULT ");
                writeColumnDefaultValue(table, column, ddl);
            }
        } else if (databaseInfo.isDefaultValueUsedForIdentitySpec()
                && column.isAutoIncrement()) {
            ddl.append(" DEFAULT ");
            writeColumnDefaultValue(table, column, ddl);
        } else if (!StringUtils.isBlank(column.getDefaultValue())) {
            ddl.append(" DEFAULT ");
            writeColumnDefaultValue(table, column, ddl);
        }
    }

    @Override
    public void writeExternalIndexDropStmt(Table table, IIndex index, StringBuilder ddl) {
        ddl.append("DROP INDEX IF EXISTS ");
        printIdentifier(getIndexName(index), ddl);
        printEndOfStatement(ddl);
    }
    
    @Override
    protected void writeColumnAutoIncrementStmt(Table table, Column column, StringBuilder ddl) {
        ddl.append("AUTO_INCREMENT");
    }
    
    @Override
    protected boolean writeAlterColumnDataType(ColumnDataTypeChange change, StringBuilder ddl) {
        change.getChangedColumn().setTypeCode(change.getNewTypeCode());
        writeAlterColumn(change.getChangedTable(), change.getChangedColumn(), ddl);
        return true;
    }   
    
    protected void writeAlterColumn(Table table, Column column, StringBuilder ddl) {
        writeTableAlterStmt(table, ddl);
        ddl.append("ALTER COLUMN ");  
        writeColumn(table, column, ddl);
        printEndOfStatement(ddl);
    }
}