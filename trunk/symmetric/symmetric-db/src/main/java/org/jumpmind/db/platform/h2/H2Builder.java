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

import java.io.Writer;
import java.sql.Types;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.IDatabasePlatform;
import org.jumpmind.db.alter.AddColumnChange;
import org.jumpmind.db.alter.RemoveColumnChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Index;
import org.jumpmind.db.model.ModelException;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.SqlBuilder;
import org.jumpmind.util.Log;

/*
 * The SQL Builder for the H2 database. 
 */
public class H2Builder extends SqlBuilder {

    public H2Builder(Log log, IDatabasePlatform platform, Writer writer) {
        super(log, platform, writer);
        addEscapedCharSequence("'", "''");
    }

    protected void processChange(Database currentModel, Database desiredModel,
            AddColumnChange change)  {
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
        } else if (platform.getPlatformInfo().isDefaultValueUsedForIdentitySpec()
                && column.isAutoIncrement()) {
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
            boolean shouldUseQuotes = !TypeMap.isNumericType(typeCode)
                    && !defaultValueStr.startsWith("TO_DATE(")
                    && !defaultValue.equals("CURRENT_TIMESTAMP")
                    && !defaultValue.equals("CURRENT_TIME") && !defaultValue.equals("CURRENT_DATE");
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
    
    @Override
    public void writeExternalIndexDropStmt(Table table, Index index) {
        print("DROP INDEX IF EXISTS ");
        printIdentifier(getIndexName(index));
        printEndOfStatement();
    }
}