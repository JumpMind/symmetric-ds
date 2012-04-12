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

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.alter.AddColumnChange;
import org.jumpmind.db.alter.RemoveColumnChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.ModelException;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.AbstractDdlBuilder;
import org.jumpmind.db.platform.IDatabasePlatform;

/*
 * The SQL Builder for the H2 database. 
 */
public class H2DdlBuilder extends AbstractDdlBuilder {

    public H2DdlBuilder(IDatabasePlatform platform) {
        super(platform);
        addEscapedCharSequence("'", "''");
    }

    protected void processChange(Database currentModel, Database desiredModel,
            AddColumnChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
        printIndent(ddl);
        ddl.append("ADD COLUMN ");
        writeColumn(change.getChangedTable(), change.getNewColumn(), ddl);
        if (change.getNextColumn() != null) {
            ddl.append(" BEFORE ");
            printIdentifier(getColumnName(change.getNextColumn()), ddl);
        }
        printEndOfStatement(ddl);
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
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
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }

    @Override
    protected void writeColumnDefaultValueStmt(Table table, Column column, StringBuilder ddl) {
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
                ddl.append(" DEFAULT ");
                writeColumnDefaultValue(table, column, ddl);
            }
        } else if (platform.getPlatformInfo().isDefaultValueUsedForIdentitySpec()
                && column.isAutoIncrement()) {
            ddl.append(" DEFAULT ");
            writeColumnDefaultValue(table, column, ddl);
        } else if (!StringUtils.isBlank(column.getDefaultValue())) {
            ddl.append(" DEFAULT ");
            writeColumnDefaultValue(table, column, ddl);
        }
    }

    @Override
    protected void printDefaultValue(Object defaultValue, int typeCode, StringBuilder ddl) {
        if (defaultValue != null) {
            String defaultValueStr = defaultValue.toString();
            boolean shouldUseQuotes = !TypeMap.isNumericType(typeCode)
                    && !defaultValueStr.startsWith("TO_DATE(")
                    && !defaultValue.equals("CURRENT_TIMESTAMP")
                    && !defaultValue.equals("CURRENT_TIME") && !defaultValue.equals("CURRENT_DATE");
            ;

            if (shouldUseQuotes) {
                // characters are only escaped when within a string literal
                ddl.append(platform.getPlatformInfo().getValueQuoteToken());
                ddl.append(escapeStringValue(defaultValueStr));
                ddl.append(platform.getPlatformInfo().getValueQuoteToken());
            } else {
                ddl.append(defaultValueStr);
            }
        }
    }

    @Override
    public void writeExternalIndexDropStmt(Table table, IIndex index, StringBuilder ddl) {
        ddl.append("DROP INDEX IF EXISTS ");
        printIdentifier(getIndexName(index), ddl);
        printEndOfStatement(ddl);
    }
}