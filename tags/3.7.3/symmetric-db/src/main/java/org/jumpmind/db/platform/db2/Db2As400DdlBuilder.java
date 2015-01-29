package org.jumpmind.db.platform.db2;

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
import java.util.Collection;
import java.util.Iterator;

import org.jumpmind.db.alter.ColumnDataTypeChange;
import org.jumpmind.db.alter.TableChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class Db2As400DdlBuilder extends Db2DdlBuilder {

    public Db2As400DdlBuilder() {
        this.databaseName = DatabaseNamesConstants.DB2AS400;
        databaseInfo.addNativeTypeMapping(Types.LONGVARCHAR, "LONG VARCHAR", Types.VARCHAR);
        databaseInfo.setRequiresAutoCommitForDdl(true);
    }
    
    @Override
    protected void filterChanges(Collection<TableChange> changes) {
        super.filterChanges(changes);
        Iterator<TableChange> i = changes.iterator();
        while (i.hasNext()) {
            TableChange tableChange = i.next();
            if (tableChange instanceof ColumnDataTypeChange) {
                ColumnDataTypeChange change = (ColumnDataTypeChange)tableChange;
                if (change.getNewTypeCode() == Types.LONGVARCHAR && 
                        change.getChangedColumn().getJdbcTypeCode() == Types.VARCHAR) {
                    log.debug("Not processing the detect type change to LONGVARCHAR because "
                            + "a create of a long varchar results in a variable length VARCHAR field");
                    i.remove();
                }
            }
        }
    }

    @Override
    protected void writeCastExpression(Column sourceColumn, Column targetColumn, StringBuilder ddl) {
        String sourceNativeType = getBareNativeType(sourceColumn);
        String targetNativeType = getBareNativeType(targetColumn);

        if (sourceNativeType.equals(targetNativeType)) {
            printIdentifier(getColumnName(sourceColumn), ddl);
        } else {
            String type = getSqlType(targetColumn);
            if ("LONG VARCHAR".equals(type)) {
                type = "VARCHAR";
            }

            if ("VARCHAR".equals(type)) {
                type = type + "(" + sourceColumn.getSizeAsInt() + ")";
            }

            /*
             * DB2 has the limitation that it cannot convert numeric values to
             * VARCHAR, though it can convert them to CHAR
             */
            if (TypeMap.isNumericType(sourceColumn.getMappedTypeCode())
                    && "VARCHAR".equalsIgnoreCase(targetNativeType)) {
                Object sizeSpec = targetColumn.getSize();

                if (sizeSpec == null) {
                    sizeSpec = databaseInfo.getDefaultSize(targetColumn.getMappedTypeCode());
                }
                type = "CHAR(" + sizeSpec.toString() + ")";

            }

            ddl.append("CAST(");
            printIdentifier(getColumnName(sourceColumn), ddl);
            ddl.append(" AS  ");
            ddl.append(type);
            ddl.append(")");

        }
    }

}
