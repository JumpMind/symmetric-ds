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

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class Db2As400DdlBuilder extends Db2DdlBuilder {

    public Db2As400DdlBuilder() {
        this.databaseName = DatabaseNamesConstants.DB2AS400;
        databaseInfo.addNativeTypeMapping(Types.LONGVARCHAR, "CLOB", Types.CLOB);
        databaseInfo.setRequiresAutoCommitForDdl(true);
    }
    
    @Override
    protected void writeCastExpression(Column sourceColumn, Column targetColumn, StringBuilder ddl) {
        String sourceNativeType = getBareNativeType(sourceColumn);
        String targetNativeType = getBareNativeType(targetColumn);

        if (sourceNativeType.equals(targetNativeType)) {
            printIdentifier(getColumnName(sourceColumn), ddl);
        } else {
            String type = getSqlType(targetColumn);

            /**
             * iSeries needs size in cast
             */
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

    protected void writeReorgStmt(Table table, StringBuilder ddl) {
    }

}
