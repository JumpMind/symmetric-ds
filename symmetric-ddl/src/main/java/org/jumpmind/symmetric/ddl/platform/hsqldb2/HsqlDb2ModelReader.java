package org.jumpmind.symmetric.ddl.platform.hsqldb2;

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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.ForeignKey;
import org.jumpmind.symmetric.ddl.model.Index;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.model.TypeMap;
import org.jumpmind.symmetric.ddl.platform.DatabaseMetaDataWrapper;
import org.jumpmind.symmetric.ddl.platform.JdbcModelReader;

/*
 * Reads a database model from a HsqlDb database.
 */
public class HsqlDb2ModelReader extends JdbcModelReader
{
    /*
     * Creates a new model reader for HsqlDb databases.
     * 
     * @param platform The platform that this model reader belongs to
     */
    public HsqlDb2ModelReader(Platform platform)
    {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
    }

    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String,Object> values) throws SQLException
    {
        Column column = super.readColumn(metaData, values);

        if (TypeMap.isTextType(column.getTypeCode()) &&
            (column.getDefaultValue() != null))
        {
            column.setDefaultValue(unescape(column.getDefaultValue(), "'", "''"));
        }
        
        String autoIncrement = (String)values.get("IS_AUTOINCREMENT");
        if (autoIncrement != null) {
            column.setAutoIncrement("YES".equalsIgnoreCase(autoIncrement.trim()));
        }
        return column;
    }

    @Override
    protected boolean isInternalForeignKeyIndex(Connection connection, DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk, Index index)
    {
        String name = index.getName();

        return (name != null) && name.startsWith("SYS_IDX_");
    }

    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection,DatabaseMetaDataWrapper metaData, Table table, Index index)
    {
        String name = index.getName();

        return (name != null) && (name.startsWith("SYS_PK_") || name.startsWith("SYS_IDX_"));
    }
    
}
