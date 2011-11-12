package org.jumpmind.symmetric.db.ddl.platform.mysql;

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
import java.sql.Types;
import java.util.Map;

import org.jumpmind.symmetric.db.ddl.Platform;
import org.jumpmind.symmetric.db.ddl.model.Column;
import org.jumpmind.symmetric.db.ddl.model.ForeignKey;
import org.jumpmind.symmetric.db.ddl.model.Index;
import org.jumpmind.symmetric.db.ddl.model.Table;
import org.jumpmind.symmetric.db.ddl.platform.DatabaseMetaDataWrapper;
import org.jumpmind.symmetric.db.ddl.platform.JdbcModelReader;

/*
 * Reads a database model from a MySql database.
 */
public class MySqlModelReader extends JdbcModelReader
{
    /*
     * Creates a new model reader for MySql databases.
     * 
     * @param platform The platform that this model reader belongs to
     */
    public MySqlModelReader(Platform platform)
    {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
        setDefaultTablePattern(null);
    }

    @Override
    protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData, Map values) throws SQLException
    {
        // TODO This needs some more work, since table names can be case sensitive or lowercase
        //      depending on the platform (really cute).
        //      See http://dev.mysql.com/doc/refman/4.1/en/name-case-sensitivity.html for more info.

        Table table = super.readTable(connection, metaData, values);

        if (table != null)
        {
            determineAutoIncrementFromResultSetMetaData(connection, table, table.getPrimaryKeyColumns());
        }
        return table;
    }
    

    @Override
    protected Integer overrideJdbcTypeForColumn(Map<String,Object> values) {
        String typeName = (String)values.get("TYPE_NAME");
        if("YEAR".equals(typeName)) {
            // it is safe to map a YEAR to INTEGER
            return Types.INTEGER;
        } else {
            return super.overrideJdbcTypeForColumn(values);
        }
    }

    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map values) throws SQLException
    {
        Column column = super.readColumn(metaData, values);

        // MySQL converts illegal date/time/timestamp values to "0000-00-00 00:00:00", but this
        // is an illegal ISO value, so we replace it with NULL
        if ((column.getTypeCode() == Types.TIMESTAMP) && 
            "0000-00-00 00:00:00".equals(column.getDefaultValue()))
        {
            column.setDefaultValue(null);
        }
        return column;
    }

    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection, DatabaseMetaDataWrapper metaData, Table table, Index index)
    {
        // MySql defines a unique index "PRIMARY" for primary keys
        return "PRIMARY".equals(index.getName());
    }

    @Override
    protected boolean isInternalForeignKeyIndex(Connection connection, DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk, Index index)
    {
        // MySql defines a non-unique index of the same name as the fk
        return getPlatform().getSqlBuilder().getForeignKeyName(table, fk).equals(index.getName());
    }
}
