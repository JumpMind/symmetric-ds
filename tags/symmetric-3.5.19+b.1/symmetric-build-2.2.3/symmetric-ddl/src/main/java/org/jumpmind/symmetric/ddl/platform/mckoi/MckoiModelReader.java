package org.jumpmind.symmetric.ddl.platform.mckoi;

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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.map.ListOrderedMap;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.model.TypeMap;
import org.jumpmind.symmetric.ddl.platform.DatabaseMetaDataWrapper;
import org.jumpmind.symmetric.ddl.platform.JdbcModelReader;

/**
 * Reads a database model from a Mckoi database.
 *
 * @version $Revision: $
 */
public class MckoiModelReader extends JdbcModelReader
{
    /**
     * Creates a new model reader for Mckoi databases.
     * 
     * @param platform The platform that this model reader belongs to
     */
    public MckoiModelReader(Platform platform)
    {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
    }

    /**
     * {@inheritDoc}
     */
    protected Table readTable(DatabaseMetaDataWrapper metaData, Map values) throws SQLException
    {
        Table table = super.readTable(metaData, values);

        if (table != null)
        {
            // Mckoi does not currently return unique indices in the metadata so we have to query
            // internal tables to get this info
            StringBuffer query = new StringBuffer();
        
            query.append("SELECT uniqueColumns.column, uniqueColumns.seq_no, uniqueInfo.name");
            query.append(" FROM SYS_INFO.sUSRUniqueColumns uniqueColumns, SYS_INFO.sUSRUniqueInfo uniqueInfo");
            query.append(" WHERE uniqueColumns.un_id = uniqueInfo.id AND uniqueInfo.table = '");
            query.append(table.getName());
            if (table.getSchema() != null)
            {
                query.append("' AND uniqueInfo.schema = '");
                query.append(table.getSchema());
            }
            query.append("'");
        
            Statement stmt        = getConnection().createStatement();
            ResultSet resultSet   = stmt.executeQuery(query.toString());
            Map       indices     = new ListOrderedMap();
            Map       indexValues = new HashMap();
        
            indexValues.put("NON_UNIQUE", Boolean.FALSE);
            while (resultSet.next())
            {
                indexValues.put("COLUMN_NAME",      resultSet.getString(1));
                indexValues.put("ORDINAL_POSITION", new Short(resultSet.getShort(2)));
                indexValues.put("INDEX_NAME",       resultSet.getString(3));
        
                readIndex(metaData, indexValues, indices);
            }
            resultSet.close();
        
            table.addIndices(indices.values());
        }
        
        return table;
    }

    /**
     * {@inheritDoc}
     */
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map values) throws SQLException
    {
        Column column = super.readColumn(metaData, values);

        if (column.getSize() != null)
        {
            if (column.getSizeAsInt() <= 0)
            {
                column.setSize(null);
            }
        }

        String defaultValue = column.getDefaultValue();

        if (defaultValue != null)
        {
            if (defaultValue.toLowerCase().startsWith("nextval('") ||
                defaultValue.toLowerCase().startsWith("uniquekey('"))
            {
                column.setDefaultValue(null);
                column.setAutoIncrement(true);
            }
            else if (TypeMap.isTextType(column.getTypeCode()))
            {
                column.setDefaultValue(unescape(column.getDefaultValue(), "'", "\\'"));
            }
        }
        return column;
    }
}
