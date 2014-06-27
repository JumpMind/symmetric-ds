package org.jumpmind.symmetric.ddlutils.firebird;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.ListOrderedMap;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.platform.DatabaseMetaDataWrapper;

/**
 * The Jdbc Model Reader for Firebird (with bug fixes).
 */
@SuppressWarnings("unchecked")
public class FirebirdModelReader extends org.apache.ddlutils.platform.firebird.FirebirdModelReader
{
    /**
     * Creates a new model reader for Firebird databases.
     * 
     * @param platform The platform that this model reader belongs to
     */
    public FirebirdModelReader(Platform platform)
    {
        super(platform);
    }


    protected Collection readColumns(DatabaseMetaDataWrapper metaData, String tableName) throws SQLException
    {
        ResultSet columnData = null;
        try
        {
            List columns = new ArrayList();

            if (getPlatform().isDelimitedIdentifierModeOn())
            {
                // Jaybird has a problem when delimited identifiers are used as
                // it is not able to find the columns for the table
                // So we have to filter manually below
                columnData = metaData.getColumns(getDefaultTablePattern(), getDefaultColumnPattern());

                while (columnData.next())
                {
                    Map values = readColumns(columnData, getColumnsForColumn());

                    if (tableName.equals(values.get("TABLE_NAME")))
                    {
                        columns.add(readColumn(metaData, values));
                    }
                }
            }
            else
            {
                columnData = metaData.getColumns(tableName, getDefaultColumnPattern());

                while (columnData.next())
                {
                    Map values = readColumns(columnData, getColumnsForColumn());

                    if (tableName.equals(values.get("TABLE_NAME")))
                    {
                        columns.add(readColumn(metaData, values));
                    }
                }
            }

            return columns;
        }
        finally
        {
            if (columnData != null)
            {
                columnData.close();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    protected Collection readPrimaryKeyNames(DatabaseMetaDataWrapper metaData, String tableName) throws SQLException
    {
        List      pks   = new ArrayList();
        ResultSet pkData = null;

        try
        {
            if (getPlatform().isDelimitedIdentifierModeOn())
            {
                // Jaybird has a problem when delimited identifiers are used as
                // it is not able to find the primary key info for the table
                // So we have to filter manually below
                pkData = metaData.getPrimaryKeys(getDefaultTablePattern());
                while (pkData.next())
                {
                    Map values = readColumns(pkData, getColumnsForPK());
    
                    if (tableName.equals(values.get("TABLE_NAME")))
                    {
                        pks.add(readPrimaryKeyName(metaData, values));
                    }
                }
            }
            else
            {
                pkData = metaData.getPrimaryKeys(tableName);
                while (pkData.next())
                {
                    Map values = readColumns(pkData, getColumnsForPK());
    
                    if (tableName.equals(values.get("TABLE_NAME")))
                    {
                        pks.add(readPrimaryKeyName(metaData, values));
                    }
                }
            }
        }
        finally
        {
            if (pkData != null)
            {
                pkData.close();
            }
        }
        return pks;
    }

    /**
     * {@inheritDoc}
     */
    protected Collection readForeignKeys(DatabaseMetaDataWrapper metaData, String tableName) throws SQLException
    {
        Map       fks    = new ListOrderedMap();
        ResultSet fkData = null;

        try
        {
            if (getPlatform().isDelimitedIdentifierModeOn())
            {
                // Jaybird has a problem when delimited identifiers are used as
                // it is not able to find the foreign key info for the table
                // So we have to filter manually below
                fkData = metaData.getForeignKeys(getDefaultTablePattern());
                while (fkData.next())
                {
                    Map values = readColumns(fkData, getColumnsForFK());
    
                    if (tableName.equals(values.get("FKTABLE_NAME")))
                    {
                        readForeignKey(metaData, values, fks);
                    }
                }
            }
            else
            {
                fkData = metaData.getForeignKeys(tableName);
                while (fkData.next())
                {
                    Map values = readColumns(fkData, getColumnsForFK());
    
                    if (tableName.equals(values.get("FKTABLE_NAME")))
                    {
                        readForeignKey(metaData, values, fks);
                    }
                }
            }
        }
        finally
        {
            if (fkData != null)
            {
                fkData.close();
            }
        }
        return fks.values();
    }

}
