package org.jumpmind.symmetric.ddl.platform;

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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Describes a column in a metadata result set.
 * 
 * @version $Revision: $
 */
public class MetaDataColumnDescriptor
{
    /** The name of the column. */
    private String _columnName;
    /** The jdbc type to read from the result set. */
    private int _jdbcType;
    /** The default value if the column is not present in the result set. */
    private Object _defaultValue;

    /**
     * Creates a new descriptor instance.
     * 
     * @param columnName The name of the column
     * @param jdbcType   The jdbc type for reading from the result set, one of
     *                   VARCHAR, INTEGER, TINYINT, BIT
     */
    public MetaDataColumnDescriptor(String columnName, int jdbcType)
    {
        this(columnName, jdbcType, null);
    }

    /**
     * Creates a new descriptor instance.
     * 
     * @param columnName   The name of the column
     * @param jdbcType   The jdbc type for reading from the result set, one of
     *                   VARCHAR, INTEGER, TINYINT, BIT
     * @param defaultValue The default value if the column is not present in the result set
     */
    public MetaDataColumnDescriptor(String columnName, int jdbcType, Object defaultValue)
    {
        _columnName   = columnName.toUpperCase();
        _jdbcType     = jdbcType;
        _defaultValue = defaultValue;
    }

    /**
     * Returns the name.
     *
     * @return The name
     */
    public String getName()
    {
        return _columnName;
    }

    /**
     * Returns the default value.
     *
     * @return The default value
     */
    public Object getDefaultValue()
    {
        return _defaultValue;
    }

    /**
     * Returns the jdbc type to read from the result set.
     *
     * @return The jdbc type
     */
    public int getJdbcType()
    {
        return _jdbcType;
    }

    /**
     * Reads the column from the result set.
     * 
     * @param resultSet The result set
     * @return The column value or the default value if the column is not present in the result set
     */
    public Object readColumn(ResultSet resultSet) throws SQLException
    {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int               foundIdx = -1;

        for (int idx = 1; (foundIdx < 0) && (idx <= metaData.getColumnCount()); idx++)
        {
            if (_columnName.equals(metaData.getColumnName(idx).toUpperCase()))
            {
                foundIdx = idx;
            }
        }
        if (foundIdx > 0)
        {
            switch (_jdbcType)
            {
                case Types.BIT:
                    return new Boolean(resultSet.getBoolean(foundIdx));
                case Types.INTEGER:
                    return new Integer(resultSet.getInt(foundIdx));
                case Types.TINYINT:
                    return new Short(resultSet.getShort(foundIdx));
                default:
                    return resultSet.getString(foundIdx);
            }
        }
        else
        {
            return _defaultValue;
        }
    }
}
