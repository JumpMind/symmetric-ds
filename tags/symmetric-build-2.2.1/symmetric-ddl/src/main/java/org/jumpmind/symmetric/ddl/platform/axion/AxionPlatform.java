package org.jumpmind.symmetric.ddl.platform.axion;

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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import org.jumpmind.symmetric.ddl.DatabaseOperationException;
import org.jumpmind.symmetric.ddl.PlatformInfo;
import org.jumpmind.symmetric.ddl.platform.PlatformImplBase;

/**
 * The platform for the Axion database.
 * 
 * @version $Revision: 231306 $
 */
public class AxionPlatform extends PlatformImplBase
{
    /** Database name of this platform. */
    public static final String DATABASENAME     = "Axion";
    /** The axion jdbc driver. */
    public static final String JDBC_DRIVER      = "org.axiondb.jdbc.AxionDriver";
    /** The subprotocol used by the axion driver. */
    public static final String JDBC_SUBPROTOCOL = "axiondb";

    /**
     * Creates a new axion platform instance.
     */
    public AxionPlatform()
    {
        PlatformInfo info = getPlatformInfo();

        info.setDelimitedIdentifiersSupported(false);
        info.setSqlCommentsSupported(false);
        info.setLastIdentityValueReadable(false);
        info.addNativeTypeMapping(Types.ARRAY,         "BLOB",      Types.BLOB);
        info.addNativeTypeMapping(Types.BIT,           "BOOLEAN");
        info.addNativeTypeMapping(Types.DISTINCT,      "VARBINARY", Types.VARBINARY);
        info.addNativeTypeMapping(Types.NULL,          "VARBINARY", Types.VARBINARY);
        info.addNativeTypeMapping(Types.OTHER,         "BLOB",      Types.BLOB);
        info.addNativeTypeMapping(Types.REAL,          "REAL",      Types.FLOAT);
        info.addNativeTypeMapping(Types.REF,           "VARBINARY", Types.VARBINARY);
        info.addNativeTypeMapping(Types.STRUCT,        "VARBINARY", Types.VARBINARY);
        info.addNativeTypeMapping(Types.TINYINT,       "SMALLINT",  Types.SMALLINT);
        info.addNativeTypeMapping("DATALINK", "VARBINARY", "VARBINARY");

        setSqlBuilder(new AxionBuilder(this));
        setModelReader(new AxionModelReader(this));
    }

    /**
     * {@inheritDoc}
     */
    public String getName()
    {
        return DATABASENAME;
    }

    /**
     * {@inheritDoc}
     */
    public void createDatabase(String jdbcDriverClassName, String connectionUrl, String username, String password, Map parameters) throws DatabaseOperationException, UnsupportedOperationException
    {
        // Axion will create the database automatically when connecting for the first time
        if (JDBC_DRIVER.equals(jdbcDriverClassName))
        {
            Connection connection = null;

            try
            {
                Class.forName(jdbcDriverClassName);

                connection = DriverManager.getConnection(connectionUrl, username, password);
                logWarnings(connection);
            }
            catch (Exception ex)
            {
                throw new DatabaseOperationException("Error while trying to create a database", ex);
            }
            finally
            {
                if (connection != null)
                {
                    try
                    {
                        connection.close();
                    }
                    catch (SQLException ex)
                    {}
                }
            }
        }
        else
        {
            throw new UnsupportedOperationException("Unable to create a Axion database via the driver "+jdbcDriverClassName);
        }
    }

    /**
     * {@inheritDoc}
     */
    protected Object extractColumnValue(ResultSet resultSet, String columnName, int columnIdx, int jdbcType) throws SQLException
    {
        boolean useIdx = (columnName == null);
        Object  value  = null;

        switch (jdbcType)
        {
            case Types.BIGINT:
                // The Axion JDBC driver does not support reading BIGINT values directly
                String strValue = useIdx ? resultSet.getString(columnIdx) : resultSet.getString(columnName);

                value = resultSet.wasNull() ? null : new Long(strValue);
                break;
            default:
                value = super.extractColumnValue(resultSet, columnName, columnIdx, jdbcType);
                break;
        }
        return value;
    }
}
