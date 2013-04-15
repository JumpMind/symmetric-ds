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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.Map;

import org.jumpmind.symmetric.ddl.DatabaseOperationException;
import org.jumpmind.symmetric.ddl.PlatformInfo;
import org.jumpmind.symmetric.ddl.platform.PlatformImplBase;

/**
 * The Mckoi database platform implementation.
 * 
 * @version $Revision: 231306 $
 */
public class MckoiPlatform extends PlatformImplBase
{
    /** Database name of this platform. */
    public static final String DATABASENAME     = "McKoi";
    /** The standard McKoi jdbc driver. */
    public static final String JDBC_DRIVER      = "com.mckoi.JDBCDriver";
    /** The subprotocol used by the standard McKoi driver. */
    public static final String JDBC_SUBPROTOCOL = "mckoi";

    /**
     * Creates a new platform instance.
     */
    public MckoiPlatform()
    {
        PlatformInfo info = getPlatformInfo();

        info.setIndicesSupported(false);
        info.setIndicesEmbedded(true);
        info.setDefaultValueUsedForIdentitySpec(true);
        info.setAutoCommitModeForLastIdentityValueReading(false);

        info.addNativeTypeMapping(Types.ARRAY,    "BLOB",   Types.BLOB);
        info.addNativeTypeMapping(Types.DISTINCT, "BLOB",   Types.BLOB);
        info.addNativeTypeMapping(Types.FLOAT,    "DOUBLE", Types.DOUBLE);
        info.addNativeTypeMapping(Types.NULL,     "BLOB",   Types.BLOB);
        info.addNativeTypeMapping(Types.OTHER,    "BLOB",   Types.BLOB);
        info.addNativeTypeMapping(Types.REF,      "BLOB",   Types.BLOB);
        info.addNativeTypeMapping(Types.STRUCT,   "BLOB",   Types.BLOB);
        info.addNativeTypeMapping("BIT",      "BOOLEAN", "BOOLEAN");
        info.addNativeTypeMapping("DATALINK", "BLOB",    "BLOB");

        info.setDefaultSize(Types.CHAR,      1024);
        info.setDefaultSize(Types.VARCHAR,   1024);
        info.setDefaultSize(Types.BINARY,    1024);
        info.setDefaultSize(Types.VARBINARY, 1024);
        
        setSqlBuilder(new MckoiBuilder(this));
        setModelReader(new MckoiModelReader(this));
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
        // For McKoi, you create databases by simply appending "?create=true" to the connection url
        if (JDBC_DRIVER.equals(jdbcDriverClassName))
        {
            StringBuffer creationUrl = new StringBuffer();
            Connection   connection  = null;

            creationUrl.append(connectionUrl);
            // TODO: It might be safer to parse the URN and check whethere there is already a parameter there
            //       (in which case e'd have to use '&' instead)
            creationUrl.append("?create=true");
            if ((parameters != null) && !parameters.isEmpty())
            {
                for (Iterator it = parameters.entrySet().iterator(); it.hasNext();)
                {
                    Map.Entry entry = (Map.Entry)it.next();

                    // no need to specify create twice (and create=false wouldn't help anyway)
                    if (!"create".equalsIgnoreCase(entry.getKey().toString()))
                    {
                        creationUrl.append("&");
                        creationUrl.append(entry.getKey().toString());
                        creationUrl.append("=");
                        if (entry.getValue() != null)
                        {
                            creationUrl.append(entry.getValue().toString());
                        }
                    }
                }
            }
            if (getLog().isDebugEnabled())
            {
                getLog().debug("About to create database using this URL: "+creationUrl.toString());
            }
            try
            {
                Class.forName(jdbcDriverClassName);

                connection = DriverManager.getConnection(creationUrl.toString(), username, password);
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
            throw new UnsupportedOperationException("Unable to create a Derby database via the driver "+jdbcDriverClassName);
        }
    }
}
