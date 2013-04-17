package org.jumpmind.symmetric.ddl.platform.derby;

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
import org.jumpmind.symmetric.ddl.platform.cloudscape.CloudscapePlatform;

/**
 * The platform implementation for Derby.
 * 
 * @version $Revision: 231306 $
 */
public class DerbyPlatform extends CloudscapePlatform
{
    /** Database name of this platform. */
    public static final String DATABASENAME         = "Derby";
    /** The derby jdbc driver for use as a client for a normal server. */
    public static final String JDBC_DRIVER          = "org.apache.derby.jdbc.ClientDriver";
    /** The derby jdbc driver for use as an embedded database. */
    public static final String JDBC_DRIVER_EMBEDDED = "org.apache.derby.jdbc.EmbeddedDriver";
    /** The subprotocol used by the derby drivers. */
    public static final String JDBC_SUBPROTOCOL     = "derby";

    /**
     * Creates a new Derby platform instance.
     */
    public DerbyPlatform()
    {
        super();
        getPlatformInfo().addNativeTypeMapping(Types.DOUBLE, "DOUBLE");
        getPlatformInfo().addNativeTypeMapping(Types.FLOAT,  "DOUBLE", Types.DOUBLE);
        setSqlBuilder(new DerbyBuilder(this));
        setModelReader(new DerbyModelReader(this));
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
        // For Derby, you create databases by simply appending ";create=true" to the connection url
        if (JDBC_DRIVER.equals(jdbcDriverClassName) ||
            JDBC_DRIVER_EMBEDDED.equals(jdbcDriverClassName))
        {
            StringBuffer creationUrl = new StringBuffer();
            Connection   connection  = null;

            creationUrl.append(connectionUrl);
            creationUrl.append(";create=true");
            if ((parameters != null) && !parameters.isEmpty())
            {
                for (Iterator it = parameters.entrySet().iterator(); it.hasNext();)
                {
                    Map.Entry entry = (Map.Entry)it.next();

                    // no need to specify create twice (and create=false wouldn't help anyway)
                    if (!"create".equalsIgnoreCase(entry.getKey().toString()))
                    {
                        creationUrl.append(";");
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
