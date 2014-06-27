package org.jumpmind.symmetric.ddl.platform.postgresql;

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
import java.sql.Statement;
import java.sql.Types;
import java.util.Iterator;
import java.util.Map;

import org.jumpmind.symmetric.ddl.DatabaseOperationException;
import org.jumpmind.symmetric.ddl.PlatformInfo;
import org.jumpmind.symmetric.ddl.platform.PlatformImplBase;

/**
 * The platform implementation for PostgresSql.
 * 
 * @version $Revision: 231306 $
 */
public class PostgreSqlPlatform extends PlatformImplBase
{
    /** Database name of this platform. */
    public static final String DATABASENAME      = "PostgreSql";
    /** The standard PostgreSQL jdbc driver. */
    public static final String JDBC_DRIVER       = "org.postgresql.Driver";
    /** The subprotocol used by the standard PostgreSQL driver. */
    public static final String JDBC_SUBPROTOCOL  = "postgresql";

    /**
     * Creates a new platform instance.
     */
    public PostgreSqlPlatform()
    {
        PlatformInfo info = getPlatformInfo();

        // this is the default length though it might be changed when building PostgreSQL
        // in file src/include/postgres_ext.h
        info.setMaxIdentifierLength(31);

        info.addNativeTypeMapping(Types.ARRAY,         "BYTEA",            Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.BINARY,        "BYTEA",            Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.BIT,           "BOOLEAN");
        info.addNativeTypeMapping(Types.BLOB,          "BYTEA",            Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.CLOB,          "TEXT",             Types.LONGVARCHAR);
        info.addNativeTypeMapping(Types.DECIMAL,       "NUMERIC",          Types.NUMERIC);
        info.addNativeTypeMapping(Types.DISTINCT,      "BYTEA",            Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.DOUBLE,        "DOUBLE PRECISION");
        info.addNativeTypeMapping(Types.FLOAT,         "DOUBLE PRECISION", Types.DOUBLE);
        info.addNativeTypeMapping(Types.JAVA_OBJECT,   "BYTEA",            Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.LONGVARBINARY, "BYTEA");
        info.addNativeTypeMapping(Types.LONGVARCHAR,   "TEXT",             Types.LONGVARCHAR);
        info.addNativeTypeMapping(Types.NULL,          "BYTEA",            Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.OTHER,         "BYTEA",            Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.REF,           "BYTEA",            Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.STRUCT,        "BYTEA",            Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.TINYINT,       "SMALLINT",         Types.SMALLINT);
        info.addNativeTypeMapping(Types.VARBINARY,     "BYTEA",            Types.LONGVARBINARY);
        info.addNativeTypeMapping("BOOLEAN",  "BOOLEAN", "BIT");
        info.addNativeTypeMapping("DATALINK", "BYTEA",   "LONGVARBINARY");

        info.setDefaultSize(Types.CHAR,    254);
        info.setDefaultSize(Types.VARCHAR, 254);

        // no support for specifying the size for these types (because they are mapped
        // to BYTEA which back-maps to BLOB)
        info.setHasSize(Types.BINARY,    false);
        info.setHasSize(Types.VARBINARY, false);

        setSqlBuilder(new PostgreSqlBuilder(this));
        setModelReader(new PostgreSqlModelReader(this));
    }

    /**
     * {@inheritDoc}
     */
    public String getName()
    {
        return DATABASENAME;
    }

    /**
     * Creates or drops the database referenced by the given connection url.
     * 
     * @param jdbcDriverClassName The jdbc driver class name
     * @param connectionUrl       The url to connect to the database if it were already created
     * @param username            The username for creating the database
     * @param password            The password for creating the database
     * @param parameters          Additional parameters for the operation
     * @param createDb            Whether to create or drop the database
     */
    private void createOrDropDatabase(String jdbcDriverClassName, String connectionUrl, String username, String password, Map parameters, boolean createDb) throws DatabaseOperationException, UnsupportedOperationException
    {
        if (JDBC_DRIVER.equals(jdbcDriverClassName))
        {
            int slashPos = connectionUrl.lastIndexOf('/');

            if (slashPos < 0)
            {
                throw new DatabaseOperationException("Cannot parse the given connection url "+connectionUrl);
            }

            int          paramPos   = connectionUrl.lastIndexOf('?');
            String       baseDb     = connectionUrl.substring(0, slashPos + 1) + "template1";
            String       dbName     = (paramPos > slashPos ? connectionUrl.substring(slashPos + 1, paramPos) : connectionUrl.substring(slashPos + 1));
            Connection   connection = null;
            Statement    stmt       = null;
            StringBuffer sql        = new StringBuffer();

            sql.append(createDb ? "CREATE" : "DROP");
            sql.append(" DATABASE ");
            sql.append(dbName);
            if ((parameters != null) && !parameters.isEmpty())
            {
                for (Iterator it = parameters.entrySet().iterator(); it.hasNext();)
                {
                    Map.Entry entry = (Map.Entry)it.next();

                    sql.append(" ");
                    sql.append(entry.getKey().toString());
                    if (entry.getValue() != null)
                    {
                        sql.append(" ");
                        sql.append(entry.getValue().toString());
                    }
                }
            }
            if (getLog().isDebugEnabled())
            {
                getLog().debug("About to create database via "+baseDb+" using this SQL: "+sql.toString());
            }
            try
            {
                Class.forName(jdbcDriverClassName);

                connection = DriverManager.getConnection(baseDb, username, password);
                stmt       = connection.createStatement();
                stmt.execute(sql.toString());
                logWarnings(connection);
            }
            catch (Exception ex)
            {
                throw new DatabaseOperationException("Error while trying to " + (createDb ? "create" : "drop") + " a database: "+ex.getLocalizedMessage(), ex);
            }
            finally
            {
                if (stmt != null)
                {
                    try
                    {
                        stmt.close();
                    }
                    catch (SQLException ex)
                    {}
                }
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
            throw new UnsupportedOperationException("Unable to " + (createDb ? "create" : "drop") + " a PostgreSQL database via the driver "+jdbcDriverClassName);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void createDatabase(String jdbcDriverClassName, String connectionUrl, String username, String password, Map parameters) throws DatabaseOperationException, UnsupportedOperationException
    {
        // With PostgreSQL, you create a database by executing "CREATE DATABASE" in an existing database (usually 
        // the template1 database because it usually exists)
        createOrDropDatabase(jdbcDriverClassName, connectionUrl, username, password, parameters, true);
    }

    /**
     * {@inheritDoc}
     */
    public void dropDatabase(String jdbcDriverClassName, String connectionUrl, String username, String password) throws DatabaseOperationException, UnsupportedOperationException
    {
        // With PostgreSQL, you create a database by executing "DROP DATABASE" in an existing database (usually 
        // the template1 database because it usually exists)
        createOrDropDatabase(jdbcDriverClassName, connectionUrl, username, password, null, false);
    }

}
