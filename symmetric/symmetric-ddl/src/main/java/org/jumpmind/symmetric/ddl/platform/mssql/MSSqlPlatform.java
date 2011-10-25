package org.jumpmind.symmetric.ddl.platform.mssql;

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

import org.jumpmind.symmetric.ddl.PlatformInfo;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.platform.PlatformImplBase;

/*
 * The platform implementation for the Microsoft SQL Server database.
 * 
 * @version $Revision: 231306 $
 */
public class MSSqlPlatform extends PlatformImplBase
{
    /* Database name of this platform. */
    public static final String DATABASENAME         = "MsSql";
    /* The standard SQLServer jdbc driver. */
    public static final String JDBC_DRIVER          = "com.microsoft.jdbc.sqlserver.SQLServerDriver";
    /* The new SQLServer 2005 jdbc driver which can also be used for SQL Server 2000. */
    public static final String JDBC_DRIVER_NEW      = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    /* The subprotocol used by the standard SQL Server driver. */
    public static final String JDBC_SUBPROTOCOL     = "microsoft:sqlserver";
    /* The subprotocol recommended for the newer SQL Server 2005 driver. */
    public static final String JDBC_SUBPROTOCOL_NEW = "sqlserver";
    /* The subprotocol internally returned by the newer SQL Server 2005 driver. */
    public static final String JDBC_SUBPROTOCOL_INTERNAL = "sqljdbc";

    /*
     * Creates a new platform instance.
     */
    public MSSqlPlatform()
    {
        PlatformInfo info = getPlatformInfo();

        info.setMaxIdentifierLength(128);

        info.addNativeTypeMapping(Types.ARRAY,         "IMAGE",         Types.LONGVARBINARY);
        // BIGINT will be mapped back to BIGINT by the model reader 
        info.addNativeTypeMapping(Types.BIGINT,        "DECIMAL(19,0)");
        info.addNativeTypeMapping(Types.BLOB,          "IMAGE",         Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.CLOB,          "TEXT",          Types.LONGVARCHAR);
        info.addNativeTypeMapping(Types.DATE,          "DATETIME",      Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.DISTINCT,      "IMAGE",         Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.DOUBLE,        "FLOAT",         Types.FLOAT);
        info.addNativeTypeMapping(Types.INTEGER,       "INT");
        info.addNativeTypeMapping(Types.JAVA_OBJECT,   "IMAGE",         Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.LONGVARBINARY, "IMAGE");
        info.addNativeTypeMapping(Types.LONGVARCHAR,   "TEXT");
        info.addNativeTypeMapping(Types.NULL,          "IMAGE",         Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.OTHER,         "IMAGE",         Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.REF,           "IMAGE",         Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.STRUCT,        "IMAGE",         Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.TIME,          "DATETIME",      Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.TIMESTAMP,     "DATETIME");
        info.addNativeTypeMapping(Types.TINYINT,       "SMALLINT",      Types.SMALLINT);
        info.addNativeTypeMapping("BOOLEAN",  "BIT",   "BIT");
        info.addNativeTypeMapping("DATALINK", "IMAGE", "LONGVARBINARY");

        info.setDefaultSize(Types.CHAR,       254);
        info.setDefaultSize(Types.VARCHAR,    254);
        info.setDefaultSize(Types.BINARY,     254);
        info.setDefaultSize(Types.VARBINARY,  254);
        
        info.setStoresUpperCaseInCatalog(true);

        setSqlBuilder(new MSSqlBuilder(this));
        setModelReader(new MSSqlModelReader(this));
        
        setDelimitedIdentifierModeOn(true);
    }

    /*
     * {@inheritDoc}
     */
    public String getName()
    {
        return DATABASENAME;
    }

    /*
     * Determines whether we need to use identity override mode for the given table.
     * 
     * @param table The table
     * @return <code>true</code> if identity override mode is needed
     */
    private boolean useIdentityOverrideFor(Table table)
    {
        return isIdentityOverrideOn() &&
               getPlatformInfo().isIdentityOverrideAllowed() &&
               (table.getAutoIncrementColumns().length > 0);
    }

    /*
     * {@inheritDoc}
     */
    protected void beforeInsert(Connection connection, Table table) throws SQLException
    {
        if (useIdentityOverrideFor(table))
        {
            MSSqlBuilder builder = (MSSqlBuilder)getSqlBuilder();
    
            connection.createStatement().execute(builder.getEnableIdentityOverrideSql(table));
        }
    }

    /*
     * {@inheritDoc}
     */
    protected void afterInsert(Connection connection, Table table) throws SQLException
    {
        if (useIdentityOverrideFor(table))
        {
            MSSqlBuilder builder = (MSSqlBuilder)getSqlBuilder();
    
            connection.createStatement().execute(builder.getDisableIdentityOverrideSql(table));
        }
    }

    /*
     * {@inheritDoc}
     */
    protected void beforeUpdate(Connection connection, Table table) throws SQLException
    {
        beforeInsert(connection, table);
    }

    /*
     * {@inheritDoc}
     */
    protected void afterUpdate(Connection connection, Table table) throws SQLException
    {
        afterInsert(connection, table);
    }
}
