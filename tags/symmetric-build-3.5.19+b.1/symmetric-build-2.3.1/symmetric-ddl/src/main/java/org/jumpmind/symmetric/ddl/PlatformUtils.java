package org.jumpmind.symmetric.ddl;

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
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.jumpmind.symmetric.ddl.platform.axion.AxionPlatform;
import org.jumpmind.symmetric.ddl.platform.cloudscape.CloudscapePlatform;
import org.jumpmind.symmetric.ddl.platform.db2.Db2Platform;
import org.jumpmind.symmetric.ddl.platform.derby.DerbyPlatform;
import org.jumpmind.symmetric.ddl.platform.firebird.FirebirdPlatform;
import org.jumpmind.symmetric.ddl.platform.hsqldb.HsqlDbPlatform;
import org.jumpmind.symmetric.ddl.platform.interbase.InterbasePlatform;
import org.jumpmind.symmetric.ddl.platform.mckoi.MckoiPlatform;
import org.jumpmind.symmetric.ddl.platform.mssql.MSSqlPlatform;
import org.jumpmind.symmetric.ddl.platform.mysql.MySqlPlatform;
import org.jumpmind.symmetric.ddl.platform.oracle.Oracle8Platform;
import org.jumpmind.symmetric.ddl.platform.postgresql.PostgreSqlPlatform;
import org.jumpmind.symmetric.ddl.platform.sapdb.SapDbPlatform;
import org.jumpmind.symmetric.ddl.platform.sybase.SybasePlatform;

/**
 * Utility functions for dealing with database platforms.
 * 
 * @version $Revision: 279421 $
 */
public class PlatformUtils
{
    private PlatformUtils() {
    }
    
    // Extended drivers that support more than one database

    /** The DataDirect Connect DB2 jdbc driver. */
    public static final String JDBC_DRIVER_DATADIRECT_DB2        = "com.ddtek.jdbc.db2.DB2Driver";
    /** The DataDirect Connect SQLServer jdbc driver. */
    public static final String JDBC_DRIVER_DATADIRECT_SQLSERVER  = "com.ddtek.jdbc.sqlserver.SQLServerDriver";
    /** The DataDirect Connect Oracle jdbc driver. */
    public static final String JDBC_DRIVER_DATADIRECT_ORACLE     = "com.ddtek.jdbc.oracle.OracleDriver";
    /** The DataDirect Connect Sybase jdbc driver. */
    public static final String JDBC_DRIVER_DATADIRECT_SYBASE     = "com.ddtek.jdbc.sybase.SybaseDriver";
    /** The i-net DB2 jdbc driver. */
    public static final String JDBC_DRIVER_INET_DB2              = "com.inet.drda.DRDADriver";
    /** The i-net Oracle jdbc driver. */
    public static final String JDBC_DRIVER_INET_ORACLE           = "com.inet.ora.OraDriver";
    /** The i-net SQLServer jdbc driver. */
    public static final String JDBC_DRIVER_INET_SQLSERVER        = "com.inet.tds.TdsDriver";
    /** The i-net Sybase jdbc driver. */
    public static final String JDBC_DRIVER_INET_SYBASE           = "com.inet.syb.SybDriver";
    /** The i-net pooled jdbc driver for SQLServer and Sybase. */
    public static final String JDBC_DRIVER_INET_POOLED           = "com.inet.pool.PoolDriver";
    /** The JNetDirect SQLServer jdbc driver. */
    public static final String JDBC_DRIVER_JSQLCONNECT_SQLSERVER = "com.jnetdirect.jsql.JSQLDriver";
    /** The jTDS jdbc driver for SQLServer and Sybase. */
    public static final String JDBC_DRIVER_JTDS                  = "net.sourceforge.jtds.jdbc.Driver";

    /** The subprotocol used by the DataDirect DB2 driver. */
    public static final String JDBC_SUBPROTOCOL_DATADIRECT_DB2            = "datadirect:db2";
    /** The subprotocol used by the DataDirect SQLServer driver. */
    public static final String JDBC_SUBPROTOCOL_DATADIRECT_SQLSERVER      = "datadirect:sqlserver";
    /** The subprotocol used by the DataDirect Oracle driver. */
    public static final String JDBC_SUBPROTOCOL_DATADIRECT_ORACLE         = "datadirect:oracle";
    /** The subprotocol used by the DataDirect Sybase driver. */
    public static final String JDBC_SUBPROTOCOL_DATADIRECT_SYBASE         = "datadirect:sybase";
    /** The subprotocol used by the i-net DB2 driver. */
    public static final String JDBC_SUBPROTOCOL_INET_DB2                  = "inetdb2";
    /** The subprotocol used by the i-net Oracle driver. */
    public static final String JDBC_SUBPROTOCOL_INET_ORACLE               = "inetora";
    /** A subprotocol used by the i-net SQLServer driver. */
    public static final String JDBC_SUBPROTOCOL_INET_SQLSERVER            = "inetdae";
    /** A subprotocol used by the i-net SQLServer driver. */
    public static final String JDBC_SUBPROTOCOL_INET_SQLSERVER6           = "inetdae6";
    /** A subprotocol used by the i-net SQLServer driver. */
    public static final String JDBC_SUBPROTOCOL_INET_SQLSERVER7           = "inetdae7";
    /** A subprotocol used by the i-net SQLServer driver. */
    public static final String JDBC_SUBPROTOCOL_INET_SQLSERVER7A          = "inetdae7a";
    /** A subprotocol used by the pooled i-net SQLServer driver. */
    public static final String JDBC_SUBPROTOCOL_INET_SQLSERVER_POOLED_1   = "inetpool:inetdae";
    /** A subprotocol used by the pooled i-net SQLServer driver. */
    public static final String JDBC_SUBPROTOCOL_INET_SQLSERVER6_POOLED_1  = "inetpool:inetdae6";
    /** A subprotocol used by the pooled i-net SQLServer driver. */
    public static final String JDBC_SUBPROTOCOL_INET_SQLSERVER7_POOLED_1  = "inetpool:inetdae7";
    /** A subprotocol used by the pooled i-net SQLServer driver. */
    public static final String JDBC_SUBPROTOCOL_INET_SQLSERVER7A_POOLED_1 = "inetpool:inetdae7a";
    /** A subprotocol used by the pooled i-net SQLServer driver. */
    public static final String JDBC_SUBPROTOCOL_INET_SQLSERVER_POOLED_2   = "inetpool:jdbc:inetdae";
    /** A subprotocol used by the pooled i-net SQLServer driver. */
    public static final String JDBC_SUBPROTOCOL_INET_SQLSERVER6_POOLED_2  = "inetpool:jdbc:inetdae6";
    /** A subprotocol used by the pooled i-net SQLServer driver. */
    public static final String JDBC_SUBPROTOCOL_INET_SQLSERVER7_POOLED_2  = "inetpool:jdbc:inetdae7";
    /** A subprotocol used by the pooled i-net SQLServer driver. */
    public static final String JDBC_SUBPROTOCOL_INET_SQLSERVER7A_POOLED_2 = "inetpool:jdbc:inetdae7a";
    /** The subprotocol used by the i-net Sybase driver. */
    public static final String JDBC_SUBPROTOCOL_INET_SYBASE               = "inetsyb";
    /** The subprotocol used by the pooled i-net Sybase driver. */
    public static final String JDBC_SUBPROTOCOL_INET_SYBASE_POOLED_1      = "inetpool:inetsyb";
    /** The subprotocol used by the pooled i-net Sybase driver. */
    public static final String JDBC_SUBPROTOCOL_INET_SYBASE_POOLED_2      = "inetpool:jdbc:inetsyb";
    /** The subprotocol used by the JNetDirect SQLServer driver. */
    public static final String JDBC_SUBPROTOCOL_JSQLCONNECT_SQLSERVER     = "JSQLConnect";
    /** The subprotocol used by the jTDS SQLServer driver. */
    public static final String JDBC_SUBPROTOCOL_JTDS_SQLSERVER            = "jtds:sqlserver";
    /** The subprotocol used by the jTDS Sybase driver. */
    public static final String JDBC_SUBPROTOCOL_JTDS_SYBASE               = "jtds:sybase";

    /** Maps the sub-protocl part of a jdbc connection url to a OJB platform name. */
    private static HashMap<String,String> jdbcSubProtocolToPlatform = new HashMap<String,String>();
    /** Maps the jdbc driver name to a OJB platform name. */
    private static HashMap<String,String> jdbcDriverToPlatform      = new HashMap<String,String>();

    /**
     * Creates a new instance.
     */
    static
    {
        // Note that currently Sapdb and MaxDB have equal subprotocols and
        // drivers so we have no means to distinguish them
        jdbcSubProtocolToPlatform.put(AxionPlatform.JDBC_SUBPROTOCOL,                           AxionPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(CloudscapePlatform.JDBC_SUBPROTOCOL_1,                    CloudscapePlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(CloudscapePlatform.JDBC_SUBPROTOCOL_2,                    CloudscapePlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(Db2Platform.JDBC_SUBPROTOCOL,                             Db2Platform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(Db2Platform.JDBC_SUBPROTOCOL_OS390_1,                     Db2Platform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(Db2Platform.JDBC_SUBPROTOCOL_OS390_2,                     Db2Platform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(Db2Platform.JDBC_SUBPROTOCOL_JTOPEN,                      Db2Platform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_DATADIRECT_DB2,            Db2Platform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_DB2,                  Db2Platform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(DerbyPlatform.JDBC_SUBPROTOCOL,                           DerbyPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(FirebirdPlatform.JDBC_SUBPROTOCOL,                        FirebirdPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(HsqlDbPlatform.JDBC_SUBPROTOCOL,                          HsqlDbPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(InterbasePlatform.JDBC_SUBPROTOCOL,                       InterbasePlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(SapDbPlatform.JDBC_SUBPROTOCOL,                           SapDbPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(MckoiPlatform.JDBC_SUBPROTOCOL,                           MckoiPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(MSSqlPlatform.JDBC_SUBPROTOCOL,                           MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(MSSqlPlatform.JDBC_SUBPROTOCOL_NEW,                       MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(MSSqlPlatform.JDBC_SUBPROTOCOL_INTERNAL,                  MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_DATADIRECT_SQLSERVER,      MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_SQLSERVER,            MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_SQLSERVER6,           MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_SQLSERVER7,           MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_SQLSERVER7A,          MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_SQLSERVER_POOLED_1,   MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_SQLSERVER6_POOLED_1,  MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_SQLSERVER7_POOLED_1,  MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_SQLSERVER7A_POOLED_1, MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_SQLSERVER_POOLED_2,   MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_SQLSERVER6_POOLED_2,  MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_SQLSERVER7_POOLED_2,  MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_SQLSERVER7A_POOLED_2, MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_JSQLCONNECT_SQLSERVER,     MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_JTDS_SQLSERVER,            MSSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(MySqlPlatform.JDBC_SUBPROTOCOL,                           MySqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(Oracle8Platform.JDBC_SUBPROTOCOL_THIN,                    Oracle8Platform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(Oracle8Platform.JDBC_SUBPROTOCOL_OCI8,                    Oracle8Platform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(Oracle8Platform.JDBC_SUBPROTOCOL_THIN_OLD,                Oracle8Platform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_DATADIRECT_ORACLE,         Oracle8Platform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_ORACLE,               Oracle8Platform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PostgreSqlPlatform.JDBC_SUBPROTOCOL,                      PostgreSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(SybasePlatform.JDBC_SUBPROTOCOL,                          SybasePlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_DATADIRECT_SYBASE,         SybasePlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_SYBASE,               SybasePlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_SYBASE_POOLED_1,      SybasePlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_INET_SYBASE_POOLED_2,      SybasePlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PlatformUtils.JDBC_SUBPROTOCOL_JTDS_SYBASE,               SybasePlatform.DATABASENAME);

        jdbcDriverToPlatform.put(AxionPlatform.JDBC_DRIVER,                       AxionPlatform.DATABASENAME);
        jdbcDriverToPlatform.put(Db2Platform.JDBC_DRIVER,                         Db2Platform.DATABASENAME);
        jdbcDriverToPlatform.put(Db2Platform.JDBC_DRIVER_OLD1,                    Db2Platform.DATABASENAME);
        jdbcDriverToPlatform.put(Db2Platform.JDBC_DRIVER_OLD2,                    Db2Platform.DATABASENAME);
        jdbcDriverToPlatform.put(Db2Platform.JDBC_DRIVER_JTOPEN,                  Db2Platform.DATABASENAME);
        jdbcDriverToPlatform.put(PlatformUtils.JDBC_DRIVER_DATADIRECT_DB2,        Db2Platform.DATABASENAME);
        jdbcDriverToPlatform.put(PlatformUtils.JDBC_DRIVER_INET_DB2,              Db2Platform.DATABASENAME);
        jdbcDriverToPlatform.put(DerbyPlatform.JDBC_DRIVER_EMBEDDED,              DerbyPlatform.DATABASENAME);
        jdbcDriverToPlatform.put(DerbyPlatform.JDBC_DRIVER,                       DerbyPlatform.DATABASENAME);
        jdbcDriverToPlatform.put(FirebirdPlatform.JDBC_DRIVER,                    FirebirdPlatform.DATABASENAME);
        jdbcDriverToPlatform.put(HsqlDbPlatform.JDBC_DRIVER,                      HsqlDbPlatform.DATABASENAME);
        jdbcDriverToPlatform.put(InterbasePlatform.JDBC_DRIVER,                   InterbasePlatform.DATABASENAME);
        jdbcDriverToPlatform.put(SapDbPlatform.JDBC_DRIVER,                       SapDbPlatform.DATABASENAME);
        jdbcDriverToPlatform.put(MckoiPlatform.JDBC_DRIVER,                       MckoiPlatform.DATABASENAME);
        jdbcDriverToPlatform.put(MSSqlPlatform.JDBC_DRIVER,                       MSSqlPlatform.DATABASENAME);
        jdbcDriverToPlatform.put(MSSqlPlatform.JDBC_DRIVER_NEW,                   MSSqlPlatform.DATABASENAME);
        jdbcDriverToPlatform.put(PlatformUtils.JDBC_DRIVER_DATADIRECT_SQLSERVER,  MSSqlPlatform.DATABASENAME);
        jdbcDriverToPlatform.put(PlatformUtils.JDBC_DRIVER_INET_SQLSERVER,        MSSqlPlatform.DATABASENAME);
        jdbcDriverToPlatform.put(PlatformUtils.JDBC_DRIVER_JSQLCONNECT_SQLSERVER, MSSqlPlatform.DATABASENAME);
        jdbcDriverToPlatform.put(MySqlPlatform.JDBC_DRIVER,                       MySqlPlatform.DATABASENAME);
        jdbcDriverToPlatform.put(MySqlPlatform.JDBC_DRIVER_OLD,                   MySqlPlatform.DATABASENAME);
        jdbcDriverToPlatform.put(Oracle8Platform.JDBC_DRIVER,                     Oracle8Platform.DATABASENAME);
        jdbcDriverToPlatform.put(Oracle8Platform.JDBC_DRIVER_OLD,                 Oracle8Platform.DATABASENAME);
        jdbcDriverToPlatform.put(PlatformUtils.JDBC_DRIVER_DATADIRECT_ORACLE,     Oracle8Platform.DATABASENAME);
        jdbcDriverToPlatform.put(PlatformUtils.JDBC_DRIVER_INET_ORACLE,           Oracle8Platform.DATABASENAME);
        jdbcDriverToPlatform.put(PostgreSqlPlatform.JDBC_DRIVER,                  PostgreSqlPlatform.DATABASENAME);
        jdbcDriverToPlatform.put(SybasePlatform.JDBC_DRIVER,                      SybasePlatform.DATABASENAME);
        jdbcDriverToPlatform.put(SybasePlatform.JDBC_DRIVER_OLD,                  SybasePlatform.DATABASENAME);
        jdbcDriverToPlatform.put(PlatformUtils.JDBC_DRIVER_DATADIRECT_SYBASE,     SybasePlatform.DATABASENAME);
        jdbcDriverToPlatform.put(PlatformUtils.JDBC_DRIVER_INET_SYBASE,           SybasePlatform.DATABASENAME);
    }

    /**
     * Tries to determine the database type for the given data source. Note that this will establish
     * a connection to the database.
     * 
     * @param dataSource The data source
     * @return The database type or <code>null</code> if the database type couldn't be determined
     */
    public static String determineDatabaseType(DataSource dataSource) throws DatabaseOperationException
    {
        Connection connection = null;

        try
        {
            connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            return determineDatabaseType(metaData.getDriverName(), metaData.getURL());
        }
        catch (SQLException ex)
        {
            throw new DatabaseOperationException("Error while reading the database metadata: " + ex.getMessage(), ex);
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
                {
                    // we ignore this one
                }
            }
        }
    }
    
    public static String getDatabaseProductVersion(DataSource dataSource) {        
        Connection connection = null;

        try {
            connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            return metaData.getDatabaseProductVersion();
        } catch (SQLException ex) {
            throw new DatabaseOperationException("Error while reading the database metadata: "
                    + ex.getMessage(), ex);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    // we ignore this one
                }
            }
        }
    }
    
    public static int getDatabaseMajorVersion(DataSource dataSource) {        
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            return metaData.getDatabaseMajorVersion();
        } catch (SQLException ex) {
            throw new DatabaseOperationException("Error while reading the database metadata: "
                    + ex.getMessage(), ex);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    // we ignore this one
                }
            }
        }
    }
    
    public static int getDatabaseMinorVersion(DataSource dataSource) {        
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            return metaData.getDatabaseMinorVersion();
        } catch (SQLException ex) {
            throw new DatabaseOperationException("Error while reading the database metadata: "
                    + ex.getMessage(), ex);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    // we ignore this one
                }
            }
        }
    }    
    
    public static String determineDatabaseNameVersion(DataSource dataSource) throws DatabaseOperationException {
        Connection connection = null;

        try {
            connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            String productName = metaData.getDatabaseProductName();
            int majorVersion = metaData.getDatabaseMajorVersion();
            String productString = productName;
            if (majorVersion > 0) {
                productString += majorVersion;
            }

            return productString;
        } catch (SQLException ex) {
            throw new DatabaseOperationException("Error while reading the database metadata: "
                    + ex.getMessage(), ex);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    // we ignore this one
                }
            }
        }
    }    

    /**
     * Tries to determine the database type for the given jdbc driver and connection url.
     * 
     * @param driverName        The fully qualified name of the JDBC driver 
     * @param jdbcConnectionUrl The connection url
     * @return The database type or <code>null</code> if the database type couldn't be determined
     */
    public static String determineDatabaseType(String driverName, String jdbcConnectionUrl)
    {
        if (jdbcDriverToPlatform.containsKey(driverName))
        {
            return (String)jdbcDriverToPlatform.get(driverName);
        }
        if (jdbcConnectionUrl == null)
        {
            return null;
        }
        for (Iterator<Entry<String,String>> it = jdbcSubProtocolToPlatform.entrySet().iterator(); it.hasNext();)
        {
            Map.Entry<String,String> entry          = (Map.Entry<String,String>)it.next();
            String    curSubProtocol = "jdbc:" + (String)entry.getKey() + ":";

            if (jdbcConnectionUrl.startsWith(curSubProtocol))
            {
                return (String)entry.getValue();
            }
        }
        return null;
    }
}
