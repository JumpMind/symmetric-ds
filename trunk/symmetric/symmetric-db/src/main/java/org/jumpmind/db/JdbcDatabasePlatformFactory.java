package org.jumpmind.db;

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

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.db2.Db2Platform;
import org.jumpmind.db.platform.derby.DerbyPlatform;
import org.jumpmind.db.platform.firebird.FirebirdPlatform;
import org.jumpmind.db.platform.greenplum.GreenplumPlatform;
import org.jumpmind.db.platform.h2.H2Platform;
import org.jumpmind.db.platform.hsqldb.HsqlDbPlatform;
import org.jumpmind.db.platform.hsqldb2.HsqlDb2Platform;
import org.jumpmind.db.platform.informix.InformixPlatform;
import org.jumpmind.db.platform.interbase.InterbasePlatform;
import org.jumpmind.db.platform.mssql.MsSqlPlatform;
import org.jumpmind.db.platform.mysql.MySqlPlatform;
import org.jumpmind.db.platform.oracle.OraclePlatform;
import org.jumpmind.db.platform.postgresql.PostgreSqlPlatform;
import org.jumpmind.db.platform.sqlite.SqLitePlatform;
import org.jumpmind.db.platform.sybase.SybasePlatform;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.log.Log;
import org.jumpmind.log.LogFactory;

/*
 * A factory of {@link IDatabasePlatform} instances based on a case
 * insensitive database name. Note that this is a convenience class as the platforms
 * can also simply be created via their constructors.
 */
public class JdbcDatabasePlatformFactory {

    /* The database name -> platform map. */
    private static Map<String, Class<? extends IDatabasePlatform>> platforms = new HashMap<String, Class<? extends IDatabasePlatform>>();

    /*
     * Maps the sub-protocl part of a jdbc connection url to a OJB platform
     * name.
     */
    private static HashMap<String, String> jdbcSubProtocolToPlatform = new HashMap<String, String>();

    static {

        for (String name : H2Platform.DATABASENAMES) {
            addPlatform(platforms, name, H2Platform.class);
        }
        addPlatform(platforms, SqLitePlatform.DATABASENAME, SqLitePlatform.class);
        addPlatform(platforms, InformixPlatform.DATABASENAME, InformixPlatform.class);
        addPlatform(platforms, DerbyPlatform.DATABASENAME, DerbyPlatform.class);
        addPlatform(platforms, FirebirdPlatform.DATABASENAME, FirebirdPlatform.class);
        addPlatform(platforms, GreenplumPlatform.DATABASENAME, GreenplumPlatform.class);
        addPlatform(platforms, HsqlDbPlatform.DATABASENAME, HsqlDbPlatform.class);
        addPlatform(platforms, HsqlDb2Platform.DATABASENAME, HsqlDb2Platform.class);
        addPlatform(platforms, InterbasePlatform.DATABASENAME, InterbasePlatform.class);
        addPlatform(platforms, MsSqlPlatform.DATABASENAME, MsSqlPlatform.class);
        addPlatform(platforms, MySqlPlatform.DATABASENAME, MySqlPlatform.class);
        addPlatform(platforms, OraclePlatform.DATABASENAME, OraclePlatform.class);
        addPlatform(platforms, PostgreSqlPlatform.DATABASENAME, PostgreSqlPlatform.class);
        addPlatform(platforms, SybasePlatform.DATABASENAME, SybasePlatform.class);
        addPlatform(platforms, Db2Platform.DATABASENAME, Db2Platform.class);

        // Note that currently Sapdb and MaxDB have equal subprotocols and
        // drivers so we have no means to distinguish them
        jdbcSubProtocolToPlatform.put(Db2Platform.JDBC_SUBPROTOCOL, Db2Platform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(DerbyPlatform.JDBC_SUBPROTOCOL, DerbyPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(FirebirdPlatform.JDBC_SUBPROTOCOL,
                FirebirdPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(HsqlDbPlatform.JDBC_SUBPROTOCOL, HsqlDbPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(InterbasePlatform.JDBC_SUBPROTOCOL,
                InterbasePlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(MsSqlPlatform.JDBC_SUBPROTOCOL, MsSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(MySqlPlatform.JDBC_SUBPROTOCOL, MySqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(OraclePlatform.JDBC_SUBPROTOCOL_THIN,
                OraclePlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(OraclePlatform.JDBC_SUBPROTOCOL_OCI8,
                OraclePlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(OraclePlatform.JDBC_SUBPROTOCOL_THIN_OLD,
                OraclePlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(PostgreSqlPlatform.JDBC_SUBPROTOCOL,
                PostgreSqlPlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(SybasePlatform.JDBC_SUBPROTOCOL, SybasePlatform.DATABASENAME);
        jdbcSubProtocolToPlatform.put(FirebirdPlatform.JDBC_SUBPROTOCOL,
                FirebirdPlatform.DATABASENAME);
    }
    
    public static synchronized IDatabasePlatform createNewPlatformInstance(DataSource dataSource)
    throws DdlException {
        return createNewPlatformInstance(dataSource, null);
    }
    
    /*
     * Creates a new platform for the specified database.  Note that this method installs 
     * the data source in the returned platform instance.
     * 
     * @param dataSource The data source for the database
     * @param log The logger that the platform should use
     * 
     * @return The platform or <code>null</code> if the database is not
     * supported
     */
    public static synchronized IDatabasePlatform createNewPlatformInstance(DataSource dataSource, Log log)
            throws DdlException {
        
        if (log == null) {
            log = LogFactory.getLog("org.jumpmind");
        }
        
        // connects to the database and uses actual metadata info to get db name
        // and version to determine platform
        String[] nameVersion = determineDatabaseNameVersionSubprotocol(dataSource);

        Class<? extends IDatabasePlatform> clazz =  findPlatformClass(nameVersion);
        
        try {
            Constructor<? extends IDatabasePlatform> construtor = clazz.getConstructor(DataSource.class, Log.class);
            return construtor.newInstance(dataSource, log);
        } catch (Exception e) {
            throw new DdlException("Could not create a platform of type " + nameVersion[0], e);
        }
    }


    protected static synchronized Class<? extends IDatabasePlatform> findPlatformClass(
            String[] nameVersion) throws DdlException {
        Class<? extends IDatabasePlatform> platformClass = platforms.get(String.format("%s%s",
                nameVersion[0], nameVersion[1]).toLowerCase());
        if (platformClass == null) {
            platformClass = platforms.get(nameVersion[0].toLowerCase());
        }

        if (platformClass == null) {
            String databaseName = jdbcSubProtocolToPlatform.get(nameVersion[2]);
            if (databaseName != null) {
                platformClass = platforms.get(databaseName.toLowerCase());
            }
        }

        if (platformClass == null) {
            throw new DdlException("Could not find platform for database " + nameVersion[0]);
        } else {
            return platformClass;
        }

    }

    protected static String[] determineDatabaseNameVersionSubprotocol(DataSource dataSource)
             {
        Connection connection = null;
        String[] nameVersion = new String[3];
        try {
            connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            nameVersion[0] = metaData.getDatabaseProductName();
            nameVersion[1] = Integer.toString(metaData.getDatabaseMajorVersion());
            final String PREFIX = "jdbc:";
            String url = metaData.getURL();
            if (StringUtils.isNotBlank(url) && url.length() > PREFIX.length()) {
                url = url.substring(PREFIX.length());
                if (url.indexOf(":") > 0) {
                    url = url.substring(0, url.indexOf(":"));
                }
            }
            nameVersion[2] = url;

            /*
             * if the productName is PostgreSQL, it could be either PostgreSQL
             * or Greenplum
             */
            /* query the metadata to determine which one it is */
            if (nameVersion[0].equalsIgnoreCase(PostgreSqlPlatform.DATABASENAME)) {
                if (isGreenplumDatabase(connection)) {
                    nameVersion[0] = GreenplumPlatform.DATABASE;
                    nameVersion[1] = Integer.toString(getGreenplumVersion(connection));
                }
            }

            return nameVersion;
        } catch (SQLException ex) {
            throw new SqlException("Error while reading the database metadata: "
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

    private static boolean isGreenplumDatabase(Connection connection) {
        Statement stmt = null;
        ResultSet rs = null;
        String productName = null;
        boolean isGreenplum = false;
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(GreenplumPlatform.SQL_GET_GREENPLUM_NAME);
            while (rs.next()) {
                productName = rs.getString(1);
            }
            if (productName != null && productName.equalsIgnoreCase(GreenplumPlatform.DATABASE)) {
                isGreenplum = true;
            }
        } catch (SQLException ex) {
            // ignore the exception, if it is caught, then this is most likely
            // not
            // a greenplum database
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException ex) {
            }
        }
        return isGreenplum;
    }

    private static int getGreenplumVersion(Connection connection) {
        Statement stmt = null;
        ResultSet rs = null;
        String versionName = null;
        int productVersion = 0;
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(GreenplumPlatform.SQL_GET_GREENPLUM_VERSION);
            while (rs.next()) {
                versionName = rs.getString(1);
            }
            // take up to the first "." for version number
            if (versionName.indexOf('.') != -1) {
                versionName = versionName.substring(0, versionName.indexOf('.'));
            }
            try {
                productVersion = Integer.parseInt(versionName);
            } catch (NumberFormatException ex) {
                // if we can't convert this to a version number, leave it 0
            }
        } catch (SQLException ex) {
            // ignore the exception, if it is caught, then this is most likely
            // not
            // a greenplum database
        } finally {
            try {
                rs.close();
                stmt.close();
            } catch (SQLException ex) {
            }
        }
        return productVersion;
    }

    public static String getDatabaseProductVersion(DataSource dataSource) {
        Connection connection = null;

        try {
            connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            return metaData.getDatabaseProductVersion();
        } catch (SQLException ex) {
            throw new SqlException("Error while reading the database metadata: "
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
            throw new SqlException("Error while reading the database metadata: "
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
            throw new SqlException("Error while reading the database metadata: "
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

    private static synchronized void addPlatform(
            Map<String, Class<? extends IDatabasePlatform>> platformMap, String platformName,
            Class<? extends IDatabasePlatform> platformClass) {
        if (!IDatabasePlatform.class.isAssignableFrom(platformClass)) {
            throw new IllegalArgumentException("Cannot register class " + platformClass.getName()
                    + " because it does not implement the " + IDatabasePlatform.class.getName()
                    + " interface");
        }
        platformMap.put(platformName.toLowerCase(), platformClass);

    }
}
