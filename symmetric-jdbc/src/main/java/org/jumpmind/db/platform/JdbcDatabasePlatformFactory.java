package org.jumpmind.db.platform;

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
import org.jumpmind.db.platform.db2.Db2DatabasePlatform;
import org.jumpmind.db.platform.derby.DerbyDatabasePlatform;
import org.jumpmind.db.platform.firebird.FirebirdDatabasePlatform;
import org.jumpmind.db.platform.greenplum.GreenplumPlatform;
import org.jumpmind.db.platform.h2.H2DatabasePlatform;
import org.jumpmind.db.platform.hsqldb.HsqlDbDatabasePlatform;
import org.jumpmind.db.platform.hsqldb2.HsqlDb2DatabasePlatform;
import org.jumpmind.db.platform.informix.InformixDatabasePlatform;
import org.jumpmind.db.platform.interbase.InterbaseDatabasePlatform;
import org.jumpmind.db.platform.mariadb.MariaDBDatabasePlatform;
import org.jumpmind.db.platform.mssql.MsSqlDatabasePlatform;
import org.jumpmind.db.platform.mssql2000.MsSql2000DatabasePlatform;
import org.jumpmind.db.platform.mysql.MySqlDatabasePlatform;
import org.jumpmind.db.platform.oracle.OracleDatabasePlatform;
import org.jumpmind.db.platform.postgresql.PostgreSqlDatabasePlatform;
import org.jumpmind.db.platform.sqlite.SqliteDatabasePlatform;
import org.jumpmind.db.platform.sybase.SybaseDatabasePlatform;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlTemplateSettings;

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
    private static Map<String, Class<? extends IDatabasePlatform>> jdbcSubProtocolToPlatform = new HashMap<String, Class<? extends IDatabasePlatform>>();

    static {

        addPlatform(platforms, "H2", H2DatabasePlatform.class);
        addPlatform(platforms, "H21", H2DatabasePlatform.class);
        addPlatform(platforms, "Informix Dynamic Server11", InformixDatabasePlatform.class);        
        addPlatform(platforms, "Apache Derby", DerbyDatabasePlatform.class);
        addPlatform(platforms, "Firebird", FirebirdDatabasePlatform.class);
        addPlatform(platforms, DatabaseNamesConstants.GREENPLUM, GreenplumPlatform.class);
        addPlatform(platforms, "HsqlDb", HsqlDbDatabasePlatform.class);
        addPlatform(platforms, "HSQL Database Engine2", HsqlDb2DatabasePlatform.class);
        addPlatform(platforms, "Interbase", InterbaseDatabasePlatform.class);
        addPlatform(platforms, "MariaDB", MariaDBDatabasePlatform.class);
        addPlatform(platforms, "MsSQL", MsSqlDatabasePlatform.class);
        addPlatform(platforms, "microsoft sql server8", MsSql2000DatabasePlatform.class);
        addPlatform(platforms, "microsoft sql server11", MsSqlDatabasePlatform.class);
        addPlatform(platforms, "microsoft sql server", MsSqlDatabasePlatform.class);
        addPlatform(platforms, "MySQL", MySqlDatabasePlatform.class);
        addPlatform(platforms, "Oracle", OracleDatabasePlatform.class);
        addPlatform(platforms, "PostgreSql", PostgreSqlDatabasePlatform.class);
        addPlatform(platforms, "Sybase", SybaseDatabasePlatform.class);
        addPlatform(platforms, "DB2", Db2DatabasePlatform.class);
        addPlatform(platforms, "SQLite", SqliteDatabasePlatform.class);

        jdbcSubProtocolToPlatform.put(Db2DatabasePlatform.JDBC_SUBPROTOCOL, Db2DatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(DerbyDatabasePlatform.JDBC_SUBPROTOCOL, DerbyDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(FirebirdDatabasePlatform.JDBC_SUBPROTOCOL,
                FirebirdDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(HsqlDbDatabasePlatform.JDBC_SUBPROTOCOL, HsqlDbDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(InterbaseDatabasePlatform.JDBC_SUBPROTOCOL,
                InterbaseDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(MsSqlDatabasePlatform.JDBC_SUBPROTOCOL, MsSqlDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(MsSql2000DatabasePlatform.JDBC_SUBPROTOCOL, MsSql2000DatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(MySqlDatabasePlatform.JDBC_SUBPROTOCOL, MySqlDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(OracleDatabasePlatform.JDBC_SUBPROTOCOL_THIN,
                OracleDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(OracleDatabasePlatform.JDBC_SUBPROTOCOL_OCI8,
                OracleDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(OracleDatabasePlatform.JDBC_SUBPROTOCOL_THIN_OLD,
                OracleDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(PostgreSqlDatabasePlatform.JDBC_SUBPROTOCOL,
                PostgreSqlDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(SybaseDatabasePlatform.JDBC_SUBPROTOCOL, SybaseDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(FirebirdDatabasePlatform.JDBC_SUBPROTOCOL,
                FirebirdDatabasePlatform.class);
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
    public static synchronized IDatabasePlatform createNewPlatformInstance(DataSource dataSource, SqlTemplateSettings settings, boolean delimitedIdentifierMode)
            throws DdlException {
        
        // connects to the database and uses actual metadata info to get db name
        // and version to determine platform
        String[] nameVersion = determineDatabaseNameVersionSubprotocol(dataSource);

        Class<? extends IDatabasePlatform> clazz =  findPlatformClass(nameVersion);
        
        try {
            Constructor<? extends IDatabasePlatform> construtor = clazz.getConstructor(DataSource.class, SqlTemplateSettings.class);
            IDatabasePlatform platform = construtor.newInstance(dataSource, settings);
            platform.getDdlBuilder().setDelimitedIdentifierModeOn(delimitedIdentifierMode);
            return platform;
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
            platformClass = jdbcSubProtocolToPlatform.get(nameVersion[2]);
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
            if (nameVersion[0].equalsIgnoreCase("PostgreSql")) {
                if (isGreenplumDatabase(connection)) {
                    nameVersion[0] = DatabaseNamesConstants.GREENPLUM;
                    nameVersion[1] = Integer.toString(getGreenplumVersion(connection));
                }
            }
            
            /*
             * if the productName is MySQL, it could be either MysSQL or MariaDB
             * query the metadata to determine which one it is 
             */
            if (nameVersion[0].equalsIgnoreCase(DatabaseNamesConstants.MYSQL)) {
                if (isMariaDBDatabase(connection)) {
                    nameVersion[0] = DatabaseNamesConstants.MARIADB;
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
            if (productName != null && productName.equalsIgnoreCase("Greenplum")) {
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

    private static boolean isMariaDBDatabase(Connection connection) {
        Statement stmt = null;
        ResultSet rs = null;
        String productName = null;
        boolean isMariaDB = false;
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(MariaDBDatabasePlatform.SQL_GET_MARIADB_NAME);
            while (rs.next()) {
                productName = rs.getString(1);
            }
            if (productName != null && StringUtils.containsIgnoreCase(productName, DatabaseNamesConstants.MARIADB)) {
                isMariaDB = true;
            }
        } catch (SQLException ex) {
            // ignore the exception, if it is caught, then this is most likely
            // not a mariadb database
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
        return isMariaDB;
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
