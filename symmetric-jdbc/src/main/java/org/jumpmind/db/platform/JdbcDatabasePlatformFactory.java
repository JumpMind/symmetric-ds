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
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.platform.ase.AseDatabasePlatform;
import org.jumpmind.db.platform.db2.Db2As400DatabasePlatform;
import org.jumpmind.db.platform.db2.Db2DatabasePlatform;
import org.jumpmind.db.platform.db2.Db2zOsDatabasePlatform;
import org.jumpmind.db.platform.derby.DerbyDatabasePlatform;
import org.jumpmind.db.platform.firebird.FirebirdDatabasePlatform;
import org.jumpmind.db.platform.firebird.FirebirdDialect1DatabasePlatform;
import org.jumpmind.db.platform.generic.GenericJdbcDatabasePlatform;
import org.jumpmind.db.platform.greenplum.GreenplumPlatform;
import org.jumpmind.db.platform.h2.H2DatabasePlatform;
import org.jumpmind.db.platform.hana.HanaDatabasePlatform;
import org.jumpmind.db.platform.hbase.HbasePlatform;
import org.jumpmind.db.platform.hsqldb.HsqlDbDatabasePlatform;
import org.jumpmind.db.platform.hsqldb2.HsqlDb2DatabasePlatform;
import org.jumpmind.db.platform.informix.InformixDatabasePlatform;
import org.jumpmind.db.platform.ingres.IngresDatabasePlatform;
import org.jumpmind.db.platform.interbase.InterbaseDatabasePlatform;
import org.jumpmind.db.platform.mariadb.MariaDBDatabasePlatform;
import org.jumpmind.db.platform.mssql.MsSql2000DatabasePlatform;
import org.jumpmind.db.platform.mssql.MsSql2005DatabasePlatform;
import org.jumpmind.db.platform.mssql.MsSql2008DatabasePlatform;
import org.jumpmind.db.platform.mssql.MsSql2016DatabasePlatform;
import org.jumpmind.db.platform.mysql.MySqlDatabasePlatform;
import org.jumpmind.db.platform.nuodb.NuoDbDatabasePlatform;
import org.jumpmind.db.platform.oracle.Oracle122DatabasePlatform;
import org.jumpmind.db.platform.oracle.OracleDatabasePlatform;
import org.jumpmind.db.platform.postgresql.PostgreSql95DatabasePlatform;
import org.jumpmind.db.platform.postgresql.PostgreSqlDatabasePlatform;
import org.jumpmind.db.platform.raima.RaimaDatabasePlatform;
import org.jumpmind.db.platform.redshift.RedshiftDatabasePlatform;
import org.jumpmind.db.platform.sqlanywhere.SqlAnywhereDatabasePlatform;
import org.jumpmind.db.platform.sqlite.SqliteDatabasePlatform;
import org.jumpmind.db.platform.tibero.TiberoDatabasePlatform;
import org.jumpmind.db.platform.voltdb.VoltDbDatabasePlatform;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger log = LoggerFactory.getLogger(JdbcDatabasePlatformFactory.class);

    static {

        addPlatform(platforms, "H2", H2DatabasePlatform.class);
        addPlatform(platforms, "H21", H2DatabasePlatform.class);
        addPlatform(platforms, "Informix Dynamic Server11", InformixDatabasePlatform.class);
        addPlatform(platforms, "Informix Dynamic Server", InformixDatabasePlatform.class);
        addPlatform(platforms, "Apache Derby", DerbyDatabasePlatform.class);
        addPlatform(platforms, "Firebird", FirebirdDatabasePlatform.class);
        addPlatform(platforms, DatabaseNamesConstants.GREENPLUM, GreenplumPlatform.class);
        addPlatform(platforms, DatabaseNamesConstants.FIREBIRD_DIALECT1, FirebirdDialect1DatabasePlatform.class);
        addPlatform(platforms, "HsqlDb", HsqlDbDatabasePlatform.class);
        addPlatform(platforms, "HSQL Database Engine2", HsqlDb2DatabasePlatform.class);
        addPlatform(platforms, "Interbase", InterbaseDatabasePlatform.class);
        addPlatform(platforms, "MariaDB", MariaDBDatabasePlatform.class);
        addPlatform(platforms, "microsoft sql server8", MsSql2000DatabasePlatform.class);
        addPlatform(platforms, "microsoft sql server9", MsSql2005DatabasePlatform.class);
        addPlatform(platforms, "microsoft sql server10", MsSql2008DatabasePlatform.class);
        addPlatform(platforms, "microsoft sql server11", MsSql2008DatabasePlatform.class);
        addPlatform(platforms, "microsoft sql server12", MsSql2008DatabasePlatform.class);
        addPlatform(platforms, "microsoft sql server13", MsSql2016DatabasePlatform.class);
        addPlatform(platforms, "microsoft sql server", MsSql2016DatabasePlatform.class);
        addPlatform(platforms, "MySQL", MySqlDatabasePlatform.class);
        addPlatform(platforms, "Oracle", OracleDatabasePlatform.class);
        addPlatform(platforms, DatabaseNamesConstants.ORACLE122, Oracle122DatabasePlatform.class);
        addPlatform(platforms, "PostgreSql", PostgreSqlDatabasePlatform.class);
        addPlatform(platforms, DatabaseNamesConstants.POSTGRESQL95, PostgreSql95DatabasePlatform.class);
        addPlatform(platforms, "Adaptive Server Enterprise", AseDatabasePlatform.class);
        addPlatform(platforms, "Adaptive Server Anywhere", SqlAnywhereDatabasePlatform.class);
        addPlatform(platforms, "SQL Anywhere", SqlAnywhereDatabasePlatform.class);
        addPlatform(platforms, "DB2", Db2DatabasePlatform.class);
        addPlatform(platforms, DatabaseNamesConstants.DB2ZOS, Db2zOsDatabasePlatform.class);
        addPlatform(platforms, DatabaseNamesConstants.DB2AS400, Db2As400DatabasePlatform.class);
        addPlatform(platforms, "SQLite", SqliteDatabasePlatform.class);
        addPlatform(platforms, DatabaseNamesConstants.REDSHIFT, RedshiftDatabasePlatform.class);
        addPlatform(platforms, DatabaseNamesConstants.VOLTDB, VoltDbDatabasePlatform.class);
        addPlatform(platforms, DatabaseNamesConstants.NUODB, NuoDbDatabasePlatform.class);
        addPlatform(platforms, DatabaseNamesConstants.TIBERO, TiberoDatabasePlatform.class);
        addPlatform(platforms, DatabaseNamesConstants.RAIMA, RaimaDatabasePlatform.class);
        addPlatform(platforms, "phoenix", HbasePlatform.class);
        addPlatform(platforms, DatabaseNamesConstants.HANA, HanaDatabasePlatform.class);
        addPlatform(platforms, DatabaseNamesConstants.INGRES, IngresDatabasePlatform.class);;

        jdbcSubProtocolToPlatform.put(Db2DatabasePlatform.JDBC_SUBPROTOCOL, Db2DatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(DerbyDatabasePlatform.JDBC_SUBPROTOCOL, DerbyDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(FirebirdDatabasePlatform.JDBC_SUBPROTOCOL,
                FirebirdDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(HsqlDbDatabasePlatform.JDBC_SUBPROTOCOL, HsqlDbDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(InterbaseDatabasePlatform.JDBC_SUBPROTOCOL,
                InterbaseDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(MsSql2000DatabasePlatform.JDBC_SUBPROTOCOL, MsSql2000DatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(MySqlDatabasePlatform.JDBC_SUBPROTOCOL, MySqlDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(OracleDatabasePlatform.JDBC_SUBPROTOCOL_THIN,
                OracleDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(OracleDatabasePlatform.JDBC_SUBPROTOCOL_OCI8,
                OracleDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(OracleDatabasePlatform.JDBC_SUBPROTOCOL_THIN_OLD,
                OracleDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put("polite", OracleDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(PostgreSqlDatabasePlatform.JDBC_SUBPROTOCOL,
                PostgreSqlDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(AseDatabasePlatform.JDBC_SUBPROTOCOL, AseDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(FirebirdDatabasePlatform.JDBC_SUBPROTOCOL,
                FirebirdDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(NuoDbDatabasePlatform.JDBC_SUBPROTOCOL, NuoDbDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(TiberoDatabasePlatform.JDBC_SUBPROTOCOL_THIN,
                TiberoDatabasePlatform.class);
        jdbcSubProtocolToPlatform.put(RaimaDatabasePlatform.JDBC_SUBPROTOCOL, RaimaDatabasePlatform.class);    
    }

    public static synchronized IDatabasePlatform createNewPlatformInstance(DataSource dataSource, SqlTemplateSettings settings, boolean delimitedIdentifierMode, boolean caseSensitive)
            throws DdlException {
            return createNewPlatformInstance(dataSource, settings, delimitedIdentifierMode, caseSensitive, false, false);
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
    public static synchronized IDatabasePlatform createNewPlatformInstance(DataSource dataSource, SqlTemplateSettings settings, boolean delimitedIdentifierMode, boolean caseSensitive, boolean isLoadOnly, boolean isLogBased)
            throws DdlException {

        // connects to the database and uses actual metadata info to get db name
        // and version to determine platform
        String[] nameVersion = determineDatabaseNameVersionSubprotocol(dataSource, isLoadOnly);

        Class<? extends IDatabasePlatform> clazz =  findPlatformClass(nameVersion);

        try {
            Constructor<? extends IDatabasePlatform> construtor = clazz.getConstructor(DataSource.class, SqlTemplateSettings.class);
            IDatabasePlatform platform = construtor.newInstance(dataSource, settings);
            log.info("The IDatabasePlatform being used is " + platform.getClass().getCanonicalName());
            platform.getDdlBuilder().setDelimitedIdentifierModeOn(delimitedIdentifierMode);
            platform.getDdlBuilder().setCaseSensitive(caseSensitive);
            platform.getDdlBuilder().getDatabaseInfo().setLogBased(isLogBased);
            platform.getDdlBuilder().initCteExpression();
            return platform;
        } catch (Exception e) {
            throw new DdlException("Could not create a platform of type " + nameVersion[0], e);
        }
    }


    protected static synchronized Class<? extends IDatabasePlatform> findPlatformClass(
            String[] nameVersion) {
        Class<? extends IDatabasePlatform> platformClass = platforms.get(String.format("%s%s",
                nameVersion[0], nameVersion[1]).toLowerCase());

        if (platformClass == null) {
            platformClass = platforms.get(nameVersion[0].toLowerCase());
        }

        if (platformClass == null) {
            platformClass = jdbcSubProtocolToPlatform.get(nameVersion[2]);
        }

        if (platformClass == null) {
            platformClass = GenericJdbcDatabasePlatform.class;
        } 
        
        return platformClass;
    }

    public static String[] determineDatabaseNameVersionSubprotocol(DataSource dataSource) {
            return determineDatabaseNameVersionSubprotocol(dataSource, false);
    }
    
    public static String[] determineDatabaseNameVersionSubprotocol(DataSource dataSource, boolean isLoadOnly) {
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
                } else if (isRedshiftDatabase(connection)) {
                    nameVersion[0] = DatabaseNamesConstants.REDSHIFT;
                } else if (metaData.getDatabaseMajorVersion() > 9
                        || (metaData.getDatabaseMajorVersion() == 9 && metaData.getDatabaseMinorVersion() >= 5)) {
                    nameVersion[0] = DatabaseNamesConstants.POSTGRESQL95;
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

            if (nameVersion[2].equalsIgnoreCase("as400")) {
                nameVersion[0] = DatabaseNamesConstants.DB2AS400;
            }

            if (nameVersion[0].toLowerCase().indexOf(DatabaseNamesConstants.DB2) != -1 && nameVersion[2].equalsIgnoreCase("db2")) {
                String productVersion = getDatabaseProductVersion(dataSource);
                if (nameVersion[0].toUpperCase().indexOf("Z") != -1
                        || (productVersion != null && productVersion.startsWith("DSN"))) {
                    nameVersion[0] = DatabaseNamesConstants.DB2ZOS;
                } else {
                    nameVersion[0] = DatabaseNamesConstants.DB2;
                }
            }
            if (nameVersion[0].equalsIgnoreCase("AS") && nameVersion[2].equalsIgnoreCase("db2")) {
                nameVersion[0] = DatabaseNamesConstants.DB2AS400;
            }

            if (nameVersion[0].toLowerCase().startsWith(DatabaseNamesConstants.FIREBIRD)) {
                if (isFirebirdDialect1(connection)) {
                    nameVersion[0] = DatabaseNamesConstants.FIREBIRD_DIALECT1;
                }
            }
            
            if (nameVersion[0].equalsIgnoreCase(DatabaseNamesConstants.ORACLE)) {
                int majorVersion = Integer.valueOf(metaData.getDatabaseMajorVersion());
                int minorVersion = Integer.valueOf(metaData.getDatabaseMinorVersion());
                if (majorVersion > 12 || (majorVersion == 12 && minorVersion >= 2)) {
                    if (isOracle122Compatible(connection)) {
                        nameVersion[0] = DatabaseNamesConstants.ORACLE122;
                    }
                }
            }
            
            log.info("Detected database '" + nameVersion[0] + "', version '" + nameVersion[1] + "', protocol '" + nameVersion[2] + "'");

            return nameVersion;
        } catch (Throwable ex) {
                if (!isLoadOnly) {
                throw new SqlException("Error while reading the database metadata: "
                        + ex.getMessage(), ex);
                } else {
                    return nameVersion;
                }
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
        int greenplumCount = 0;
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(GreenplumPlatform.SQL_GET_GREENPLUM_COUNT);
            if (rs.next()) {
                greenplumCount = rs.getInt(1);
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
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
        return greenplumCount > 0;
    }

    private static boolean isRedshiftDatabase(Connection connection) {
        boolean isRedshift = false;
        try {
            DatabaseMetaData dmd = connection.getMetaData();
            dmd.getMaxColumnsInIndex();
            if (dmd.getDriverName().toUpperCase().contains("REDSHIFT")) {
                isRedshift = true;
            }
        } catch (SQLException ex) {
            if (ex.getSQLState().equals("99999")) {
                isRedshift = true;
            }
        }
        
        return isRedshift;
    }

    private static boolean isFirebirdDialect1(Connection connection) {
        boolean isDialect1 = false;
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery("select current_time from rdb$database")) {
            rs.next();
        } catch (SQLException ex) {
            isDialect1 = true;
        }
        if (isDialect1) {
            try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery("select cast(1 as numeric(10,0)) from rdb$database")) {
                rs.next();
            } catch (SQLException e) {
                log.error("The client sql dialect does not match the database, which is not a supported mode.  You must add ?sql_dialect=1 to the end of the JDBC URL.");
            }
        }
        return isDialect1;
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

    private static boolean isOracle122Compatible(Connection connection) {
        boolean isOracle122 = false;
        String compatible = null;
        try (Statement s = connection.createStatement()) {
            try {
                s.executeUpdate("begin dbms_output.enable(); end;");
                s.executeUpdate("declare lver varchar(100); lcomp varchar(100);" +
                        " begin dbms_utility.db_version(lver, lcomp); dbms_output.put_line(lcomp); end;");
        
                String sql = "declare num integer := 1; begin dbms_output.get_lines(?, num); end;";
                try (CallableStatement call = connection.prepareCall(sql)) {
                    call.registerOutParameter(1, Types.ARRAY, "DBMSOUTPUT_LINESARRAY");
                    call.execute();
                    Array array = call.getArray(1);
                    if (array != null) {
                        String[] compatibleArray = (String[]) array.getArray();
                        if (compatibleArray != null && compatibleArray.length > 0) {
                            compatible = compatibleArray[0];
                        }
                        array.free();
                    }
                }
            } finally {
                s.executeUpdate("begin dbms_output.disable(); end;");
            }
        } catch (SQLException e) {
            log.warn("Could not check Oracle compatible parameter", e);
        }
        
        if (compatible != null) {
            String[] valueArr = compatible.split("\\.");
            if (valueArr != null) {
                try {
                    isOracle122 = (valueArr.length > 0 && Integer.parseInt(valueArr[0]) > 12) ||
                            (valueArr.length > 1 && Integer.parseInt(valueArr[0]) == 12 && Integer.parseInt(valueArr[1]) >= 2);
                } catch (Exception e) {
                    log.warn("Could not parse Oracle compatible version " + compatible + " because ", e.getMessage());
                }
            }
        }
        return isOracle122;
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
