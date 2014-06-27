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

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.ddl.platform.axion.AxionPlatform;
import org.jumpmind.symmetric.ddl.platform.cloudscape.CloudscapePlatform;
import org.jumpmind.symmetric.ddl.platform.db2.Db2v8Platform;
import org.jumpmind.symmetric.ddl.platform.derby.DerbyPlatform;
import org.jumpmind.symmetric.ddl.platform.firebird.FirebirdPlatform;
import org.jumpmind.symmetric.ddl.platform.h2.H2Platform;
import org.jumpmind.symmetric.ddl.platform.hsqldb.HsqlDbPlatform;
import org.jumpmind.symmetric.ddl.platform.hsqldb2.HsqlDb2Platform;
import org.jumpmind.symmetric.ddl.platform.informix.InformixPlatform;
import org.jumpmind.symmetric.ddl.platform.interbase.InterbasePlatform;
import org.jumpmind.symmetric.ddl.platform.maxdb.MaxDbPlatform;
import org.jumpmind.symmetric.ddl.platform.mckoi.MckoiPlatform;
import org.jumpmind.symmetric.ddl.platform.mssql.MSSqlPlatform;
import org.jumpmind.symmetric.ddl.platform.mysql.MySql50Platform;
import org.jumpmind.symmetric.ddl.platform.mysql.MySqlPlatform;
import org.jumpmind.symmetric.ddl.platform.oracle.Oracle10Platform;
import org.jumpmind.symmetric.ddl.platform.oracle.Oracle8Platform;
import org.jumpmind.symmetric.ddl.platform.oracle.Oracle9Platform;
import org.jumpmind.symmetric.ddl.platform.postgresql.PostgreSqlPlatform;
import org.jumpmind.symmetric.ddl.platform.sapdb.SapDbPlatform;
import org.jumpmind.symmetric.ddl.platform.sqlite.SqLitePlatform;
import org.jumpmind.symmetric.ddl.platform.sybase.SybaseASE15Platform;
import org.jumpmind.symmetric.ddl.platform.sybase.SybasePlatform;

/**
 * A factory of {@link org.jumpmind.symmetric.ddl.Platform} instances based on a case
 * insensitive database name. Note that this is a convenience class as the platforms
 * can also simply be created via their constructors.
 * 
 * @version $Revision: 209952 $
 */
public class PlatformFactory
{
    /** The log for this platform. */
    private static final Log _log = LogFactory.getLog(PlatformFactory.class);
    
    /** The database name -> platform map. */
    private static Map<String,Class<? extends Platform>> _platforms = null;

    /**
     * Returns the platform map.
     * 
     * @return The platform list
     */
    private static synchronized Map<String,Class<? extends Platform>> getPlatforms()
    {
        if (_platforms == null)
        {
            // lazy initialization
            _platforms = new HashMap<String,Class<? extends Platform>>();
            registerPlatforms();
        }
        return _platforms;
    }
    
    /**
     * Creates a new platform for the given (case insensitive) database name
     * or returns null if the database is not recognized.
     * 
     * @param databaseName The name of the database (case is not important)
     * @return The platform or <code>null</code> if the database is not supported
     */
    public static synchronized Platform createNewPlatformInstance(String databaseName) throws DdlUtilsException
    {
        Class<? extends Platform> platformClass = getPlatforms().get(databaseName.toLowerCase());

        try
        {
            return platformClass != null ? (Platform)platformClass.newInstance() : null;
        }
        catch (Exception ex)
        {
            throw new DdlUtilsException("Could not create platform for database "+databaseName, ex);
        }
    }

    /**
     * Creates a new platform for the specified database. This is a shortcut method that uses
     * {@link PlatformUtils#determineDatabaseType(String, String)} to determine the parameter
     * for {@link #createNewPlatformInstance(String)}. Note that no database connection is
     * established when using this method.
     * 
     * @param jdbcDriver        The jdbc driver
     * @param jdbcConnectionUrl The connection url
     * @return The platform or <code>null</code> if the database is not supported
     */
    public static synchronized Platform createNewPlatformInstance(String jdbcDriver, String jdbcConnectionUrl) throws DdlUtilsException
    {
        return createNewPlatformInstance(PlatformUtils.determineDatabaseType(jdbcDriver, jdbcConnectionUrl));
    }

    /**
     * Creates a new platform for the specified database. This is a shortcut method that uses
     * {@link PlatformUtils#determineDatabaseType(DataSource)} to determine the parameter
     * for {@link #createNewPlatformInstance(String)}. Note that this method sets the data source
     * at the returned platform instance (method {@link Platform#setDataSource(DataSource)}).
     * 
     * @param dataSource The data source for the database
     * @return The platform or <code>null</code> if the database is not supported
     */
    public static synchronized Platform createNewPlatformInstance(DataSource dataSource) throws DdlUtilsException
    {
        String nameVersion = PlatformUtils.determineDatabaseNameVersion(dataSource);
        Platform platform = createNewPlatformInstance(nameVersion);
        if (platform == null) {
            String dbType = PlatformUtils.determineDatabaseType(dataSource);
            _log.warn("The name/version pair returned for the database, "
                    + nameVersion
                    + ",  was not mapped to a known database platform.  Defaulting to using just the database type of "
                    + dbType);
            platform = createNewPlatformInstance(dbType);
        }
        platform.setDataSource(dataSource);
        return platform;
    }

    /**
     * Returns a list of all supported platforms.
     * 
     * @return The names of the currently registered platforms
     */
    public static synchronized String[] getSupportedPlatforms()
    {
        return (String[])getPlatforms().keySet().toArray(new String[0]);
    }

    /**
     * Determines whether the indicated platform is supported.
     * 
     * @param platformName The name of the platform
     * @return <code>true</code> if the platform is supported
     */
    public static boolean isPlatformSupported(String platformName)
    {
        return getPlatforms().containsKey(platformName.toLowerCase());
    }

    /**
     * Registers a new platform.
     * 
     * @param platformName  The platform name
     * @param platformClass The platform class which must implement the {@link Platform} interface
     */
    public static synchronized void registerPlatform(String platformName, Class<? extends Platform> platformClass)
    {
        addPlatform(getPlatforms(), platformName, platformClass);
    }

    /**
     * Registers the known platforms.
     */
    private static void registerPlatforms()
    {
        for (String name : H2Platform.DATABASENAMES) {
            addPlatform(_platforms, name,
                    H2Platform.class);                
        }
        addPlatform(_platforms, SqLitePlatform.DATABASENAME,      SqLitePlatform.class);
        addPlatform(_platforms, InformixPlatform.DATABASENAME,    InformixPlatform.class);
        addPlatform(_platforms, AxionPlatform.DATABASENAME,       AxionPlatform.class);
        addPlatform(_platforms, CloudscapePlatform.DATABASENAME,  CloudscapePlatform.class);
        addPlatform(_platforms, Db2v8Platform.DATABASENAMES,       Db2v8Platform.class);
        addPlatform(_platforms, DerbyPlatform.DATABASENAME,       DerbyPlatform.class);
        addPlatform(_platforms, FirebirdPlatform.DATABASENAME,    FirebirdPlatform.class);
        addPlatform(_platforms, HsqlDbPlatform.DATABASENAME,      HsqlDbPlatform.class);
        addPlatform(_platforms, HsqlDb2Platform.DATABASENAME,     HsqlDb2Platform.class);
        addPlatform(_platforms, InterbasePlatform.DATABASENAME,   InterbasePlatform.class);
        addPlatform(_platforms, MaxDbPlatform.DATABASENAME,       MaxDbPlatform.class);
        addPlatform(_platforms, MckoiPlatform.DATABASENAME,       MckoiPlatform.class);
        addPlatform(_platforms, MSSqlPlatform.DATABASENAME,       MSSqlPlatform.class);
        addPlatform(_platforms, MySqlPlatform.DATABASENAME,       MySqlPlatform.class);
        addPlatform(_platforms, MySql50Platform.DATABASENAME,     MySql50Platform.class);
        addPlatform(_platforms, Oracle8Platform.DATABASENAME,     Oracle8Platform.class);
        addPlatform(_platforms, Oracle9Platform.DATABASENAME,     Oracle9Platform.class);
        addPlatform(_platforms, Oracle10Platform.DATABASENAME10,    Oracle10Platform.class);
        addPlatform(_platforms, Oracle10Platform.DATABASENAME11,    Oracle10Platform.class);
        addPlatform(_platforms, PostgreSqlPlatform.DATABASENAME,  PostgreSqlPlatform.class);
        addPlatform(_platforms, SapDbPlatform.DATABASENAME,       SapDbPlatform.class);
        addPlatform(_platforms, SybasePlatform.DATABASENAME,      SybasePlatform.class);
        addPlatform(_platforms, SybaseASE15Platform.DATABASENAME, SybaseASE15Platform.class);
    }

    /**
     * Registers a new platform.
     * 
     * @param platformMap   The map to add the platform info to 
     * @param platformName  The platform name
     * @param platformClass The platform class which must implement the {@link Platform} interface
     */
    private static synchronized void addPlatform(Map<String,Class<? extends Platform>> platformMap, String[] platformNames, Class<? extends Platform> platformClass)
    {
        for (String platformName : platformNames) {
            addPlatform(platformMap, platformName, platformClass);
        }
    }
    
    private static synchronized void addPlatform(Map<String,Class<? extends Platform>> platformMap, String platformName, Class<? extends Platform> platformClass)
    {
        if (!Platform.class.isAssignableFrom(platformClass))
        {
            throw new IllegalArgumentException("Cannot register class "+platformClass.getName()+" because it does not implement the "+Platform.class.getName()+" interface");
        }
        platformMap.put(platformName.toLowerCase(), platformClass);        

    }
}
