package org.jumpmind.symmetric.jdbc.db;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.db.DbException;
import org.jumpmind.symmetric.core.db.IPlatform;
import org.jumpmind.symmetric.jdbc.db.h2.H2Platform;
import org.jumpmind.symmetric.jdbc.db.oracle.OraclePlatform;

public class JdbcPlatformFactory {

    private static Map<String, Class<? extends IPlatform>> platforms = null;

    public static IJdbcPlatform createPlatform(DataSource dataSource) {
        String platformId = lookupPlatformId(dataSource, true);
        AbstractJdbcPlatform platform = createNewPlatformInstance(platformId, dataSource);
        if (platform == null) {
            platformId = lookupPlatformId(dataSource, false);
            platform = createNewPlatformInstance(platformId, dataSource);
        }
        return platform;
    }

    private static AbstractJdbcPlatform createNewPlatformInstance(String databaseName,
            DataSource dataSource) {
        Class<? extends IPlatform> platformClass = getPlatforms().get(databaseName.toLowerCase());

        if (platformClass != null) {
            try {
                Constructor<?> constructor = platformClass.getConstructor(DataSource.class);
                return (AbstractJdbcPlatform) constructor.newInstance(dataSource);
            } catch (Exception ex) {
                throw new DbException("Could not create platform for database " + databaseName, ex);
            }
        } else {
            return null;
        }
    }

    public static String lookupPlatformId(DataSource dataSource, boolean includeVersion)
            throws DbException {
        Connection connection = null;

        try {
            connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            String productString = metaData.getDatabaseProductName();
            if (includeVersion) {
                int majorVersion = metaData.getDatabaseMajorVersion();
                if (majorVersion > 0) {
                    productString += majorVersion;
                }
            }

            return productString;
        } catch (SQLException ex) {
            throw new DbException("Error while reading the database metadata: " + ex.getMessage(),
                    ex);
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

    private static synchronized Map<String, Class<? extends IPlatform>> getPlatforms() {
        if (platforms == null) {
            platforms = registerPlatforms();
        }
        return platforms;
    }

    private static synchronized Map<String, Class<? extends IPlatform>> registerPlatforms() {
        Map<String, Class<? extends IPlatform>> platforms = new HashMap<String, Class<? extends IPlatform>>();
        platforms.put("oracle", OraclePlatform.class);
        platforms.put("h2", H2Platform.class);
        return platforms;
    }

}
