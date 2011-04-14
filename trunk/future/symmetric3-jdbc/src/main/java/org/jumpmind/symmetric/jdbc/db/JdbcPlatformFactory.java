package org.jumpmind.symmetric.jdbc.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.db.IPlatform;
import org.jumpmind.symmetric.core.process.sql.DataException;
import org.jumpmind.symmetric.jdbc.db.oracle.OraclePlatform;

public class JdbcPlatformFactory {

    private static Map<String, Class<? extends IPlatform>> platforms = null;

    public static IPlatform createPlatform(DataSource dataSource) {
        String platformId = lookupPlatformId(dataSource, true);
        AbstractJdbcPlatform platform = createNewPlatformInstance(platformId);
        if (platform == null) {
            platformId = lookupPlatformId(dataSource, false);
            platform = createNewPlatformInstance(platformId);
        }
        
        if (platform != null) {
            platform.setDataSource(dataSource);
        }
        return platform;
    }

    /**
     * Creates a new platform for the given (case insensitive) platform
     * identifier or returns null if the database is not recognized.
     * 
     * @param databaseName
     *            The name of the database (case is not important)
     * @return The platform or <code>null</code> if the database is not
     *         supported
     */
    public static AbstractJdbcPlatform createNewPlatformInstance(String databaseName) {
        Class<? extends IPlatform> platformClass = getPlatforms().get(databaseName.toLowerCase());

        try {
            return platformClass != null ? (AbstractJdbcPlatform) platformClass.newInstance() : null;
        } catch (Exception ex) {
            throw new DataException("Could not create platform for database " + databaseName, ex);
        }
    }

    public static String lookupPlatformId(DataSource dataSource, boolean includeVersion)
            throws DataException {
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
            throw new DataException(
                    "Error while reading the database metadata: " + ex.getMessage(), ex);
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
        platforms.put(OraclePlatform.PLATFORMID, OraclePlatform.class);
        return platforms;
    }

}
