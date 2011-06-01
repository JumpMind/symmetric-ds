package org.jumpmind.symmetric.jdbc.db;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.db.SqlException;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.jdbc.db.h2.H2DbDialect;
import org.jumpmind.symmetric.jdbc.db.oracle.OracleDbDialect;

public class JdbcDbDialectFactory {

    private static Map<String, Class<? extends IDbDialect>> platforms = null;

    public static AbstractJdbcDbDialect createPlatform(DataSource dataSource, Parameters parameters) {
        String platformId = lookupPlatformId(dataSource, true);
        AbstractJdbcDbDialect platform = createNewPlatformInstance(platformId, dataSource,
                parameters);
        if (platform == null) {
            platformId = lookupPlatformId(dataSource, false);
            platform = createNewPlatformInstance(platformId, dataSource, parameters);
        }
        return platform;
    }

    private static AbstractJdbcDbDialect createNewPlatformInstance(String databaseName,
            DataSource dataSource, Parameters parameters) {
        Class<? extends IDbDialect> platformClass = getPlatforms().get(databaseName.toLowerCase());

        if (platformClass != null) {
            try {
                Constructor<?> constructor = platformClass.getConstructor(DataSource.class,
                        Parameters.class);
                return (AbstractJdbcDbDialect) constructor.newInstance(dataSource, parameters);
            } catch (Exception ex) {
                throw new SqlException("Could not create platform for database " + databaseName, ex);
            }
        } else {
            return null;
        }
    }

    public static String lookupPlatformId(DataSource dataSource, boolean includeVersion)
            throws SqlException {
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
            throw new SqlException("Error while reading the database metadata: " + ex.getMessage(),
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

    private static synchronized Map<String, Class<? extends IDbDialect>> getPlatforms() {
        if (platforms == null) {
            platforms = registerPlatforms();
        }
        return platforms;
    }

    private static synchronized Map<String, Class<? extends IDbDialect>> registerPlatforms() {
        Map<String, Class<? extends IDbDialect>> platforms = new HashMap<String, Class<? extends IDbDialect>>();
        platforms.put("oracle", OracleDbDialect.class);
        platforms.put("h2", H2DbDialect.class);
        return platforms;
    }

}
