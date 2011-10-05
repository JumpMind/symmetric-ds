package org.jumpmind.symmetric.jdbc.db;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.db.DbDialectNotFoundException;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.db.SqlException;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.jdbc.db.h2.H2DbDialect;
import org.jumpmind.symmetric.jdbc.db.oracle.OracleDbDialect;
import org.jumpmind.symmetric.jdbc.db.postgres.PostgresDbDialect;

public class JdbcDbDialectFactory {
    
    final protected Log log = LogFactory.getLog(getClass());

    private static Map<String, Class<? extends IDbDialect>> platforms = null;

    public static AbstractJdbcDbDialect createPlatform(DataSource dataSource, Parameters parameters) {
        String dbDialectId = lookupDialectId(dataSource, true);
        AbstractJdbcDbDialect dbDialect = createNewDialectInstance(dbDialectId, dataSource,
                parameters);
        if (dbDialect == null) {
            dbDialectId = lookupDialectId(dataSource, false);
            dbDialect = createNewDialectInstance(dbDialectId, dataSource, parameters);
        }
        
        if (dbDialect == null) {
            throw new DbDialectNotFoundException(dbDialectId);
        }
        return dbDialect;
    }

    private static AbstractJdbcDbDialect createNewDialectInstance(String databaseName,
            DataSource dataSource, Parameters parameters) {
        Class<? extends IDbDialect> platformClass = getDbDialects().get(databaseName.toLowerCase());

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

    public static String lookupDialectId(DataSource dataSource, boolean includeVersion)
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

    private static synchronized Map<String, Class<? extends IDbDialect>> getDbDialects() {
        if (platforms == null) {
            platforms = registerPlatforms();
        }
        return platforms;
    }

    private static synchronized Map<String, Class<? extends IDbDialect>> registerPlatforms() {
        Map<String, Class<? extends IDbDialect>> platforms = new HashMap<String, Class<? extends IDbDialect>>();
        platforms.put("oracle", OracleDbDialect.class);
        platforms.put("h2", H2DbDialect.class);
        platforms.put("postgresql", PostgresDbDialect.class);
        return platforms;
    }

}
