package org.jumpmind.symmetric.ddlutils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.ddlutils.Platform;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ddlutils.h2.H2Platform;
import org.jumpmind.symmetric.ddlutils.sqlite.SqLitePlatform;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

public class PlatformFactory {

    final static ILog log = LogFactory.getLog(PlatformFactory.class);
    
    static private boolean initialized = false;

    public static Platform getPlatform(DataSource dataSource) {
        initPlatforms();
        String productName = getDbProductName(dataSource);
        int majorVersion = getDbMajorVersion(dataSource);

        // Try to use latest version of platform, then fallback on default
        // platform
        String productString = productName;
        if (majorVersion > 0) {
            productString += majorVersion;
        }
        
        if (productName.startsWith("DB2")) {
            productString = "DB2v8";
        }

        Platform pf = org.apache.ddlutils.PlatformFactory.createNewPlatformInstance(productString);

        if (pf == null) {
            pf = org.apache.ddlutils.PlatformFactory.createNewPlatformInstance(dataSource);
        } else {
            pf.setDataSource(dataSource);
        }
        
        log.info("PlatformInUse", pf.getClass().getName());

        return pf;
    }

    public static String getDbProductName(DataSource dataSource) {
        return new JdbcTemplate(dataSource).execute(new ConnectionCallback<String>() {
            public String doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = c.getMetaData();
                return metaData.getDatabaseProductName();
            }
        });
    }

    public static String getDatabaseProductVersion(DataSource dataSource) {
        return new JdbcTemplate(dataSource).execute(new ConnectionCallback<String>() {
            public String doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = c.getMetaData();
                return metaData.getDatabaseProductVersion();
            }
        });
    }

    public static int getDbMajorVersion(DataSource dataSource) {
        return new JdbcTemplate(dataSource).execute(new ConnectionCallback<Integer>() {
            public Integer doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = c.getMetaData();
                try {
                    return metaData.getDatabaseMajorVersion();
                } catch (UnsupportedOperationException e) {
                    return 0;
                }
            }
        });
    }
    
    public static int getDbMinorVersion(DataSource dataSource) {
        return new JdbcTemplate(dataSource).execute(new ConnectionCallback<Integer>() {
            public Integer doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = c.getMetaData();
                try {
                    return metaData.getDatabaseMinorVersion();
                } catch (UnsupportedOperationException e) {
                    return 0;
                }
            }
        });
    }

    private synchronized static void initPlatforms() {
        if (!initialized) {
            org.apache.ddlutils.PlatformFactory.registerPlatform(SqLitePlatform.DATABASENAME,
                    SqLitePlatform.class);
            /*org.apache.ddlutils.PlatformFactory.registerPlatform(OraclePlatform.DATABASENAME, 
                    OraclePlatform.class);
            */for (String name : H2Platform.DATABASENAMES) {
                org.apache.ddlutils.PlatformFactory.registerPlatform(name,
                        H2Platform.class);                
            }
            initialized = true;
        }
    }

}
