package org.jumpmind.symmetric;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.JdbcDatabasePlatformFactory;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.exception.IoException;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.SecurityConstants;
import org.jumpmind.symmetric.config.PropertiesFactoryBean;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.db.JdbcSymmetricDialectFactory;
import org.jumpmind.symmetric.ext.ExtensionPointManager;
import org.jumpmind.symmetric.ext.IExtensionPointManager;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.io.stage.StagingManager;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.job.JobManager;
import org.jumpmind.symmetric.service.ISecurityService;
import org.jumpmind.symmetric.util.AppUtils;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

public class ClientSymmetricEngine extends AbstractSymmetricEngine {

    protected File propertiesFile;

    protected Properties properties;

    protected BasicDataSource dataSource;

    public ClientSymmetricEngine(File propertiesFile) {
        this.propertiesFile = propertiesFile;
        this.init();
    }

    public ClientSymmetricEngine(Properties properties) {
        this.properties = properties;
        this.init();
    }

    public DataSource getDataSource() {
        return ((JdbcSqlTemplate) platform.getSqlTemplate()).getDataSource();
    }

    public static BasicDataSource createBasicDataSource(File propsFile) {
        return createBasicDataSource(createTypedPropertiesFactory(propsFile, null).reload(),
                createSecurityService());
    }

    public static BasicDataSource createBasicDataSource(TypedProperties properties,
            ISecurityService securityService) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(properties.get(ParameterConstants.DBPOOL_DRIVER, null));
        dataSource.setUrl(properties.get(ParameterConstants.DBPOOL_URL, null));
        String user = properties.get(ParameterConstants.DBPOOL_USER, "");
        if (user != null && user.startsWith(SecurityConstants.PREFIX_ENC)) {
            user = securityService.decrypt(user.substring(SecurityConstants.PREFIX_ENC.length()));
        }
        dataSource.setUsername(user);

        String password = properties.get(ParameterConstants.DBPOOL_PASSWORD, "");
        if (password != null && password.startsWith(SecurityConstants.PREFIX_ENC)) {
            password = securityService.decrypt(password.substring(SecurityConstants.PREFIX_ENC
                    .length()));
        }
        dataSource.setPassword(password);
        dataSource.setInitialSize(properties.getInt(ParameterConstants.DBPOOL_INITIAL_SIZE, 5));
        dataSource.setMaxActive(properties.getInt(ParameterConstants.DBPOOL_MAX_ACTIVE, 20));
        dataSource.setMaxWait(properties.getInt(ParameterConstants.DBPOOL_MAX_WAIT, 5000));
        dataSource.setMinEvictableIdleTimeMillis(properties.getInt(
                ParameterConstants.DBPOOL_MIN_EVICTABLE_IDLE_TIME_MILLIS, 60000));
        dataSource.setTimeBetweenEvictionRunsMillis(120000);
        dataSource.setNumTestsPerEvictionRun(10);
        dataSource.setValidationQuery(properties.get(ParameterConstants.DBPOOL_VALIDATION_QUERY,
                null));

        String connectionProperties = properties.get(
                ParameterConstants.DBPOOL_CONNECTION_PROPERTIES, null);
        if (StringUtils.isNotBlank(connectionProperties)) {
            String[] tokens = connectionProperties.split(";");
            for (String property : tokens) {
                String[] keyValue = property.split("=");
                if (keyValue != null && keyValue.length > 1) {
                    LoggerFactory.getLogger(ClientSymmetricEngine.class).info(
                            "Setting database connection property %s=%s", keyValue[0], keyValue[1]);
                    dataSource.addConnectionProperty(keyValue[0], keyValue[1]);
                }
            }
        }
        return dataSource;

    }

    @Override
    protected ISymmetricDialect createSymmetricDialect() {
        return new JdbcSymmetricDialectFactory(parameterService, platform).create();
    }

    @Override
    protected IDatabasePlatform createDatabasePlatform(TypedProperties properties) {
        createDataSource(properties);
        waitForAvailableDatabase();
        return JdbcDatabasePlatformFactory.createNewPlatformInstance(this.dataSource,
                createDatabasePlatformSettings(properties));
    }

    protected DatabasePlatformSettings createDatabasePlatformSettings(TypedProperties properties) {
        DatabasePlatformSettings settings = new DatabasePlatformSettings();
        settings.setFetchSize(properties.getInt(ParameterConstants.DB_FETCH_SIZE, 1000));
        settings.setQueryTimeout(properties.getInt(ParameterConstants.DB_QUERY_TIMEOUT_SECS, 300));
        settings.setBatchSize(properties.getInt(ParameterConstants.JDBC_EXECUTE_BATCH_SIZE, 100));
        return settings;
    }

    protected void createDataSource(TypedProperties properties) {
        this.dataSource = createBasicDataSource(properties, securityService);
    }

    @Override
    protected IExtensionPointManager createExtensionPointManager() {
        return new ExtensionPointManager(this);
    }

    @Override
    protected IJobManager createJobManager() {
        return new JobManager(this);
    }

    @Override
    protected IStagingManager createStagingManager() {
        long memoryThresholdInBytes = parameterService
                .getLong(ParameterConstants.STREAM_TO_FILE_THRESHOLD);
        long timeToLiveInMs = parameterService
                .getLong(ParameterConstants.STREAM_TO_FILE_TIME_TO_LIVE_MS);
        String directory = parameterService.getString("java.io.tmpdir",
                System.getProperty("java.io.tmpdir"));
        return new StagingManager(memoryThresholdInBytes, timeToLiveInMs, directory);
    }

    protected void waitForAvailableDatabase() {
        boolean success = false;
        while (!success) {
            Connection c = null;
            try {
                c = this.dataSource.getConnection();
                success = true;
            } catch (Exception ex) {
                log.error(
                        "Could not get a connection to the database: {}.  Waiting for 10 seconds before trying to connect to the database again.",
                        ex.getMessage());
                AppUtils.sleep(10000);
            } finally {
                JdbcSqlTemplate.close(c);
            }
        }
    }

    @Override
    protected ITypedPropertiesFactory createTypedPropertiesFactory() {
        return createTypedPropertiesFactory(propertiesFile, properties);
    }

    protected static ITypedPropertiesFactory createTypedPropertiesFactory(
            final File propertiesFile, final Properties properties) {
        return new ITypedPropertiesFactory() {
            public TypedProperties reload() {
                PropertiesFactoryBean factoryBean = new PropertiesFactoryBean();
                factoryBean.setIgnoreResourceNotFound(true);
                factoryBean.setLocalOverride(true);
                factoryBean.setSingleton(false);
                factoryBean.setProperties(properties);
                factoryBean.setLocations(buildLocations(propertiesFile));
                try {
                    return new TypedProperties(factoryBean.getObject());
                } catch (IOException e) {
                    throw new IoException(e);
                }
            }

            protected Resource[] buildLocations(File propertiesFile) {
                List<Resource> resources = new ArrayList<Resource>();
                resources.add(new ClassPathResource("/symmetric-default.properties"));
                resources.add(new ClassPathResource("/symmetric-console-default.properties"));
                resources.add(new FileSystemResource("../conf/symmetric.properties"));
                resources.add(new ClassPathResource("/symmetric.properties"));
                resources.add(new ClassPathResource("/symmetric-override.properties"));
                if (propertiesFile != null && propertiesFile.exists()) {
                    resources.add(new FileSystemResource(propertiesFile.getAbsolutePath()));
                }
                return resources.toArray(new Resource[resources.size()]);

            }
        };
    }

    @Override
    public synchronized void destroy() {
        super.destroy();
        if (dataSource != null) {
            try {
                dataSource.close();
            } catch (SQLException e) {
            }
        }
    }

    @Override
    protected void registerWithJMX() {
        // TODO
    }

}
