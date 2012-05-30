package org.jumpmind.symmetric;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.JdbcDatabasePlatformFactory;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.util.ResettableBasicDataSource;
import org.jumpmind.exception.IoException;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.SecurityConstants;
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
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

public class ClientSymmetricEngine extends AbstractSymmetricEngine {

    protected File propertiesFile;

    protected Properties properties;

    protected BasicDataSource dataSource;

    protected ClassPathXmlApplicationContext springContext;

    public ClientSymmetricEngine(File propertiesFile, boolean registerEngine) {
        super(registerEngine);
        setDeploymentType("client");
        this.propertiesFile = propertiesFile;
        this.init();
    }

    public ClientSymmetricEngine(File propertiesFile) {
        this(propertiesFile, true);
    }

    public ClientSymmetricEngine(Properties properties, boolean registerEngine) {
        super(registerEngine);
        setDeploymentType("client");
        this.properties = properties;
        this.init();
    }

    public ClientSymmetricEngine(Properties properties) {
        this(properties, true);
    }

    @Override
    protected void init() {
        super.init();

        PropertyPlaceholderConfigurer configurer = new PropertyPlaceholderConfigurer();
        Properties properties = new Properties();
        properties.setProperty("engine.name", getEngineName());
        configurer.setProperties(properties);

        this.springContext = new ClassPathXmlApplicationContext();
        springContext.addBeanFactoryPostProcessor(configurer);

        if (registerEngine) {
            springContext.setConfigLocations(new String[] { "classpath:/symmetric-ext-points.xml",
                    "classpath:/symmetric-jmx.xml" });
        } else {
            springContext
                    .setConfigLocations(new String[] { "classpath:/symmetric-ext-points.xml" });
        }
        springContext.refresh();

        this.extensionPointManger = createExtensionPointManager(springContext);
        this.extensionPointManger.register();
    }

    @Override
    public synchronized void stop() {
        if (this.springContext != null) {
            this.springContext.stop();
            this.springContext = null;
        }
        super.stop();
    }

    public static BasicDataSource createBasicDataSource(File propsFile) {
        TypedProperties properties = createTypedPropertiesFactory(propsFile, null).reload();
        return createBasicDataSource(properties, createSecurityService(properties));
    }

    public static BasicDataSource createBasicDataSource(TypedProperties properties,
            ISecurityService securityService) {
        ResettableBasicDataSource dataSource = new ResettableBasicDataSource();
        dataSource.setDriverClassName(properties.get(ParameterConstants.DB_POOL_DRIVER, null));
        dataSource.setUrl(properties.get(ParameterConstants.DB_POOL_URL, null));
        String user = properties.get(ParameterConstants.DB_POOL_USER, "");
        if (user != null && user.startsWith(SecurityConstants.PREFIX_ENC)) {
            user = securityService.decrypt(user.substring(SecurityConstants.PREFIX_ENC.length()));
        }
        dataSource.setUsername(user);

        String password = properties.get(ParameterConstants.DB_POOL_PASSWORD, "");
        if (password != null && password.startsWith(SecurityConstants.PREFIX_ENC)) {
            password = securityService.decrypt(password.substring(SecurityConstants.PREFIX_ENC
                    .length()));
        }
        dataSource.setPassword(password);
        dataSource.setInitialSize(properties.getInt(ParameterConstants.DB_POOL_INITIAL_SIZE, 5));
        dataSource.setMaxActive(properties.getInt(ParameterConstants.DB_POOL_MAX_ACTIVE, 20));
        dataSource.setMaxWait(properties.getInt(ParameterConstants.DB_POOL_MAX_WAIT, 5000));
        dataSource.setMinEvictableIdleTimeMillis(properties.getInt(
                ParameterConstants.DB_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS, 60000));
        dataSource.setTimeBetweenEvictionRunsMillis(120000);
        dataSource.setNumTestsPerEvictionRun(10);
        dataSource.setValidationQuery(properties.get(ParameterConstants.DB_POOL_VALIDATION_QUERY,
                null));
        dataSource.setTestOnBorrow(properties.is(ParameterConstants.DB_POOL_TEST_ON_BORROW, true));
        dataSource.setTestOnReturn(properties.is(ParameterConstants.DB_POOL_TEST_ON_RETURN, false));
        dataSource.setTestWhileIdle(properties.is(ParameterConstants.DB_POOL_TEST_WHILE_IDLE, false));

        String connectionProperties = properties.get(
                ParameterConstants.DB_POOL_CONNECTION_PROPERTIES, null);
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
        boolean delimitedIdentifierMode = properties.is(ParameterConstants.DB_DELIMITED_IDENTIFIER_MODE, true);
        return JdbcDatabasePlatformFactory.createNewPlatformInstance(this.dataSource,
                createSqlTemplateSettings(properties), delimitedIdentifierMode);
    }

    protected SqlTemplateSettings createSqlTemplateSettings(TypedProperties properties) {
        SqlTemplateSettings settings = new SqlTemplateSettings();
        settings.setFetchSize(properties.getInt(ParameterConstants.DB_FETCH_SIZE, 1000));
        settings.setQueryTimeout(properties.getInt(ParameterConstants.DB_QUERY_TIMEOUT_SECS, 300));
        settings.setBatchSize(properties.getInt(ParameterConstants.JDBC_EXECUTE_BATCH_SIZE, 100));
        return settings;
    }

    protected void createDataSource(TypedProperties properties) {
        this.dataSource = createBasicDataSource(properties, securityService);
    }

    protected IExtensionPointManager createExtensionPointManager(ApplicationContext springContext) {
        return new ExtensionPointManager(this, springContext);
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
        String directory = parameterService.getTempDirectory();
        return new StagingManager(memoryThresholdInBytes, timeToLiveInMs, directory);
    }

    protected void waitForAvailableDatabase() {
        boolean success = false;
        while (!success) {
            Connection c = null;
            try {
                synchronized (ClientSymmetricEngine.class) {
                    c = this.dataSource.getConnection();
                    success = true;
                }
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
                resources.add(new ClassPathResource("/symmetric-console-default.properties"));
                resources.add(new ClassPathResource("/symmetric-override.properties"));
                if (propertiesFile != null && propertiesFile.exists()) {
                    resources.add(new FileSystemResource(propertiesFile.getAbsolutePath()));
                }
                return resources.toArray(new Resource[resources.size()]);

            }
        };
    }

    protected static class PropertiesFactoryBean extends
            org.springframework.beans.factory.config.PropertiesFactoryBean {

        private static Properties localProperties;

        public PropertiesFactoryBean() {
            this.setLocalOverride(true);
            if (localProperties != null) {
                this.setProperties(localProperties);
            }
        }

        public static void setLocalProperties(Properties localProperties) {
            PropertiesFactoryBean.localProperties = localProperties;
        }

        public static void clearLocalProperties() {
            PropertiesFactoryBean.localProperties = null;
        }
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

}
