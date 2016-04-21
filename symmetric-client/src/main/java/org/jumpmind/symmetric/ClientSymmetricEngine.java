/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.io.File;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.JdbcDatabasePlatformFactory;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.LogSqlBuilder;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.util.BasicDataSourceFactory;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.security.SecurityServiceFactory;
import org.jumpmind.security.SecurityServiceFactory.SecurityServiceType;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.db.JdbcSymmetricDialectFactory;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.io.stage.StagingManager;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.job.JobManager;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.impl.ClientExtensionService;
import org.jumpmind.symmetric.util.LogSummaryAppenderUtils;
import org.jumpmind.symmetric.util.SnapshotUtil;
import org.jumpmind.symmetric.util.TypedPropertiesFactory;
import org.jumpmind.util.AppUtils;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jndi.JndiObjectFactoryBean;
import org.xml.sax.InputSource;

/**
 * Represents the client portion of a SymmetricDS engine. This class can be used
 * to embed SymmetricDS into another application.
 */
public class ClientSymmetricEngine extends AbstractSymmetricEngine {

    public static final String DEPLOYMENT_TYPE_CLIENT = "client";
    
    public static final String PROPERTIES_FACTORY_CLASS_NAME = "properties.factory.class.name";

    protected File propertiesFile;

    protected Properties properties;

    protected DataSource dataSource;

    protected ApplicationContext springContext;

    /**
     * @param dataSource
     *            If not null, SymmetricDS will use this provided datasource
     *            instead of creating it's own.
     * @param springContext
     *            If not null, SymmetricDS will use this provided Spring context
     *            instead of creating it's own.
     * @param properties
     *            Properties to use for configuration.
     * @param registerEngine
     *            Whether to store a reference to this engine in a local static
     *            map.
     */
    public ClientSymmetricEngine(DataSource dataSource, ApplicationContext springContext,
            Properties properties, boolean registerEngine) {
        super(registerEngine);
        setDeploymentType(DEPLOYMENT_TYPE_CLIENT);
        this.dataSource = dataSource;
        this.springContext = springContext;
        this.properties = properties;
        this.init();        
    }

    public ClientSymmetricEngine(DataSource dataSource, Properties properties,
            boolean registerEngine) {
        super(registerEngine);
        setDeploymentType(DEPLOYMENT_TYPE_CLIENT);
        this.dataSource = dataSource;
        this.properties = properties;
        this.init();
    }

    public ClientSymmetricEngine(File propertiesFile, boolean registerEngine) {
        super(registerEngine);
        setDeploymentType(DEPLOYMENT_TYPE_CLIENT);
        this.propertiesFile = propertiesFile;
        this.init();
    }

    public ClientSymmetricEngine(File propertiesFile) {
        this(propertiesFile, true);
    }
    
    public ClientSymmetricEngine(File propertiesFile, ApplicationContext springContext) {
        super(true);
        setDeploymentType(DEPLOYMENT_TYPE_CLIENT);
        this.propertiesFile = propertiesFile;
        this.springContext = springContext;
        this.init();
                
    }

    public ClientSymmetricEngine(Properties properties, boolean registerEngine) {
        super(registerEngine);
        setDeploymentType(DEPLOYMENT_TYPE_CLIENT);
        this.properties = properties;
        this.init();
    }

    public ClientSymmetricEngine(Properties properties) {
        this(properties, true);
    }

    public ClientSymmetricEngine() {
        this((Properties) null, true);
    }

    public ClientSymmetricEngine(boolean registerEngine) {
        this((Properties) null, registerEngine);
    }

    @Override
    protected SecurityServiceType getSecurityServiceType() {
        return SecurityServiceType.CLIENT;
    }

    @Override
    protected void init() {
        try {
            LogSummaryAppenderUtils.registerLogSummaryAppender();

            super.init();

            this.dataSource = platform.getDataSource();
            
            PropertyPlaceholderConfigurer configurer = new PropertyPlaceholderConfigurer();
            configurer.setProperties(parameterService.getAllParameters());
            
            ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(springContext);
            ctx.addBeanFactoryPostProcessor(configurer);
            
            List<String> extensionLocations = new ArrayList<String>();
            extensionLocations.add("classpath:/symmetric-ext-points.xml");
            if (registerEngine) {
                extensionLocations.add("classpath:/symmetric-jmx.xml");
            } 
                            
            String xml = parameterService.getString(ParameterConstants.EXTENSIONS_XML);
            File file = new File(parameterService.getTempDirectory(), "extension.xml");     
            FileUtils.deleteQuietly(file);
            if (isNotBlank(xml)) {
                try {
                    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                    factory.setValidating(false);
                    factory.setNamespaceAware(true);
                    DocumentBuilder builder = factory.newDocumentBuilder();
                    // the "parse" method also validates XML, will throw an exception if misformatted
                    builder.parse(new InputSource(new StringReader(xml)));               
                    FileUtils.write(file, xml, false); 
                    extensionLocations.add("file:" + file.getAbsolutePath());
                } catch (Exception e) {
                    log.error("Invalid " + ParameterConstants.EXTENSIONS_XML + " parameter.");
                }
            }
            
            try {
                ctx.setConfigLocations(extensionLocations.toArray(new String[extensionLocations
                        .size()]));
                ctx.refresh();

                this.springContext = ctx;

                ((ClientExtensionService) this.extensionService).setSpringContext(springContext);
                this.extensionService.refresh();
            } catch (Exception ex) {
                log.error(
                        "Failed to initialize the extension points.  Please fix the problem and restart the server.",
                        ex);
            }
        } catch (RuntimeException ex) {
            destroy();
            throw ex;
        }
    }
    
    @Override
    public synchronized boolean start() {
        if (this.springContext instanceof AbstractApplicationContext) {
            AbstractApplicationContext ctx = (AbstractApplicationContext) this.springContext;
            try {
                if (!ctx.isActive()) {
                    ctx.start();
                }
            } catch (Exception ex) {
            }
        }

        return super.start();
    }

    @Override
    public synchronized void stop() {
        if (this.springContext instanceof AbstractApplicationContext) {
            AbstractApplicationContext ctx = (AbstractApplicationContext) this.springContext;
            try {
                if (ctx.isActive()) {
                    ctx.stop();
                }
            } catch (Exception ex) {
            }
        }
        super.stop();
    }

    public static BasicDataSource createBasicDataSource(File propsFile) {
        TypedProperties properties = createTypedPropertiesFactory(propsFile, null).reload();
        return BasicDataSourceFactory.create(properties, SecurityServiceFactory.create(SecurityServiceType.CLIENT, properties));
    }

    @Override
    protected ISymmetricDialect createSymmetricDialect() {
        return new JdbcSymmetricDialectFactory(parameterService, platform).create();
    }

    @Override
    protected IDatabasePlatform createDatabasePlatform(TypedProperties properties) {
        IDatabasePlatform platform = createDatabasePlatform(springContext, properties, dataSource, 
                Boolean.parseBoolean(System.getProperty(SystemConstants.SYSPROP_WAIT_FOR_DATABASE, "true")));
        return platform;
    }

    public static IDatabasePlatform createDatabasePlatform(ApplicationContext springContext, TypedProperties properties,
            DataSource dataSource, boolean waitOnAvailableDatabase) {
        if (dataSource == null) {
            String jndiName = properties.getProperty(ParameterConstants.DB_JNDI_NAME);
            if (StringUtils.isNotBlank(jndiName)) {
                try {
                    log.info("Looking up datasource in jndi.  The jndi name is {}", jndiName);
                    JndiObjectFactoryBean jndiFactory = new JndiObjectFactoryBean();
                    jndiFactory.setJndiName(jndiName);
                    jndiFactory.afterPropertiesSet();
                    dataSource = (DataSource)jndiFactory.getObject();

                    if (dataSource == null) {
                        throw new SymmetricException("Could not locate the configured datasource in jndi.  The jndi name is %s", jndiName);
                    }
                } catch (IllegalArgumentException e) {
                    throw new SymmetricException("Could not locate the configured datasource in jndi.  The jndi name is %s", e, jndiName);
                } catch (NamingException e) {
                    throw new SymmetricException("Could not locate the configured datasource in jndi.  The jndi name is %s", e, jndiName);
                }
            }
            
            String springBeanName = properties.getProperty(ParameterConstants.DB_SPRING_BEAN_NAME);
            if (isNotBlank(springBeanName) && springContext != null) {
                log.info("Using datasource from spring.  The spring bean name is {}", springBeanName);
                dataSource = (DataSource)springContext.getBean(springBeanName);
            }
            
            if (dataSource == null) {
                dataSource = BasicDataSourceFactory.create(properties, SecurityServiceFactory.create(SecurityServiceType.CLIENT, properties));
            }
        }
        if (waitOnAvailableDatabase) {
            waitForAvailableDatabase(dataSource);
        }
        boolean delimitedIdentifierMode = properties.is(
                ParameterConstants.DB_DELIMITED_IDENTIFIER_MODE, true);
        boolean caseSensitive = !properties.is(ParameterConstants.DB_METADATA_IGNORE_CASE, true);
        return JdbcDatabasePlatformFactory.createNewPlatformInstance(dataSource,
                createSqlTemplateSettings(properties), delimitedIdentifierMode, caseSensitive);
    }

    protected static SqlTemplateSettings createSqlTemplateSettings(TypedProperties properties) {
        SqlTemplateSettings settings = new SqlTemplateSettings();
        settings.setFetchSize(properties.getInt(ParameterConstants.DB_FETCH_SIZE, 1000));
        settings.setQueryTimeout(properties.getInt(ParameterConstants.DB_QUERY_TIMEOUT_SECS, 300));
        settings.setBatchSize(properties.getInt(ParameterConstants.JDBC_EXECUTE_BATCH_SIZE, 100));
        settings.setOverrideIsolationLevel(properties.getInt(ParameterConstants.JDBC_ISOLATION_LEVEL, -1));
        settings.setReadStringsAsBytes(properties.is(ParameterConstants.JDBC_READ_STRINGS_AS_BYTES, false));
        LogSqlBuilder logSqlBuilder = new LogSqlBuilder();
        logSqlBuilder.setLogSlowSqlThresholdMillis(properties.getInt(ParameterConstants.LOG_SLOW_SQL_THRESHOLD_MILLIS, 20000));
        logSqlBuilder.setLogSqlParametersInline(properties.is(ParameterConstants.LOG_SQL_PARAMETERS_INLINE, true));
        settings.setLogSqlBuilder(logSqlBuilder);
        
        if (settings.getOverrideIsolationLevel() >=0 ) {
            log.info("Overriding isolation level to " + settings.getOverrideIsolationLevel());
        }
        return settings;
    }

    @Override
    protected IExtensionService createExtensionService() {
        return new ClientExtensionService(this, springContext);
    }

    @Override
    protected IJobManager createJobManager() {
        return new JobManager(this);
    }

    @Override
    protected IStagingManager createStagingManager() {
        String directory = parameterService.getTempDirectory();
        return new StagingManager(directory);
    }

    protected static void waitForAvailableDatabase(DataSource dataSource) {
        boolean success = false;
        while (!success) {
            Connection c = null;
            try {
                synchronized (ClientSymmetricEngine.class) {
                    c = dataSource.getConnection();
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

    protected static ITypedPropertiesFactory createTypedPropertiesFactory(File propFile, Properties prop) {
        String propFactoryClassName = System.getProperties().getProperty(PROPERTIES_FACTORY_CLASS_NAME);
        ITypedPropertiesFactory factory = null;
        if (propFactoryClassName != null) {
            try {
                factory = (ITypedPropertiesFactory) Class.forName(propFactoryClassName).newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            factory = new TypedPropertiesFactory();
        }
        factory.init(propFile, prop);
        return factory;
    }

    @Override
    public synchronized void destroy() {
        super.destroy();
        if (springContext instanceof AbstractApplicationContext) {
            try {
            ((AbstractApplicationContext)springContext).destroy();
            } catch (Exception ex) {                
            }
        }
        springContext = null;
        if (dataSource != null && dataSource instanceof BasicDataSource) {
            try {
                ((BasicDataSource)dataSource).close();
            } catch (SQLException e) {
            }
        }
    }

    public List<File> listSnapshots() {
        File snapshotsDir = SnapshotUtil.getSnapshotDirectory(this);
        List<File> files = new ArrayList<File>(FileUtils.listFiles(snapshotsDir, new String[] {"zip"}, false));
        Collections.sort(files, new Comparator<File>() {
            public int compare(File o1, File o2) {
                return -o1.compareTo(o2);
            }
        });
        return files;
    }

    public ApplicationContext getSpringContext() {
        return springContext;
    }

    public File snapshot() {
        return SnapshotUtil.createSnapshot(this);
    }

}
