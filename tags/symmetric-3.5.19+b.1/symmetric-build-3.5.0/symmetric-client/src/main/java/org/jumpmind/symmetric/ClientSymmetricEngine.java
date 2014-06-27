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

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.JdbcDatabasePlatformFactory;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.util.BasicDataSourceFactory;
import org.jumpmind.exception.IoException;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.security.SecurityServiceFactory;
import org.jumpmind.security.SecurityServiceFactory.SecurityServiceType;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.db.JdbcSymmetricDialectFactory;
import org.jumpmind.symmetric.ext.ExtensionPointManager;
import org.jumpmind.symmetric.ext.IExtensionPointManager;
import org.jumpmind.symmetric.io.data.DbExport;
import org.jumpmind.symmetric.io.data.DbExport.Format;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.io.stage.StagingManager;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.job.JobManager;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.util.AppUtils;
import org.jumpmind.util.JarBuilder;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.jndi.JndiObjectFactoryBean;

/**
 * Represents the client portion of a SymmetricDS engine. This class can be used
 * to embed SymmetricDS into another application.
 */
public class ClientSymmetricEngine extends AbstractSymmetricEngine {

    public static final String DEPLOYMENT_TYPE_CLIENT = "client";

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
            super.init();

            this.dataSource = platform.getDataSource();

            if (springContext == null) {
                PropertyPlaceholderConfigurer configurer = new PropertyPlaceholderConfigurer();
                configurer.setProperties(parameterService.getAllParameters());

                ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext();
                ctx.addBeanFactoryPostProcessor(configurer);

                if (registerEngine) {
                    ctx.setConfigLocations(new String[] { "classpath:/symmetric-ext-points.xml",
                            "classpath:/symmetric-jmx.xml" });
                } else {
                    ctx.setConfigLocations(new String[] { "classpath:/symmetric-ext-points.xml" });
                }
                ctx.refresh();

                this.springContext = ctx;
            }

            this.extensionPointManger = createExtensionPointManager(springContext);
            this.extensionPointManger.register();
        } catch (RuntimeException ex) {
            destroy();
            throw ex;
        }
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
            } finally {
                this.springContext = null;
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
        IDatabasePlatform platform = createDatabasePlatform(properties, dataSource, Boolean.parseBoolean(System.getProperty(SystemConstants.SYSPROP_WAIT_FOR_DATABASE, "true")));
        return platform;
    }

    public static IDatabasePlatform createDatabasePlatform(TypedProperties properties,
            DataSource dataSource, boolean waitOnAvailableDatabase) {
        if (dataSource == null) {
            String jndiName = properties.getProperty(ParameterConstants.DB_JNDI_NAME);
            if (StringUtils.isBlank(jndiName)) {
                dataSource = BasicDataSourceFactory.create(properties, SecurityServiceFactory.create(SecurityServiceType.CLIENT, properties));
            } else {
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
        }
        if (waitOnAvailableDatabase) {
            waitForAvailableDatabase(dataSource);
        }
        boolean delimitedIdentifierMode = properties.is(
                ParameterConstants.DB_DELIMITED_IDENTIFIER_MODE, true);
        return JdbcDatabasePlatformFactory.createNewPlatformInstance(dataSource,
                createSqlTemplateSettings(properties), delimitedIdentifierMode);
    }

    protected static SqlTemplateSettings createSqlTemplateSettings(TypedProperties properties) {
        SqlTemplateSettings settings = new SqlTemplateSettings();
        settings.setFetchSize(properties.getInt(ParameterConstants.DB_FETCH_SIZE, 1000));
        settings.setQueryTimeout(properties.getInt(ParameterConstants.DB_QUERY_TIMEOUT_SECS, 300));
        settings.setBatchSize(properties.getInt(ParameterConstants.JDBC_EXECUTE_BATCH_SIZE, 100));
        return settings;
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
                /*
                 * System properties always override the properties found in
                 * these files. System properties are merged in the parameter
                 * service.
                 */
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
        if (dataSource != null && dataSource instanceof BasicDataSource) {
            try {
                ((BasicDataSource)dataSource).close();
            } catch (SQLException e) {
            }
        }
    }
    
    public List<File> listSnapshots() {
        File snapshotsDir = getSnapshotDirectory();
        List<File> files = new ArrayList<File>(FileUtils.listFiles(snapshotsDir, new String[] {"zip"}, false));
        Collections.sort(files, new Comparator<File>() {
            public int compare(File o1, File o2) {             
                return -o1.compareTo(o2);
            }
        });
        return files;
    }
    
    protected File getSnapshotDirectory() {
        File snapshotsDir = new File(parameterService.getTempDirectory(), "snapshots");
        snapshotsDir.mkdirs();
        return snapshotsDir;        
    }

    public File snapshot() {
        
        String dirName = getEngineName().replaceAll(" ", "-") + "-" + new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
        
        File snapshotsDir = getSnapshotDirectory();
        
        File logfile = new File(parameterService.getString(ParameterConstants.SERVER_LOG_FILE));
        
        File tmpDir = new File(parameterService.getTempDirectory(), dirName);
        tmpDir.mkdirs();
        
        if (logfile.exists() && logfile.isFile()) {
            try {
                FileUtils.copyFileToDirectory(logfile, tmpDir);
            } catch (IOException e) {
                log.warn("Failed to copy the log file to the snapshot directory", e);
            }
        } else {
            log.warn("Could not find {} to copy to the snapshot directory",
                    logfile.getAbsolutePath());
        }

        List<TriggerHistory> triggerHistories = triggerRouterService.getActiveTriggerHistories();
        List<Table> tables = new ArrayList<Table>();
        for (TriggerHistory triggerHistory : triggerHistories) {
            Table table = platform.getTableFromCache(triggerHistory.getSourceCatalogName(),
                    triggerHistory.getSourceSchemaName(), triggerHistory.getSourceTableName(),
                    false);
            if (table != null) {
                tables.add(table);
            }
        }

        FileWriter fwriter = null;
        try {
            fwriter = new FileWriter(new File(tmpDir, "config-export.csv"));
            dataExtractorService.extractConfigurationStandalone(nodeService.findIdentity(),
                    fwriter, TableConstants.SYM_NODE, TableConstants.SYM_NODE_SECURITY,
                    TableConstants.SYM_NODE_IDENTITY, TableConstants.SYM_NODE_HOST,
                    TableConstants.SYM_NODE_CHANNEL_CTL);
        } catch (IOException e) {
            log.warn("Failed to export symmetric configuration", e);
        } finally {
            IOUtils.closeQuietly(fwriter);
        }

        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "table-definitions.xml"));
            DbExport export = new DbExport(platform);
            export.setFormat(Format.XML);
            export.setNoData(true);
            export.exportTables(fos, tables.toArray(new Table[tables.size()]));
        } catch (IOException e) {
            log.warn("Failed to export table definitions", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "runtime-data.xml"));
            DbExport export = new DbExport(platform);
            export.setFormat(Format.XML);
            export.setNoCreateInfo(true);
            export.exportTables(
                    fos,
                    new String[] {
                            TableConstants.getTableName(getTablePrefix(), TableConstants.SYM_NODE),
                            TableConstants.getTableName(getTablePrefix(),
                                    TableConstants.SYM_NODE_SECURITY),
                            TableConstants.getTableName(getTablePrefix(),
                                    TableConstants.SYM_NODE_HOST),
                            TableConstants.getTableName(getTablePrefix(),
                                    TableConstants.SYM_TRIGGER_HIST),
                            TableConstants.getTableName(getTablePrefix(), TableConstants.SYM_LOCK),
                            TableConstants.getTableName(getTablePrefix(), TableConstants.SYM_NODE_COMMUNICATION)});
        } catch (IOException e) {
            log.warn("Failed to export table definitions", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        final int THREAD_INDENT_SPACE = 50;
        fwriter = null;
        try {
            fwriter = new FileWriter(new File(tmpDir, "threads.txt"));
            ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
            long[] threadIds = threadBean.getAllThreadIds();
            for (long l : threadIds) {
                ThreadInfo info = threadBean.getThreadInfo(l, 100);
                if (info != null) {
                    String threadName = info.getThreadName();
                    fwriter.append(StringUtils.rightPad(threadName, THREAD_INDENT_SPACE));
                    StackTraceElement[] trace = info.getStackTrace();
                    boolean first = true;
                    for (StackTraceElement stackTraceElement : trace) {
                        if (!first) {
                            fwriter.append(StringUtils.rightPad("", THREAD_INDENT_SPACE));
                        } else {
                            first = false;
                        }
                        fwriter.append(stackTraceElement.getClassName());
                        fwriter.append(".");
                        fwriter.append(stackTraceElement.getMethodName());
                        fwriter.append("()");
                        int lineNumber = stackTraceElement.getLineNumber();
                        if (lineNumber > 0) {
                            fwriter.append(": ");
                            fwriter.append(Integer.toString(stackTraceElement.getLineNumber()));
                        }
                        fwriter.append("\n");
                    }
                    fwriter.append("\n");
                }
            }
        } catch (IOException e) {
            log.warn("Failed to export thread information", e);
        } finally {
            IOUtils.closeQuietly(fwriter);
        }

        fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "parameters.properties"));
            Properties effectiveParameters = parameterService.getAllParameters();
            effectiveParameters.store(fos, "parameters.properties");
        } catch (IOException e) {
            log.warn("Failed to export thread information", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "runtime-stats.properties"));
            Properties runtimeProperties = new Properties();
            runtimeProperties.setProperty("unrouted.data.count",
                    Long.toString(routerService.getUnroutedDataCount()));
            runtimeProperties.setProperty("outgoing.errors.count",
                    Long.toString(outgoingBatchService.countOutgoingBatchesInError()));
            runtimeProperties.setProperty("outgoing.tosend.count",
                    Long.toString(outgoingBatchService.countOutgoingBatchesUnsent()));
            runtimeProperties.setProperty("incoming.errors.count",
                    Long.toString(incomingBatchService.countIncomingBatchesInError()));
            runtimeProperties.store(fos, "runtime-stats.properties");
        } catch (IOException e) {
            log.warn("Failed to export thread information", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "system.properties"));
            System.getProperties().store(fos, "system.properties");
        } catch (IOException e) {
            log.warn("Failed to export thread information", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        try {
            File jarFile = new File(snapshotsDir, tmpDir.getName()
                    + ".zip");
            JarBuilder builder = new JarBuilder(tmpDir, jarFile, new File[] { tmpDir }, Version.version());
            builder.build();
            FileUtils.deleteDirectory(tmpDir);
            return jarFile;
        } catch (IOException e) {
            throw new IoException("Failed to package snapshot files into archive", e);
        }

    }

}
