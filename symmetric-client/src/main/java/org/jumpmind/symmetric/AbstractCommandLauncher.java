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

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.Provider;
import java.security.Security;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.security.SecurityConstants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.ServerConstants;
import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.symmetric.transport.TransportManagerFactory;
import org.jumpmind.symmetric.util.LogSummaryAppenderUtils;
import org.jumpmind.util.AppUtils;
import org.jumpmind.util.SymRollingFileAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCommandLauncher {

    private static final Logger log;
    
    public static final String DEFAULT_SERVER_PROPERTIES;

    protected static final String HELP = "help";

    protected static final String OPTION_PROPERTIES_FILE = "properties";

    protected static final String OPTION_ENGINE = "engine";

    protected static final String OPTION_VERBOSE_CONSOLE = "verbose";

    protected static final String OPTION_DEBUG = "debug";

    protected static final String OPTION_NO_LOG_CONSOLE = "no-log-console";

    protected static final String OPTION_NO_LOG_FILE = "no-log-file";

    protected static final String OPTION_KEYSTORE_PASSWORD = "storepass";

    protected static final String OPTION_KEYSTORE_TYPE = "storetype";

    protected static final String OPTION_JCE_PROVIDER = "providerClass";

    protected static final String COMMON_MESSAGE_KEY_PREFIX = "Common.Option.";

    protected String app;

    protected String argSyntax;

    protected String messageKeyPrefix;

    protected File propertiesFile;

    protected ISymmetricEngine engine;

    protected IDatabasePlatform platform;
    
    private static boolean serverPropertiesInitialized = false;

    static {
        String symHome = AppUtils.getSymHome();
        if (isBlank(System.getProperty("h2.baseDir.disable")) && isBlank(System.getProperty("h2.baseDir"))) {
           System.setProperty("h2.baseDir", symHome + "/db/h2");
        }
        if (isBlank(System.getProperty("derby.baseDir"))) {
            System.setProperty("derby.baseDir", symHome + "/db/derby");
        }
        DEFAULT_SERVER_PROPERTIES = System.getProperty(SystemConstants.SYSPROP_SERVER_PROPERTIES_PATH, symHome + "/conf/symmetric-server.properties");
        log = LoggerFactory.getLogger(AbstractCommandLauncher.class);
        initFromServerProperties();
    }

    public AbstractCommandLauncher(String app, String argSyntax, String messageKeyPrefix) {
        this.app = app;
        this.argSyntax = argSyntax;
        this.messageKeyPrefix = messageKeyPrefix;
        TypedProperties serverProperties = new TypedProperties(System.getProperties());
        boolean allowSelfSignedCerts = serverProperties.is(ServerConstants.HTTPS_ALLOW_SELF_SIGNED_CERTS, true);
        String allowServerNames = serverProperties.get(ServerConstants.HTTPS_VERIFIED_SERVERS, "all");
        boolean https2Enabled = serverProperties.is(ServerConstants.HTTPS2_ENABLE, true);
        TransportManagerFactory.initHttps(allowServerNames, allowSelfSignedCerts, https2Enabled);
    }
    
    protected static void initFromServerProperties() {
        if (!serverPropertiesInitialized) {
            File serverPropertiesFile = new File(DEFAULT_SERVER_PROPERTIES);
            TypedProperties serverProperties = new TypedProperties();

            if (serverPropertiesFile.exists() && serverPropertiesFile.isFile()) {
                try(FileInputStream fis = new FileInputStream(serverPropertiesFile)) {
                    serverProperties.load(fis);

                    /* System properties always override */
                    serverProperties.merge(System.getProperties());

                    /*
                     * Put server properties back into System properties so they
                     * are available to the parameter service
                     */
                    System.getProperties().putAll(serverProperties);

                } catch (IOException ex) {
                    log.error("Failed to load " + DEFAULT_SERVER_PROPERTIES, ex);
                }
            } else if (!serverPropertiesFile.exists()) {
                log.debug("Failed to load " + DEFAULT_SERVER_PROPERTIES + ". File does not exist.");
            } else if (!serverPropertiesFile.isFile()) {
                log.debug("Failed to load " + DEFAULT_SERVER_PROPERTIES + ". Object is not a file.");
            }
            serverPropertiesInitialized = true;
        }
    }

    abstract protected boolean printHelpIfNoOptionsAreProvided();
    
    abstract protected boolean requiresPropertiesFile(CommandLine line);

    public void execute(String args[]) {
        DefaultParser parser = new DefaultParser();
        Options options = new Options();
        buildOptions(options);
        try {
            CommandLine line = parser.parse(options, args);

            if (line.hasOption(HELP) || (line.getArgList().contains(HELP))
                    || ((args == null || args.length == 0) 
                            && line.getOptions().length == 0 && printHelpIfNoOptionsAreProvided())) {
                printHelp(line, options);
                System.exit(2);
            }

            configureLogging(line);
            configurePropertiesFile(line);

            if (line.getOptions() != null) {
                for (Option option : line.getOptions()) {
                    log.info("Option: name={}, value={}", new Object[] { 
                            option.getLongOpt() != null ? option.getLongOpt() : option.getOpt(),
                            ArrayUtils.toString(option.getValues()) });
                }
            }

            executeWithOptions(line);

        } catch (ParseException e) {
            System.err.println(e.getMessage());
            printUsage(options);
            System.exit(4);
        } catch (Exception e) {
            System.err
                    .println("-------------------------------------------------------------------------------");
            System.err.println("An exception occurred.  Please see the following for details:");
            System.err
                    .println("-------------------------------------------------------------------------------");

            ExceptionUtils.printRootCauseStackTrace(e, System.err);
            System.err
                    .println("-------------------------------------------------------------------------------");
            System.exit(1);
        }
    }

    protected void printHelp(CommandLine cmd, Options options) {
        new HelpFormatter().printHelp(app + " " + argSyntax, options);
    }

    protected void printUsage(Options options) {
        PrintWriter writer = new PrintWriter(System.out);
        new HelpFormatter().printUsage(writer, 80, app, options);
        writer.write("For more options, use " + app + " --" + HELP + "\n");
        writer.flush();
    }

    protected void configureLogging(CommandLine line) throws MalformedURLException {
        URL log4jUrl = new URL(System.getProperty("log4j2.configurationFile", "file:" + AppUtils.getSymHome() + "/conf/log4j2-blank.xml"));
        File log4jFile = new File(new File(log4jUrl.getFile()).getParent(), "log4j2.xml");

        if (line.hasOption(OPTION_DEBUG)) {
            log4jFile = new File(log4jFile.getParent(), "log4j2-debug.xml");
        }

        if (log4jFile.exists()) {
            Configurator.initialize("SYM", log4jFile.getAbsolutePath());
        }
        
        if (line.hasOption(OPTION_VERBOSE_CONSOLE)) {
            LogSummaryAppenderUtils.removeAppender("CONSOLE");
            PatternLayout layout = PatternLayout.newBuilder().withPattern("%d %-5p [%c{2}] [%t] %m%ex%n").build();
            Appender appender = ConsoleAppender.newBuilder().setName("CONSOLE").setTarget(ConsoleAppender.Target.SYSTEM_ERR)
                    .setLayout(layout).build();
            LogSummaryAppenderUtils.addAppender(appender);
        }
        if (line.hasOption(OPTION_NO_LOG_CONSOLE)) {
            LogSummaryAppenderUtils.removeAppender("CONSOLE");
        }

        if (line.hasOption(OPTION_NO_LOG_FILE)) {
            LogSummaryAppenderUtils.removeAppender("ROLLING");
        } else {
            Appender appender = LogSummaryAppenderUtils.getAppender("ROLLING");
            if (appender instanceof SymRollingFileAppender) {
                SymRollingFileAppender fa = (SymRollingFileAppender) appender;
                String fileName = fa.getFileName();
                
                if (line.hasOption(OPTION_PROPERTIES_FILE)) {
                    File file = new File(line.getOptionValue(OPTION_PROPERTIES_FILE));
                    String name = file.getName();
                    int index = name.lastIndexOf(".");
                    if (index > 0) {
                        name = name.substring(0, index);
                    }
                    fileName = fileName.replace("symmetric.log", name + ".log");
                    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
                    Configuration config = ctx.getConfiguration();
                    RollingFileAppender rolling = RollingFileAppender.newBuilder().setConfiguration(config).setName("ROLLING")
                        .withFileName(fileName)
                        .withFilePattern(fa.getFilePattern().replace("symmetric.log", name + ".log"))
                        .setLayout(fa.getLayout()).withPolicy(fa.getTriggeringPolicy())
                        .withStrategy(fa.getManager().getRolloverStrategy()).build();
                    LogSummaryAppenderUtils.removeAppender("ROLLING");
                    LogSummaryAppenderUtils.addAppender(rolling);
                }
                System.err.println(String.format("Log output will be written to %s", fileName));
            }
        }
    }

    protected void configurePropertiesFile(CommandLine line) throws ParseException {
        if (line.hasOption(OPTION_PROPERTIES_FILE)) {
            String propertiesFilename = line.getOptionValue(OPTION_PROPERTIES_FILE);
            propertiesFile = new File(propertiesFilename);
            if (!propertiesFile.exists()) {
                throw new SymmetricException("Could not find the properties file specified: %s",
                        line.getOptionValue(OPTION_PROPERTIES_FILE));
            }
        } else if (line.hasOption(OPTION_ENGINE)) {
            propertiesFile = findPropertiesFileForEngineWithName(line.getOptionValue(OPTION_ENGINE));
            if (propertiesFile == null || (propertiesFile != null && !propertiesFile.exists())) {
                throw new SymmetricException(
                        "Could not find the properties file for the engine specified: %s",
                        line.getOptionValue(OPTION_ENGINE));
            }
        } else {
            propertiesFile = findSingleEnginesPropertiesFile();

            if (propertiesFile == null && requiresPropertiesFile(line)) {
                throw new ParseException(String.format("You must specify either --%s or --%s",
                        OPTION_ENGINE, OPTION_PROPERTIES_FILE));
            }
        }
    }

    public static String getEnginesDir() {
        String enginesDir = System.getProperty(SystemConstants.SYSPROP_ENGINES_DIR, AppUtils.getSymHome() + "/engines");
        new File(enginesDir).mkdirs();
        return enginesDir;
    }

    public static File findPropertiesFileForEngineWithName(String engineName) {
        File[] files = findEnginePropertiesFiles();
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            Properties properties = new Properties();
            try(FileInputStream is = new FileInputStream(file)) {
                properties.load(is);
                if (engineName.equals(properties.getProperty(ParameterConstants.ENGINE_NAME))) {
                    return file;
                }
            } catch (IOException ex) {
            }
        }
        return null;

    }

    public static File[] findEnginePropertiesFiles() {
        List<File> propFiles = new ArrayList<File>();
        File enginesDir = new File(getEnginesDir());
        File[] files = enginesDir.listFiles();
        if (files != null ) {
            for (int i = 0; i < files.length; i++) {
                File file = files[i];
                if (file.getName().endsWith(".properties")) {
                    propFiles.add(file);
                }
            }
        }
        return propFiles.toArray(new File[propFiles.size()]);
    }

    public File findSingleEnginesPropertiesFile() {
        File[] files = findEnginePropertiesFiles();
        if (files.length == 1) {
            return files[0];
        } else {
            return null;
        }
    }

    protected void configureCrypto(CommandLine line) throws Exception {
        if (line.hasOption(OPTION_KEYSTORE_PASSWORD)) {
            System.setProperty(SecurityConstants.SYSPROP_KEYSTORE_PASSWORD,
                    line.getOptionValue(OPTION_KEYSTORE_PASSWORD));
        }

        if (line.hasOption(OPTION_KEYSTORE_TYPE)) {
            System.setProperty(SystemConstants.SYSPROP_KEYSTORE_TYPE,
                    line.getOptionValue(OPTION_KEYSTORE_TYPE));
        }

        if (line.hasOption(OPTION_JCE_PROVIDER)) {
            Provider provider = (Provider) Class.forName(line.getOptionValue(OPTION_JCE_PROVIDER))
                    .getDeclaredConstructor().newInstance();
            Security.addProvider(provider);
        }
    }

    protected ISymmetricEngine getSymmetricEngine() {
        return getSymmetricEngine(true);
    }

    protected ISymmetricEngine getSymmetricEngine(boolean testConnection) {
        if (engine == null) {
            if (testConnection) {
                testConnection();
            }
            engine = new ClientSymmetricEngine(propertiesFile);
            platform = engine.getSymmetricDialect().getPlatform();
        }
        return engine;
    }
    
    protected void testConnection() {
        try {
            BasicDataSource ds = ClientSymmetricEngine
                    .createBasicDataSource(propertiesFile);
            Connection conn = ds.getConnection();
            conn.close();
            ds.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }        
    }

    protected IDatabasePlatform getDatabasePlatform(boolean testConnection) {
        if (platform == null) {
            if (testConnection) {
                testConnection();
            }
            platform = ClientSymmetricEngine.createDatabasePlatform(null, new TypedProperties(
                    propertiesFile), null, false);
        }
        return platform;
    }
    
    protected TypedProperties getTypedProperties() {
        return new TypedProperties(propertiesFile);
    }

    protected void buildOptions(Options options) {
        addCommonOption(options, "h", HELP, false);
        addCommonOption(options, "p", OPTION_PROPERTIES_FILE, true);
        addCommonOption(options, "e", OPTION_ENGINE, true);
        addCommonOption(options, "v", OPTION_VERBOSE_CONSOLE, false);
        addCommonOption(options, null, OPTION_DEBUG, false);
        addCommonOption(options, null, OPTION_NO_LOG_CONSOLE, false);
        addCommonOption(options, null, OPTION_NO_LOG_FILE, false);
    }

    protected void buildCryptoOptions(Options options) {
        addCommonOption(options, OPTION_KEYSTORE_PASSWORD, true);
        addCommonOption(options, OPTION_KEYSTORE_TYPE, true);
        addCommonOption(options, OPTION_JCE_PROVIDER, true);
    }

    protected void addOption(Options options, String opt, String longOpt, boolean hasArg) {
        options.addOption(opt, longOpt, hasArg, Message.get(messageKeyPrefix + longOpt));
    }

    protected void addOption(Options options, String opt, boolean hasArg) {
        options.addOption(opt, null, hasArg, Message.get(messageKeyPrefix + opt));
    }

    protected void addCommonOption(Options options, String opt, String longOpt, boolean hasArg) {
        options.addOption(opt, longOpt, hasArg, Message.get(COMMON_MESSAGE_KEY_PREFIX + longOpt));
    }

    protected void addCommonOption(Options options, String opt, boolean hasArg) {
        options.addOption(opt, null, hasArg, Message.get(COMMON_MESSAGE_KEY_PREFIX + opt));
    }

    protected abstract boolean executeWithOptions(CommandLine line) throws Exception;

}
