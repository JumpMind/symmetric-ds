/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.xml.DOMConfigurator;
import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.log.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.Message;
import org.jumpmind.symmetric.common.SecurityConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.ISecurityService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.internal.InternalOutgoingTransport;
import org.jumpmind.symmetric.util.JarBuilder;

/**
 * Run SymmetricDS utilities and/or launch an embedded version of SymmetricDS.
 * If you run this program without any arguments 'help' will print out.
 */
public class SymmetricLauncher {

    private static final String OPTION_DUMP_BATCH = "dump-batch";

    private static final String OPTION_OPEN_REGISTRATION = "open-registration";

    private static final String OPTION_RELOAD_NODE = "reload-node";

    private static final String OPTION_AUTO_CREATE = "auto-create";

    private static final String OPTION_PORT_SERVER = "port";

    private static final String OPTION_SECURE_PORT_SERVER = "secure-port";

    private static final String OPTION_MAX_IDLE_TIME = "max-idle-time";

    private static final String OPTION_DDL_GEN = "generate-config-ddl";

    private static final String OPTION_TRIGGER_GEN = "generate-triggers";

    private static final String OPTION_TRIGGER_GEN_ALWAYS = "generate-triggers-always";

    private static final String OPTION_PURGE = "purge";

    private static final String OPTION_RUN_DDL_XML = "run-ddl";

    private static final String OPTION_RUN_SQL = "run-sql";

    private static final String OPTION_PROPERTIES_GEN = "generate-default-properties";

    private static final String OPTION_EXPORT_SCHEMA = "export-schema";

    private static final String OPTION_PROPERTIES_FILE = "properties";

    private static final String OPTION_START_SERVER = "server";

    private static final String OPTION_START_CLIENT = "client";

    private static final String OPTION_START_SECURE_SERVER = "secure-server";

    private static final String OPTION_START_MIXED_SERVER = "mixed-server";

    private static final String OPTION_NO_NIO = "no-nio";

    private static final String OPTION_NO_DIRECT_BUFFER = "no-directbuffer";

    private static final String OPTION_LOAD_BATCH = "load-batch";

    private static final String OPTION_SKIP_DB_VALIDATION = "skip-db-validate";

    private static final String OPTION_ENCRYPT_TEXT = "encrypt";

    private static final String OPTION_VERBOSE_CONSOLE = "verbose";

    private static final String OPTION_DEBUG = "debug";

    private static final String OPTION_NOCONSOLE = "noconsole";

    private static final String OPTION_NOLOGFILE = "nologfile";

    private static final String OPTION_CREATE_WAR = "create-war";

    private static final String MESSAGE_BUNDLE = "Launcher.Option.";

    private static final String OPTION_KEYSTORE_PASSWORD = "keystore-password";

    private static final String OPTION_KEYSTORE_TYPE = "keystore-type";

    protected SymmetricWebServer webServer;

    protected Exception exception;

    protected boolean join = true;

    public static void main(String... args) throws Exception {
        new SymmetricLauncher().execute(args);
    }

    public void execute(String... args) throws Exception {
        PosixParser parser = new PosixParser();
        Options options = new Options();
        buildOptions(options);
        try {
            CommandLine line = parser.parse(options, args);

            configureLogging(line);

            if (line.getOptions() != null) {
                for (Option option : line.getOptions()) {
                    LogFactory.getLog(SymmetricLauncher.class).info("Option: name=%s, value=%s", option.getLongOpt(),
                            ArrayUtils.toString(option.getValues()));
                }
            }

            if (!executeOptions(line)) {
                printHelp(options);
            }

        } catch (ParseException exp) {
            exception = exp;
            System.err.println(exp.getMessage());
            printHelp(options);
            System.exit(-1);
        } catch (Exception ex) {
            exception = ex;
            System.err
                    .println("-----------------------------------------------------------------------------------------------");
            System.err.println(Message.get("An exception occurred.  Please see the following for details:"));
            System.err
                    .println("-----------------------------------------------------------------------------------------------");

            ExceptionUtils.printRootCauseStackTrace(ex, System.err);
            System.err
                    .println("-----------------------------------------------------------------------------------------------");
            System.exit(-1);
        }
    }

    private void generateWar(String warFileName, String propertiesFileName) throws Exception {
        final File workingDirectory = new File("../.war");
        FileUtils.deleteDirectory(workingDirectory);
        FileUtils.copyDirectory(new File("../web"), workingDirectory);
        FileUtils.copyDirectory(new File("../conf"), new File(workingDirectory, "WEB-INF/classes"));
        if (propertiesFileName != null) {
            File propsFile = new File(propertiesFileName);
            if (propsFile.exists()) {
                FileUtils.copyFile(propsFile, new File(workingDirectory,
                        "WEB-INF/classes/symmetric.properties"));
            }
        }
        JarBuilder builder = new JarBuilder(workingDirectory, new File(warFileName),
                new File[] { workingDirectory });
        builder.build();
        FileUtils.deleteDirectory(workingDirectory);
    }

    private boolean isStartApplicationOption(CommandLine line) {
        return line.hasOption(OPTION_START_CLIENT) || line.hasOption(OPTION_START_SERVER)
                || line.hasOption(OPTION_START_SECURE_SERVER)
                || line.hasOption(OPTION_START_MIXED_SERVER);
    }

    private File getLog4JFile() throws MalformedURLException {
        URL url = new URL(System.getProperty("log4j.configuration", "file:../conf/log4j-blank.xml"));
        return new File(new File(url.getFile()).getParent(), "log4j.xml");
    }

    private File getLog4JFileDebugEnabled() throws MalformedURLException {
        File original = getLog4JFile();
        return new File(original.getParent(), "log4j-debug.xml");
    }

    private void printHelp(Options options) {
        new HelpFormatter().printHelp("sym", options);
    }

    private void testConnection(CommandLine line, String propertiesFile) throws Exception {
        if (!line.hasOption(OPTION_SKIP_DB_VALIDATION)) {
            BasicDataSource ds = ClientSymmetricEngine
                    .createBasicDataSource(propertiesFile != null ? new File(propertiesFile) : null);
            Connection c = ds.getConnection();
            c.close();
            ds.close();
        }
    }

    private void configureLogging(CommandLine line) throws MalformedURLException {
        File log4jFile = getLog4JFile();
        if (line.hasOption(OPTION_DEBUG)) {
            log4jFile = getLog4JFileDebugEnabled();
        }

        if (log4jFile.exists()) {
            DOMConfigurator.configure(log4jFile.getAbsolutePath());
        }

        if (line.hasOption(OPTION_VERBOSE_CONSOLE)) {
            Appender consoleAppender = Logger.getRootLogger().getAppender("CONSOLE");
            if (consoleAppender != null) {
                Layout layout = consoleAppender.getLayout();
                if (layout instanceof PatternLayout) {
                    ((PatternLayout) layout).setConversionPattern("%d %-5p [%c{2}] [%t] %m%n");
                }
            }
        }

        if (line.hasOption(OPTION_NOCONSOLE)) {
            Logger.getRootLogger().removeAppender("CONSOLE");
        }

        if (line.hasOption(OPTION_NOLOGFILE)) {
            Logger.getRootLogger().removeAppender("ROLLING");
        } else {
            Appender appender = Logger.getRootLogger().getAppender("ROLLING");
            if (appender instanceof FileAppender) {
                FileAppender fileAppender = (FileAppender) appender;

                if (line.hasOption(OPTION_PROPERTIES_FILE) && isStartApplicationOption(line)) {
                    File file = new File(line.getOptionValue(OPTION_PROPERTIES_FILE));
                    String name = file.getName();
                    int index = name.lastIndexOf(".");
                    if (index > 0) {
                        name = name.substring(0, index);
                    }
                    fileAppender.setFile(fileAppender.getFile().replace("symmetric.log",
                            name + ".log"));
                    fileAppender.activateOptions();
                }

                System.out.println(Message.get("Log output will be written to %s", fileAppender.getFile()));

            }
        }
    }

    protected void addPropertiesOption(Options options) {
        addOption(options, "p", OPTION_PROPERTIES_FILE, true);
    }

    protected void buildOptions(Options options) {
        addOption(options, "S", OPTION_START_SERVER, false);
        addOption(options, "C", OPTION_START_CLIENT, false);
        addOption(options, "T", OPTION_START_SECURE_SERVER, false);
        addOption(options, "U", OPTION_START_MIXED_SERVER, false);
        addOption(options, "P", OPTION_PORT_SERVER, true);
        addOption(options, "Q", OPTION_SECURE_PORT_SERVER, true);
        addOption(options, "I", OPTION_MAX_IDLE_TIME, true);
        addOption(options, "nnio", OPTION_NO_NIO, false);
        addOption(options, "ndb", OPTION_NO_DIRECT_BUFFER, false);

        addOption(options, "c", OPTION_DDL_GEN, true);

        addPropertiesOption(options);
        addOption(options, "X", OPTION_PURGE, false);
        addOption(options, "g", OPTION_PROPERTIES_GEN, true);
        addOption(options, "r", OPTION_RUN_DDL_XML, true);
        addOption(options, "s", OPTION_RUN_SQL, true);

        addOption(options, "a", OPTION_AUTO_CREATE, false);
        addOption(options, "R", OPTION_OPEN_REGISTRATION, true);
        addOption(options, "l", OPTION_RELOAD_NODE, true);
        addOption(options, "d", OPTION_DUMP_BATCH, true);
        addOption(options, "b", OPTION_LOAD_BATCH, true);
        addOption(options, "t", OPTION_TRIGGER_GEN, true);
        addOption(options, "o", OPTION_TRIGGER_GEN_ALWAYS, false);
        addOption(options, "e", OPTION_ENCRYPT_TEXT, true);
        addOption(options, "v", OPTION_VERBOSE_CONSOLE, false);
        addOption(options, OPTION_DEBUG, OPTION_DEBUG, false);
        addOption(options, OPTION_NOCONSOLE, OPTION_NOCONSOLE, false);
        addOption(options, OPTION_NOLOGFILE, OPTION_NOLOGFILE, false);
        addOption(options, "w", OPTION_CREATE_WAR, true);

        addOption(options, "x", OPTION_EXPORT_SCHEMA, true);

        addOption(options, "y", OPTION_ENCRYPT_TEXT, true);

        addOption(options, "ksp", OPTION_KEYSTORE_PASSWORD, true);

        addOption(options, "kst", OPTION_KEYSTORE_TYPE, true);

    }

    protected boolean executeOptions(CommandLine line) throws Exception {

        int port = Integer.parseInt(SymmetricWebServer.DEFAULT_HTTP_PORT);
        int securePort = Integer.parseInt(SymmetricWebServer.DEFAULT_HTTPS_PORT);
        String webDir = SymmetricWebServer.DEFAULT_WEBAPP_DIR;
        int maxIdleTime = SymmetricWebServer.DEFAULT_MAX_IDLE_TIME;
        String propertiesFile = null;
        boolean noNio = false;
        boolean noDirectBuffer = false;

        if (line.hasOption(OPTION_PORT_SERVER)) {
            port = new Integer(line.getOptionValue(OPTION_PORT_SERVER));
        }

        if (line.hasOption(OPTION_SECURE_PORT_SERVER)) {
            securePort = new Integer(line.getOptionValue(OPTION_SECURE_PORT_SERVER));
        }

        if (line.hasOption(OPTION_KEYSTORE_PASSWORD)) {
            System.setProperty(SecurityConstants.SYSPROP_KEYSTORE_PASSWORD,
                    line.getOptionValue(OPTION_KEYSTORE_PASSWORD));
        }

        if (line.hasOption(OPTION_KEYSTORE_TYPE)) {
            System.setProperty(SecurityConstants.SYSPROP_KEYSTORE_TYPE,
                    line.getOptionValue(OPTION_KEYSTORE_TYPE));
        }

        if (line.hasOption(OPTION_MAX_IDLE_TIME)) {
            maxIdleTime = new Integer(line.getOptionValue(OPTION_MAX_IDLE_TIME));
        }

        if (line.hasOption(OPTION_NO_NIO)) {
            noNio = true;
        }

        if (line.hasOption(OPTION_NO_DIRECT_BUFFER)) {
            noDirectBuffer = true;
        }

        if (line.hasOption(OPTION_PROPERTIES_GEN)) {
            generateDefaultProperties(line.getOptionValue(OPTION_PROPERTIES_GEN));
            System.exit(0);
            return true;
        }

        // validate that block-size has been set
        if (line.hasOption(OPTION_PROPERTIES_FILE)) {
            propertiesFile = "file:" + line.getOptionValue(OPTION_PROPERTIES_FILE);
            System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_1, propertiesFile);
            if (!new File(line.getOptionValue(OPTION_PROPERTIES_FILE)).exists()) {
                throw new SymmetricException("Could not find the properties file specified: %s",
                        line.getOptionValue(OPTION_PROPERTIES_FILE));
            }
        } else {
            propertiesFile = choosePropertiesFile(line, propertiesFile);
        }

        if (line.hasOption(OPTION_CREATE_WAR)) {
            generateWar(line.getOptionValue(OPTION_CREATE_WAR), propertiesFile);
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_DDL_GEN)) {
            testConnection(line, propertiesFile);
            generateDDL(createEngine(propertiesFile), line.getOptionValue(OPTION_DDL_GEN));
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_PURGE)) {
            testConnection(line, propertiesFile);
            ISymmetricEngine engine = createEngine(propertiesFile);
            IPurgeService purgeService = engine.getPurgeService();
            purgeService.purgeOutgoing();
            purgeService.purgeIncoming();
            purgeService.purgeDataGaps();
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_OPEN_REGISTRATION)) {
            testConnection(line, propertiesFile);
            String arg = line.getOptionValue(OPTION_OPEN_REGISTRATION);
            openRegistration(createEngine(propertiesFile), arg);
            System.out.println(Message.get("Opened Registration for %s", arg));
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_RELOAD_NODE)) {
            testConnection(line, propertiesFile);
            String arg = line.getOptionValue(OPTION_RELOAD_NODE);
            String message = reloadNode(createEngine(propertiesFile), arg);
            System.out.println(message);
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_DUMP_BATCH)) {
            testConnection(line, propertiesFile);
            String arg = line.getOptionValue(OPTION_DUMP_BATCH);
            dumpBatch(createEngine(propertiesFile), arg);
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_TRIGGER_GEN)) {
            testConnection(line, propertiesFile);
            String arg = line.getOptionValue(OPTION_TRIGGER_GEN);
            boolean gen_always = line.hasOption(OPTION_TRIGGER_GEN_ALWAYS);
            syncTrigger(createEngine(propertiesFile), arg, gen_always);
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_AUTO_CREATE)) {
            testConnection(line, propertiesFile);
            autoCreateDatabase(createEngine(propertiesFile));
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_EXPORT_SCHEMA)) {
            testConnection(line, propertiesFile);
            exportSchema(createEngine(propertiesFile), line.getOptionValue(OPTION_EXPORT_SCHEMA));
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_RUN_DDL_XML)) {
            testConnection(line, propertiesFile);
            runDdlXml(createEngine(propertiesFile), line.getOptionValue(OPTION_RUN_DDL_XML));
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_RUN_SQL)) {
            testConnection(line, propertiesFile);
            runSql(createEngine(propertiesFile), line.getOptionValue(OPTION_RUN_SQL));
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_LOAD_BATCH)) {
            testConnection(line, propertiesFile);
            loadBatch(createEngine(propertiesFile), line.getOptionValue(OPTION_LOAD_BATCH));
            System.exit(0);
            return true;
        }

        if (line.hasOption(OPTION_ENCRYPT_TEXT)) {
            testConnection(line, propertiesFile);
            encryptText(createEngine(propertiesFile), line.getOptionValue(OPTION_ENCRYPT_TEXT));
            return true;
        }

        if (line.hasOption(OPTION_START_CLIENT)) {
            createEngine(propertiesFile).start();
            return true;
        }

        if (line.hasOption(OPTION_START_SERVER) || line.hasOption(OPTION_START_SECURE_SERVER)
                || line.hasOption(OPTION_START_MIXED_SERVER)) {
            webServer = new SymmetricWebServer(chooseWebDir(line, webDir), maxIdleTime,
                    propertiesFile, join, noNio, noDirectBuffer);
            if (line.hasOption(OPTION_START_SERVER)) {
                webServer.start(port);
            } else if (line.hasOption(OPTION_START_SECURE_SERVER)) {
                webServer.startSecure(securePort);
            } else {
                webServer.startMixed(port, securePort);
            }
            return true;
        }

        return false;

    }

    protected String choosePropertiesFile(CommandLine line, String propertiesFile) {
        return propertiesFile;
    }

    protected String chooseWebDir(CommandLine line, String webDir) {
        return webDir;
    }

    protected ISymmetricEngine createEngine(String propertiesFile) {
        File propsFile = new File(propertiesFile);
        if (propsFile.exists()) {
            return new ClientSymmetricEngine(propsFile);
        } else {
            throw new SymmetricException("");
        }
    }

    protected void addOption(Options options, String opt, String longOpt, boolean hasArg) {
        options.addOption(opt, longOpt, hasArg, Message.get(MESSAGE_BUNDLE + longOpt));
    }

    private void dumpBatch(ISymmetricEngine engine, String batchId) throws Exception {
        IDataExtractorService dataExtractorService = engine.getDataExtractorService();
        IOutgoingTransport transport = new InternalOutgoingTransport(System.out);
        dataExtractorService.extractBatchRange(transport, batchId, batchId);
        transport.close();
    }

    private void loadBatch(ISymmetricEngine engine, String fileName) throws Exception {
        IDataLoaderService service = engine.getDataLoaderService();
        File file = new File(fileName);
        if (file.exists() && file.isFile()) {
            FileInputStream in = new FileInputStream(file);
            service.loadDataFromPush("", in, System.out);
            System.out.flush();
            in.close();

        } else {
            throw new SymmetricException("Launcher.Exception.FileNotFound", fileName);
        }
    }

    private void encryptText(ISymmetricEngine engine, String plainText) {
        ISecurityService service = engine.getSecurityService();
        System.out.println(SecurityConstants.PREFIX_ENC + service.encrypt(plainText));
    }

    private void exportSchema(ISymmetricEngine engine, String fileName) {
        IDatabasePlatform platform = engine.getSymmetricDialect().getPlatform();
        Database db = platform.readDatabase(platform.getDefaultCatalog(),
                platform.getDefaultSchema(), null);
        new DatabaseIO().write(db, fileName);
    }

    private void openRegistration(ISymmetricEngine engine, String argument) {
        argument = argument.replace('\"', ' ');
        int index = argument.trim().indexOf(",");
        if (index < 0) {
            throw new SymmetricException("Please provide a file name to write the trigger SQL to",
                    OPTION_OPEN_REGISTRATION);
        }
        String nodeGroupId = argument.substring(0, index).trim();
        String externalId = argument.substring(index + 1).trim();
        IRegistrationService registrationService = engine.getRegistrationService();
        registrationService.openRegistration(nodeGroupId, externalId);
    }

    private String reloadNode(ISymmetricEngine engine, String argument) {
        IDataService dataService = engine.getDataService();
        return dataService.reloadNode(argument);
    }

    private void syncTrigger(ISymmetricEngine engine, String fileName, boolean gen_always)
            throws IOException {
        if (fileName != null) {
            File file = new File(fileName);
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
            ITriggerRouterService triggerService = engine.getTriggerRouterService();
            StringBuilder sqlBuffer = new StringBuilder();
            triggerService.syncTriggers(sqlBuffer, gen_always);
            FileUtils.writeStringToFile(file, sqlBuffer.toString(), null);
        } else {
            throw new SymmetricException("MissingFilenameTriggerSQL");
        }
    }

    private void generateDDL(ISymmetricEngine engine, String fileName) throws IOException {
        File file = new File(fileName);
        if (file.getParentFile() != null) {
            file.getParentFile().mkdirs();
        }
        FileWriter os = new FileWriter(file, false);
        os.write(engine.getSymmetricDialect().getCreateSymmetricDDL());
        os.close();
    }

    private void generateDefaultProperties(String fileName) throws IOException {
        File file = new File(fileName);
        if (file.getParentFile() != null) {
            file.getParentFile().mkdirs();
        }
        BufferedReader is = new BufferedReader(new InputStreamReader(
                SymmetricLauncher.class.getResourceAsStream("/symmetric-default.properties"),
                Charset.defaultCharset()));
        FileWriter os = new FileWriter(file, false);
        String line = is.readLine();
        while (line != null) {
            os.write(line);
            os.write(System.getProperty("line.separator"));
            line = is.readLine();
        }
        is.close();
        os.close();
    }

    private void autoCreateDatabase(ISymmetricEngine engine) {
        engine.setupDatabase(true);
    }

    private void runDdlXml(ISymmetricEngine engine, String fileName) throws FileNotFoundException {
        ISymmetricDialect dialect = engine.getSymmetricDialect();
        File file = new File(fileName);
        if (file.exists() && file.isFile()) {
            IDatabasePlatform pf = dialect.getPlatform();
            Database db = new DatabaseIO().read(new File(fileName));
            pf.createDatabase(db, false, true);
        } else {
            throw new SymmetricException("Could not find file %s", fileName);
        }
    }

    private void runSql(ISymmetricEngine engine, String fileName) throws FileNotFoundException,
            MalformedURLException {
        ISymmetricDialect dialect = engine.getSymmetricDialect();
        File file = new File(fileName);
        if (file.exists() && file.isFile()) {
            SqlScript script = new SqlScript(file.toURI().toURL(), dialect.getPlatform()
                    .getSqlTemplate(), true, SqlScript.QUERY_ENDS, dialect.getPlatform()
                    .getSqlScriptReplacementTokens());
            script.execute();
        } else {
            throw new SymmetricException("Could not find file %s", fileName);
        }
    }

}