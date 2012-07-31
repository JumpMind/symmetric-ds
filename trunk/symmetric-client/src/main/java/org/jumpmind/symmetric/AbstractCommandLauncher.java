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
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.xml.DOMConfigurator;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.SystemConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCommandLauncher {

    protected static final Logger log = LoggerFactory.getLogger(AbstractCommandLauncher.class);

    protected static final String HELP = "help";

    protected static final String OPTION_PROPERTIES_FILE = "properties";

    protected static final String OPTION_ENGINE = "engine";

    protected static final String OPTION_VERBOSE_CONSOLE = "verbose";

    protected static final String OPTION_DEBUG = "debug";

    protected static final String OPTION_NOCONSOLE = "noconsole";

    protected static final String OPTION_NOLOGFILE = "nologfile";

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

    public AbstractCommandLauncher(String app, String argSyntax, String messageKeyPrefix) {
        this.app = app;
        this.argSyntax = argSyntax;
        this.messageKeyPrefix = messageKeyPrefix;
    }

    abstract protected boolean printHelpIfNoOptionsAreProvided();

    abstract protected boolean requiresPropertiesFile();

    public void execute(String args[]) {
        PosixParser parser = new PosixParser();
        Options options = new Options();
        buildOptions(options);
        try {
            CommandLine line = parser.parse(options, args);
            
            if (line.hasOption(HELP)
                    || (line.getArgList().contains(HELP))
                    || (line.getArgList().size() == 0 && printHelpIfNoOptionsAreProvided())) {
                printHelp(line, options);
                System.exit(2);
            }

            configureLogging(line);
            configurePropertiesFile(line);

            if (line.getOptions() != null) {
                for (Option option : line.getOptions()) {
                    log.info("Option: name={}, value={}", new Object[] { option.getLongOpt(),
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
        URL log4jUrl = new URL(System.getProperty("log4j.configuration",
                "file:../conf/log4j-blank.xml"));
        File log4jFile = new File(new File(log4jUrl.getFile()).getParent(), "log4j.xml");

        if (line.hasOption(OPTION_DEBUG)) {
            log4jFile = new File(log4jFile.getParent(), "log4j-debug.xml");
        }

        if (log4jFile.exists()) {
            DOMConfigurator.configure(log4jFile.getAbsolutePath());
        }

        if (line.hasOption(OPTION_VERBOSE_CONSOLE)) {
            Appender consoleAppender = org.apache.log4j.Logger.getRootLogger().getAppender(
                    "CONSOLE");
            if (consoleAppender != null) {
                Layout layout = consoleAppender.getLayout();
                if (layout instanceof PatternLayout) {
                    ((PatternLayout) layout).setConversionPattern("%d %-5p [%c{2}] [%t] %m%n");
                }
            }
        }

        if (line.hasOption(OPTION_NOCONSOLE)) {
            org.apache.log4j.Logger.getRootLogger().removeAppender("CONSOLE");
        }

        if (line.hasOption(OPTION_NOLOGFILE)) {
            org.apache.log4j.Logger.getRootLogger().removeAppender("ROLLING");
        } else {
            Appender appender = org.apache.log4j.Logger.getRootLogger().getAppender("ROLLING");
            if (appender instanceof FileAppender) {
                FileAppender fileAppender = (FileAppender) appender;

                if (line.hasOption(OPTION_PROPERTIES_FILE)) {
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

                System.out.println(String.format("Log output will be written to %s",
                        fileAppender.getFile()));
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
            if (propertiesFile == null || (propertiesFile!=null && !propertiesFile.exists())) {
                throw new SymmetricException(
                        "Could not find the properties file for the engine specified: %s",
                        line.getOptionValue(OPTION_ENGINE));
            }
        } else {
            propertiesFile = findSingleEnginesPropertiesFile();
            
            if (propertiesFile == null && requiresPropertiesFile()) {
                throw new ParseException(String.format("You must specify either --%s or --%s",
                        OPTION_ENGINE, OPTION_PROPERTIES_FILE));
            }
        }
    }

    public static String getEnginesDir() {
        String enginesDir = System.getProperty(SystemConstants.SYSPROP_ENGINES_DIR, "../engines");
        new File(enginesDir).mkdirs();
        return enginesDir;
    }

    public File findPropertiesFileForEngineWithName(String engineName) {
        File[] files = findEnginePropertiesFiles();
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            Properties properties = new Properties();
            FileInputStream is = null;
            try {
                is = new FileInputStream(file);
                properties.load(is);
                if (engineName.equals(properties.getProperty(ParameterConstants.ENGINE_NAME))) {
                    return file;
                }
            } catch (IOException ex) {
            } finally {
                IOUtils.closeQuietly(is);
            }
        }
        return null;

    }

    public File[] findEnginePropertiesFiles() {
        List<File> propFiles = new ArrayList<File>();
        File enginesDir = new File(getEnginesDir());
        File[] files = enginesDir.listFiles();
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            if (file.getName().endsWith(".properties")) {
                propFiles.add(file);
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
            System.setProperty(SystemConstants.SYSPROP_KEYSTORE_PASSWORD,
                    line.getOptionValue(OPTION_KEYSTORE_PASSWORD));
        }

        if (line.hasOption(OPTION_KEYSTORE_TYPE)) {
            System.setProperty(SystemConstants.SYSPROP_KEYSTORE_TYPE,
                    line.getOptionValue(OPTION_KEYSTORE_TYPE));
        }

        if (line.hasOption(OPTION_JCE_PROVIDER)) {
            Provider provider = (Provider) Class.forName(line.getOptionValue(OPTION_JCE_PROVIDER))
                    .newInstance();
            Security.addProvider(provider);
        }
    }

    protected ISymmetricEngine getSymmetricEngine() {
        return getSymmetricEngine(true);
    }

    protected ISymmetricEngine getSymmetricEngine(boolean testConnection) {
        if (engine == null) {
            if (testConnection) {
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
            engine = new ClientSymmetricEngine(propertiesFile);
        }
        return engine;
    }

    protected IDatabasePlatform getDatabasePlatform() {
        if (platform == null) {
            platform = getSymmetricEngine().getSymmetricDialect().getPlatform();
        }
        return platform;
    }

    protected void buildOptions(Options options) {
        addCommonOption(options, "h", HELP, false);
        addCommonOption(options, "p", OPTION_PROPERTIES_FILE, true);
        addCommonOption(options, "e", OPTION_ENGINE, true);
        addCommonOption(options, "v", OPTION_VERBOSE_CONSOLE, false);
        addCommonOption(options, null, OPTION_DEBUG, false);
        addCommonOption(options, null, OPTION_NOCONSOLE, false);
        addCommonOption(options, null, OPTION_NOLOGFILE, false);
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
