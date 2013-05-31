package org.jumpmind.symmetric;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Launch the SymmetricDS engine as a standalone client or server.
 */
public class SymmetricLauncher extends AbstractCommandLauncher {

    static final Logger log = LoggerFactory.getLogger(SymmetricLauncher.class);

    private static final String OPTION_PORT_SERVER = "port";

    private static final String OPTION_HOST_SERVER = "host";
    
    private static final String OPTION_HTTP_BASIC_AUTH_USER = "http-basic-auth-user";
    
    private static final String OPTION_HTTP_BASIC_AUTH_PASSWORD = "http-basic-auth-password";

    private static final String OPTION_SECURE_PORT_SERVER = "secure-port";

    private static final String OPTION_MAX_IDLE_TIME = "max-idle-time";

    private static final String OPTION_START_SERVER = "server";

    private static final String OPTION_START_CLIENT = "client";

    private static final String OPTION_START_SECURE_SERVER = "secure-server";

    private static final String OPTION_START_MIXED_SERVER = "mixed-server";

    private static final String OPTION_NO_NIO = "no-nio";

    private static final String OPTION_NO_DIRECT_BUFFER = "no-directbuffer";

    private static final String OPTION_JMX_DISABLE = "jmx-disable";
    
    private static final String OPTION_JMX_PORT = "jmx-port";
    
    public SymmetricLauncher(String app, String argSyntax, String messageKeyPrefix) {
        super(app, argSyntax, messageKeyPrefix);
    }

    public static void main(String... args) throws Exception {
        MDC.put("engineName", "startup");
        new SymmetricLauncher("sym", "", "Launcher.Option.").execute(args);
    }

    @Override
    protected boolean printHelpIfNoOptionsAreProvided() {
        return false;
    }

    @Override
    protected boolean requiresPropertiesFile() {
        return false;
    }

    @Override
    protected void printHelp(CommandLine cmd, Options options) {
        System.out.println(app + " version " + Version.version());
        System.out.println("Launch the SymmetricDS engine as a standalone client or server.\n");
        super.printHelp(cmd, options);
    }

    protected void buildOptions(Options options) {
        super.buildOptions(options);
        buildCryptoOptions(options);
        addOption(options, "S", OPTION_START_SERVER, false);
        addOption(options, "C", OPTION_START_CLIENT, false);
        addOption(options, "T", OPTION_START_SECURE_SERVER, false);
        addOption(options, "U", OPTION_START_MIXED_SERVER, false);
        addOption(options, "H", OPTION_HOST_SERVER, true);
        addOption(options, "P", OPTION_PORT_SERVER, true);
        addOption(options, "Q", OPTION_SECURE_PORT_SERVER, true);
        addOption(options, "I", OPTION_MAX_IDLE_TIME, true);
        addOption(options, "nnio", OPTION_NO_NIO, false);
        addOption(options, "ndb", OPTION_NO_DIRECT_BUFFER, false);
        addOption(options, "hbau", OPTION_HTTP_BASIC_AUTH_USER, true);
        addOption(options, "hbap", OPTION_HTTP_BASIC_AUTH_PASSWORD, true);
        addOption(options, "JD", OPTION_JMX_DISABLE, false);
        addOption(options, "J", OPTION_JMX_PORT, true);
    }

    protected boolean executeWithOptions(CommandLine line) throws Exception {

        String host = null;
        int httpPort = 0;
        int httpSecurePort = 0;
        int jmxPort = 0;
        
        String webDir = SymmetricWebServer.DEFAULT_WEBAPP_DIR;
        int maxIdleTime = SymmetricWebServer.DEFAULT_MAX_IDLE_TIME;
        boolean noNio = false;
        boolean noDirectBuffer = false;
        String httpBasicAuthUser = null;
        String httpBasicAuthPassword = null;

        configureCrypto(line);

        if (line.hasOption(OPTION_HTTP_BASIC_AUTH_USER) && 
                line.hasOption(OPTION_HTTP_BASIC_AUTH_PASSWORD)) {
            httpBasicAuthUser = line.getOptionValue(OPTION_HTTP_BASIC_AUTH_USER);
            httpBasicAuthPassword = line.getOptionValue(OPTION_HTTP_BASIC_AUTH_PASSWORD);
        }

        if (line.hasOption(OPTION_HOST_SERVER)) {
            host = line.getOptionValue(OPTION_HOST_SERVER);
        }

        if (line.hasOption(OPTION_PORT_SERVER)) {
            httpPort = new Integer(line.getOptionValue(OPTION_PORT_SERVER));
        }

        if (line.hasOption(OPTION_SECURE_PORT_SERVER)) {
            httpSecurePort = new Integer(line.getOptionValue(OPTION_SECURE_PORT_SERVER));
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

        if (!line.hasOption(OPTION_JMX_DISABLE)) {
            if (line.hasOption(OPTION_JMX_PORT)) {
                jmxPort = new Integer(line.getOptionValue(OPTION_JMX_PORT));
            } else {
                if (line.hasOption(OPTION_START_SECURE_SERVER)) {
                    if (httpSecurePort > 0) {
                        jmxPort = httpSecurePort + 1;
                    }
                } else {
                    if (httpPort > 0) {
                        jmxPort = httpPort + 1;
                    }
                }
            }
        }

        if (line.hasOption(OPTION_START_CLIENT)) {
            getSymmetricEngine(false).start();
            return true;
        } else {
            SymmetricWebServer webServer = new SymmetricWebServer(chooseWebDir(line, webDir),
                    maxIdleTime, propertiesFile != null ? propertiesFile.getCanonicalPath() : null,
                    true, noNio, noDirectBuffer);
            webServer.setHost(host);
            webServer.setBasicAuthUsername(httpBasicAuthUser);
            webServer.setBasicAuthPassword(httpBasicAuthPassword);
            
            if (jmxPort > 0) {
                webServer.setJmxPort(jmxPort);
            }
            
            if (httpPort > 0) {
                webServer.setHttpPort(httpPort);
            }     

            if (httpSecurePort > 0) {
                webServer.setHttpsPort(httpSecurePort);
            }
            
            if (line.hasOption(OPTION_START_MIXED_SERVER)) {
                webServer.setHttpEnabled(true);
                webServer.setHttpsEnabled(true);
                if (httpPort > 0) {
                    webServer.setHttpPort(httpPort);
                }
                if (httpSecurePort > 0) {
                    webServer.setHttpsPort(httpSecurePort);
                }
            } else if (line.hasOption(OPTION_START_SECURE_SERVER)) {
                webServer.setHttpEnabled(false);
                webServer.setHttpsEnabled(true);
                if (httpSecurePort > 0) {
                    webServer.setHttpsPort(httpSecurePort);
                }
            } else if (line.hasOption(OPTION_START_SERVER)) {
                webServer.setHttpEnabled(true);
                webServer.setHttpsEnabled(false);                
            }
            
            webServer.start();

            return true;
        }

    }

    protected String chooseWebDir(CommandLine line, String webDir) {
        return webDir;
    }

}
