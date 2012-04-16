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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launch the SymmetricDS engine as a standalone client or server.
 */
public class SymmetricLauncher extends AbstractCommandLauncher {

    static final Logger log = LoggerFactory.getLogger(SymmetricLauncher.class);

    private static final String OPTION_PORT_SERVER = "port";
    
    private static final String OPTION_HOST_SERVER = "host";

    private static final String OPTION_SECURE_PORT_SERVER = "secure-port";

    private static final String OPTION_MAX_IDLE_TIME = "max-idle-time";

    private static final String OPTION_START_SERVER = "server";

    private static final String OPTION_START_CLIENT = "client";

    private static final String OPTION_START_SECURE_SERVER = "secure-server";

    private static final String OPTION_START_MIXED_SERVER = "mixed-server";

    private static final String OPTION_NO_NIO = "no-nio";

    private static final String OPTION_NO_DIRECT_BUFFER = "no-directbuffer";

    public SymmetricLauncher(String app, String argSyntax, String messageKeyPrefix) {
		super(app, argSyntax, messageKeyPrefix);
	}

    public static void main(String... args) throws Exception {
        new SymmetricLauncher("sym", "", "Launcher.Option.").execute(args);
    }

    protected void printHelp(Options options) {
    	System.out.println(app + " version " + Version.version());
    	System.out.println("Launch the SymmetricDS engine as a standalone client or server.\n");
    	super.printHelp(options);
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
    }

    protected boolean executeOptions(CommandLine line) throws Exception {

        String host = null;
        int port = Integer.parseInt(SymmetricWebServer.DEFAULT_HTTP_PORT);
        int securePort = Integer.parseInt(SymmetricWebServer.DEFAULT_HTTPS_PORT);
        String webDir = SymmetricWebServer.DEFAULT_WEBAPP_DIR;
        int maxIdleTime = SymmetricWebServer.DEFAULT_MAX_IDLE_TIME;
        boolean noNio = false;
        boolean noDirectBuffer = false;

        configureCrypto(line);

        if (line.hasOption(OPTION_HOST_SERVER)) {
            host = line.getOptionValue(OPTION_HOST_SERVER);
        }
        
        if (line.hasOption(OPTION_PORT_SERVER)) {
            port = new Integer(line.getOptionValue(OPTION_PORT_SERVER));
        }

        if (line.hasOption(OPTION_SECURE_PORT_SERVER)) {
            securePort = new Integer(line.getOptionValue(OPTION_SECURE_PORT_SERVER));
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

        if (line.hasOption(OPTION_START_CLIENT)) {
            getSymmetricEngine(false).start();
            return true;
        }

        if (line.hasOption(OPTION_START_SERVER) || line.hasOption(OPTION_START_SECURE_SERVER)
                || line.hasOption(OPTION_START_MIXED_SERVER)) {
        	SymmetricWebServer webServer = new SymmetricWebServer(chooseWebDir(line, webDir), maxIdleTime,
                    propertiesFile != null ? propertiesFile.getCanonicalPath() : null, true, noNio, noDirectBuffer);
            webServer.setHost(host);
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

    protected String chooseWebDir(CommandLine line, String webDir) {
        return webDir;
    }

}
