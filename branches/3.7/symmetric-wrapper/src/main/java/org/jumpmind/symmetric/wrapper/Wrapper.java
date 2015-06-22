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
package org.jumpmind.symmetric.wrapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Wrapper {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            printUsage();
            System.exit(Constants.RC_BAD_USAGE);
        }
       
        String dir = System.getenv(Constants.ENV_SYM_HOME);
        if (dir == null || dir.equals("")) {
            // Backwards compatible with 3.6 by allowing config file argument to determine home
            if (args.length > 1) {
                int index = args[1].lastIndexOf(File.separator);
                if (index == -1) {
                    dir = "..";
                } else {
                    dir = args[1].substring(0, index + 1) + "..";
                }
            } else {
                System.out.println("Missing " + Constants.ENV_SYM_HOME + " environment variable.");
                System.exit(Constants.RC_MISSING_SYM_HOME);
            }
        }
        System.setProperty("java.io.tmpdir", dir + File.separator + "tmp");
        String configFile = dir + File.separator + "conf" + File.separator + "sym_service.conf";

        WrapperService service = WrapperService.getInstance();
        try {
            service.loadConfig(configFile);
        } catch (FileNotFoundException e) {
            System.out.println("Missing config file " + args[1]);
            System.out.println(e.getMessage());
            System.exit(Constants.RC_MISSING_CONFIG_FILE);
        } catch (IOException e) {
            System.out.println("Cannot read config file " + args[1]);
            System.out.println(e.getMessage());
            System.exit(Constants.RC_FAIL_READ_CONFIG_FILE);
        }

        try {
            if (args[0].equalsIgnoreCase("start")) {
                service.start();
            } else if (args[0].equalsIgnoreCase("exec")) {
                service.execJava(false);
            } else if (args[0].equalsIgnoreCase("init")) {
                service.init();
            } else if (args[0].equalsIgnoreCase("stop")) {
                service.stop();
            } else if (args[0].equalsIgnoreCase("restart")) {    
                service.restart();
            } else if (args[0].equalsIgnoreCase("install")) {
                service.install();
            } else if (args[0].equalsIgnoreCase("uninstall")) {
                service.uninstall();
            } else if (args[0].equalsIgnoreCase("status")) {
                service.status();
            } else if (args[0].equalsIgnoreCase("console")) {
                service.console();
            } else {
                System.out.println("ERROR: Invalid argument");
                printUsage();
                System.exit(Constants.RC_INVALID_ARGUMENT);
            }
        } catch (WrapperException e) {
            System.out.println("Error " + e.getErrorCode() + ": " + e.getMessage());
            if (e.getCause() != null) {
                System.out.println("Exception " + e.getCause().getClass().getSimpleName() + ": "
                        + e.getCause().getMessage());
            }
            if (e.getNativeErrorCode() > 0) {
                System.out.println("Native error " + e.getErrorCode());    
            }
            System.exit(e.getErrorCode());
        }
    }

    private static void printUsage() {
        System.out.println("Usage: <start|stop|restart|install|remove|console>");
        System.out.println("   start      - Start service");
        System.out.println("   stop       - Stop service");
        System.out.println("   restart    - Restart service");
        System.out.println("   install    - Install service");
        System.out.println("   uninstall  - Uninstall service");
        System.out.println("   status     - Status of service");
        System.out.println("   console    - Run from console");
    }
}
