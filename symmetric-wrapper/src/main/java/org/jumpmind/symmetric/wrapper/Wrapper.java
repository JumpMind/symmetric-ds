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

public class Wrapper {

    public static void main(String[] args) throws Exception {
    	
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
        String configFile = dir + File.separator + "conf" + File.separator + "sym_service.conf";
        String jarFile = dir + File.separator + "lib" + File.separator + Constants.JAR_NAME;
        
        WrapperHelper.run(args, dir, configFile, jarFile);
    }
}
