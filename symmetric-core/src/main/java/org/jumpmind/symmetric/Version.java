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

import org.jumpmind.util.AbstractVersion;

/**
 * Follow the Apache versioning scheme documented <a
 * href="http://apr.apache.org/versioning.html">here</a>.
 */
final public class Version {

    private static AbstractVersion version = new AbstractVersion() {
        @Override
        protected String getPropertiesFileLocation() {
            return "/META-INF/maven/org.jumpmind.symmetric/symmetric-core/pom.properties";
        }
    };

    public static String version() {
        return version.version();
    }

    public static String versionWithUnderscores() {
        return version.versionWithUnderscores();
    }

    public static int[] parseVersion(String version) {
        return Version.version.parseVersion(version);
    }    

    public static boolean isOlderVersion(String version) {
        return isOlderThanVersion(version, version());
    }

    public static boolean isOlderThanVersion(String checkVersion, String targetVersion) {
        return version.isOlderThanVersion(checkVersion, targetVersion);
    }
       
    public static boolean hasOlderMinorVersion(String version) {
        return Version.version.isOlderMinorVersion(version);
    }



}