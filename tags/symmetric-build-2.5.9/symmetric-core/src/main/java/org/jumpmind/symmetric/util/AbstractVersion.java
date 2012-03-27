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
 * under the License.  */
package org.jumpmind.symmetric.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

/**
 * Follow the Apache versioning scheme documented <a
 * href="http://apr.apache.org/versioning.html">here</a>.
 */
abstract public class AbstractVersion {

    final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
            .getLog(getClass());

    public static final int MAJOR_INDEX = 0;

    public static final int MINOR_INDEX = 1;

    public static final int PATCH_INDEX = 2;

    private String version = null;
    
    abstract protected String getPropertiesFileLocation();

    public  String version() {
        if (version == null) {
            InputStream is = AbstractVersion.class
                    .getResourceAsStream(getPropertiesFileLocation());
            if (is != null) {
                Properties p = new Properties();
                try {
                    p.load(is);
                    version = p.getProperty("version");
                } catch (IOException e) {
                    version = "unknown";
                    log.warn(e, e);
                }
            } else {
                version = "development";
            }
        }
        return version;
    }

    public String versionWithUnderscores() {
        return version().replace("[\\.\\-]", "_");
    }

    public int[] parseVersion(String version) {
        version = version.replaceAll("[^0-9\\.]", "");
        int[] versions = new int[3];
        if (!StringUtils.isEmpty(version)) {
            String[] splitVersion = version.split("\\.");
            if (splitVersion.length >= 3) {
                versions[PATCH_INDEX] = parseVersionComponent(splitVersion[2]);
            }
            if (splitVersion.length >= 2) {
                versions[MINOR_INDEX] = parseVersionComponent(splitVersion[1]);
            }
            if (splitVersion.length >= 1) {
                versions[MAJOR_INDEX] = parseVersionComponent(splitVersion[0]);
            }
        }
        return versions;
    }

    private  int parseVersionComponent(String versionComponent) {
        int version = 0;
        try {
            version = Integer.parseInt(versionComponent);
        } catch (NumberFormatException e) {
        }
        return version;
    }

    protected  boolean isOlderMajorVersion(String version) {
        return isOlderMajorVersion(parseVersion(version));
    }

    protected  boolean isOlderMajorVersion(int[] versions) {
        int[] softwareVersion = parseVersion(version());
        if (versions[MAJOR_INDEX] < softwareVersion[MAJOR_INDEX]) {
            return true;
        }
        return false;
    }

    public  boolean isOlderVersion(String version) {
        return isOlderThanVersion(version, version());
    }

    public  boolean isOlderThanVersion(String checkVersion,
            String targetVersion) {
        
        if(noVersion(targetVersion) || noVersion(checkVersion)) {
            return false;
        }
        
        int[] checkVersions = parseVersion(checkVersion);
        int[] targetVersions = parseVersion(targetVersion);
        
        if (checkVersions[MAJOR_INDEX] < targetVersions[MAJOR_INDEX]) {
            return true;
        } else if (checkVersions[MAJOR_INDEX] == targetVersions[MAJOR_INDEX]
                && checkVersions[MINOR_INDEX] < targetVersions[MINOR_INDEX]) {
            return true;
        } else if (checkVersions[MAJOR_INDEX] == targetVersions[MAJOR_INDEX]
                && checkVersions[MINOR_INDEX] == targetVersions[MINOR_INDEX]
                && checkVersions[PATCH_INDEX] < targetVersions[PATCH_INDEX]) {
            return true;
        }
        return false;
    }
    
    private  boolean noVersion(String targetVersion) {
        return StringUtils.isBlank(targetVersion);
    }
}