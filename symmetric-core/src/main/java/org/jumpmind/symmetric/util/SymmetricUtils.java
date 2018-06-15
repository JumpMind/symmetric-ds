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
package org.jumpmind.symmetric.util;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.util.AppUtils;
import org.jumpmind.util.CollectionUtils;
import org.jumpmind.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final public class SymmetricUtils {

    protected static final Logger log = LoggerFactory.getLogger(SymmetricUtils.class);

    protected static boolean isJava7 = true;
    
    protected static Method copyMethod;
    
    protected static Method fileMethod; 
    
    protected static Object optionArray;
    
    protected static boolean isNoticeLogged;

    private SymmetricUtils() {
    }
    
    public static String quote(ISymmetricDialect symmetricDialect, String name) {
        String quote = symmetricDialect.getPlatform().getDatabaseInfo().getDelimiterToken();
        if (StringUtils.isNotBlank(quote)) {
            return quote + name + quote;
        } else {
            return name;
        }
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void copyFile(File source, File target) throws IOException {
        if (isJava7) {
            try {
                if (copyMethod == null) {
                    Class filesClass = Class.forName("java.nio.file.Files");
                    Class pathClass = Class.forName("java.nio.file.Path");
                    Class optionArrayClass = Class.forName("[Ljava.nio.file.CopyOption;");
                    Class optionClass = Class.forName("java.nio.file.CopyOption");
                    Class standardOptionClass = Class.forName("java.nio.file.StandardCopyOption");
        
                    copyMethod = filesClass.getMethod("copy", new Class[] { pathClass, pathClass, optionArrayClass });
                    fileMethod = File.class.getMethod("toPath", (Class[]) null);
                    optionArray = Array.newInstance(optionClass, 1);
                    Array.set(optionArray, 0, Enum.valueOf(standardOptionClass, "REPLACE_EXISTING"));
                }

                Object sourcePath = fileMethod.invoke(source, (Object[]) null);
                Object targetPath = fileMethod.invoke(target, (Object[]) null);
                copyMethod.invoke(null, new Object[] { sourcePath, targetPath, optionArray });
                return;
            } catch (Exception e) {
                isJava7 = false;
            }
        }
        FileUtils.copyFile(source, target);
    }
    
    public static final void replaceSystemAndEnvironmentVariables(Properties properties) {
        Set<Object> keys = new HashSet<Object>(properties.keySet());
        Map<String, String> env = System.getenv();
        Map<String, String> systemProperties = CollectionUtils.toMap(System.getProperties());
        for (Object object : keys) {
            String value = properties.getProperty((String)object);
            if (isNotBlank(value)) {
                value = FormatUtils.replaceTokens(value, env, true);
                value = FormatUtils.replaceTokens(value, systemProperties, true);
                if (value.contains("hostName")) {
                    value = FormatUtils.replace("hostName", AppUtils.getHostName(), value);
                }
                if (value.contains("portNumber")) {
                    value = FormatUtils.replace("portNumber", AppUtils.getPortNumber(), value);
                }
                if (value.contains("ipAddress")) {
                    value = FormatUtils.replace("ipAddress", AppUtils.getIpAddress(), value);
                }
                if (value.contains("engineName")) {
                    value = FormatUtils.replace("engineName", properties.getProperty(ParameterConstants.ENGINE_NAME, ""), value);
                }
                if (value.contains("nodeGroupId")) {
                    value = FormatUtils.replace("nodeGroupId", properties.getProperty(ParameterConstants.NODE_GROUP_ID, ""), value);
                }
                if (value.contains("externalId")) {
                    value = FormatUtils.replace("externalId", properties.getProperty(ParameterConstants.EXTERNAL_ID, ""), value);
                }
                if (value.contains("syncUrl")) {
                    value = FormatUtils.replace("syncUrl", properties.getProperty(ParameterConstants.SYNC_URL, ""), value);
                }
                if (value.contains("registrationUrl")) {
                    value = FormatUtils.replace("registrationUrl", properties.getProperty(ParameterConstants.REGISTRATION_URL, ""), value);
                }
                properties.put(object, value);
            }                        
        }        
    }
    
    public static void logNotices() {
        synchronized (SymmetricUtils.class) {
            if (isNoticeLogged) {
                return;
            }
            isNoticeLogged = true;
        }

        String notices = null;
        try {            
            notices = String.format("%n%s%n", IOUtils.toString(Thread.currentThread().getContextClassLoader().getResource("symmetricds.asciiart")));
        } catch (Exception ex) {
            notices = String.format("SymmetricDS Start%n");
        }

        String buildTime = Long.toString(Version.getBuildTime());
        String year = null;
        if (buildTime.length() >= 4) {
            year = buildTime.substring(0, 4);
        } else {
            year = new SimpleDateFormat("yyyy").format(new Date());
        }

        int pad = 65;
        notices += String.format(
                "+" + StringUtils.repeat("-",  pad) + "+%n" +
                "|" + StringUtils.rightPad(" Copyright (C) 2007-" + year + " JumpMind, Inc.", pad) + "|%n" +
                "|" + StringUtils.repeat(" ", pad) + "|%n");
        
        InputStream in = null;
        try {
            in = AppUtils.class.getResourceAsStream("/symmetric-console-default.properties");
            if (in != null) {
                in.close();
            }
        } catch (Exception e) {
        }

        if (in != null) {
            notices += String.format(
                    "|" + StringUtils.rightPad(" Licensed under one or more agreements from JumpMind, Inc.", pad) + "|%n" +
                    "|" + StringUtils.rightPad(" See doc/license.html", pad) + "|%n");
        } else {
            notices += String.format(
                    "|" + StringUtils.rightPad(" Licensed under the GNU General Public License version 3.", pad) + "|%n" +
                    "|" + StringUtils.rightPad(" This software comes with ABSOLUTELY NO WARRANTY.", pad) + "|%n" +
                    "|" + StringUtils.rightPad(" See http://www.gnu.org/licenses/gpl.html", pad) + "|%n");
        }

        notices += "+" + StringUtils.repeat("-",  pad) + "+";
        log.info(notices);
    }

}
