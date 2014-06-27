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
package org.jumpmind.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Enumeration;
import java.util.TimeZone;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.jumpmind.exception.IoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General application utility methods
 */
public class AppUtils {

    private static String UNKNOWN = "unknown";

    private static Logger log = LoggerFactory.getLogger(AppUtils.class);

    private static FastDateFormat timezoneFormatter = FastDateFormat.getInstance("Z");

    public static String getHostName() {
        String hostName = System.getProperty("host.name", UNKNOWN);
        if (UNKNOWN.equals(hostName)) {
            try {
                hostName = InetAddress.getLocalHost().getHostName();
            } catch (Exception ex) {
                log.warn(ex.getMessage(), ex);
            }
        }
        return hostName;
    }

    public static String getIpAddress() {
        String ipAddress = System.getProperty("ip.address", UNKNOWN);
        if (UNKNOWN.equals(ipAddress)) {
            try {
                Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                while (interfaces.hasMoreElements()) {
                    NetworkInterface networkInterface = interfaces.nextElement();
                    Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                    while (inetAddresses.hasMoreElements()) {
                        InetAddress inetAddress = inetAddresses.nextElement();
                        if (!inetAddress.isLoopbackAddress()) {
                            ipAddress = inetAddress.getHostAddress();
                        }
                    }
                }
            } catch (Exception ex) {
                log.warn(ex.getMessage(), ex);
            } finally {
            }
        }

        if (UNKNOWN.equals(ipAddress)) {
            try {
                ipAddress = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException ex) {
                log.warn(ex.getMessage(), ex);
                ipAddress = "127.0.0.1";
            }
        }
        return ipAddress;
    }

    /**
     * This method will return the timezone in RFC822 format. </p> The format
     * ("-+HH:MM") has advantages over the older timezone codes ("AAA"). The
     * difference of 5 hours from GMT is obvious with "-05:00" but only implied
     * with "EST". There is no ambiguity saying "-06:00", but you don't know if
     * "CST" means Central Standard Time ("-06:00") or China Standard Time
     * ("+08:00"). The timezone codes need to be loaded on the system, and
     * definitions are not standardized between systems. Therefore, to remain
     * agnostic to operating systems and databases, the RFC822 format is the
     * best choice.
     */
    public static String getTimezoneOffset() {
        String tz = timezoneFormatter.format(new Date());
        if (tz != null && tz.length() == 5) {
            return tz.substring(0, 3) + ":" + tz.substring(3, 5);
        }
        return null;
    }

    /**
     * @param timezoneOffset
     *            see description for {@link #getTimezoneOffset()}
     * @return a date object that represents the local date and time at the
     *         passed in offset
     */
    public static Date getLocalDateForOffset(String timezoneOffset) {
        long currentTime = System.currentTimeMillis();
        int myOffset = TimeZone.getDefault().getOffset(currentTime);
        int theirOffset = TimeZone.getTimeZone("GMT" + timezoneOffset).getOffset(currentTime);
        return new Date(currentTime - myOffset + theirOffset);
    }

    /**
     * Useful method to sleep that catches and ignores the
     * {@link InterruptedException}
     * 
     * @param ms
     *            milliseconds to sleep
     */
    public static void sleep(long ms) {
        if (ms > 0) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                log.warn("{}", e.getMessage());
            }
        }
    }

    public static boolean isSystemPropertySet(String propName, boolean defaultValue) {
        return "true"
                .equalsIgnoreCase(System.getProperty(propName, Boolean.toString(defaultValue)));
    }

    /**
     * Checks to see if a specific port is available.
     * 
     * @param port
     *            the port to check for availability
     */
    public static boolean isPortAvailable(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Invalid start port: " + port);
        }

        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }

        return false;
    }
    
    public static void unzip(InputStream in, File toDir) {
        try {
            ZipInputStream is = new ZipInputStream(in);
            ZipEntry entry = null;
            do {
                entry = is.getNextEntry();
                if (entry != null) {
                    if (entry.isDirectory()) {
                        File dir = new File(toDir, entry.getName());
                        dir.mkdirs();
                        dir.setLastModified(entry.getTime());
                    } else {
                        File file = new File(toDir, entry.getName());
                        if (!file.getParentFile().exists()) {
                            file.getParentFile().mkdirs();
                            file.getParentFile().setLastModified(entry.getTime());
                        }
                        FileOutputStream fos = new FileOutputStream(file);
                        try {
                            IOUtils.copy(is, fos);
                            file.setLastModified(entry.getTime());
                        } finally {
                            IOUtils.closeQuietly(fos);
                        }
                    }
                }
            } while (entry != null);
        } catch (IOException e) {
            throw new IoException(e);
        }

    }

}
