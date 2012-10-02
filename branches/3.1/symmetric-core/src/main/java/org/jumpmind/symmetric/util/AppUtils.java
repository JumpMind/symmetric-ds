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
package org.jumpmind.symmetric.util;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Enumeration;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bsh.EvalError;
import bsh.Interpreter;

/**
 * General application utility methods
 */
public class AppUtils {

    private static String UNKNOWN = "unknown";

    private static Logger log = LoggerFactory.getLogger(AppUtils.class);

    private static String serverId;

    private static FastDateFormat timezoneFormatter = FastDateFormat.getInstance("Z");

    /**
     * Get a unique identifier that represents the JVM instance this server is
     * currently running in.
     */
    public static String getServerId() {
        if (StringUtils.isBlank(serverId)) {
            serverId = System.getProperty("runtime.symmetric.cluster.server.id", null);
            if (StringUtils.isBlank(serverId)) {
                // JBoss uses this system property to identify a server in a
                // cluster
                serverId = System.getProperty("bind.address", null);
                if (StringUtils.isBlank(serverId)) {
                    try {
                        serverId = getHostName();
                    } catch (Exception ex) {
                        serverId = "unknown";
                    }
                }
            }
        }
        return serverId;
    }

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
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            log.warn("{}", e.getMessage());
        }
    }

    public static boolean isSystemPropertySet(String propName, boolean defaultValue) {
        return "true"
                .equalsIgnoreCase(System.getProperty(propName, Boolean.toString(defaultValue)));
    }

    public static void runBsh(Map<String, Object> variables, String script) {
        try {
            Interpreter interpreter = new Interpreter();
            if (variables != null) {
                for (String variableName : variables.keySet()) {
                    interpreter.set(variableName, variables.get(variableName));
                }
            }
            log.info("{}", "About to run: \n" + script);
            interpreter.eval(script);
        } catch (EvalError e) {
            throw new RuntimeException(e);
        }
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

}
