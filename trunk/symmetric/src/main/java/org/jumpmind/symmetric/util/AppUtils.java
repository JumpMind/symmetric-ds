/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.util;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.TimeZone;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.StandaloneSymmetricEngine;
import org.jumpmind.symmetric.SymmetricWebServer;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;

public class AppUtils {

    private static ILog log = LogFactory.getLog(AppUtils.class);

    private static String serverId;

    private static FastDateFormat timezoneFormatter = FastDateFormat
            .getInstance("Z");

    /**
     * Get a unique identifier that represents the JVM instance this server is
     * currently running in.
     */
    public static String getServerId() {
        if (StringUtils.isBlank(serverId)) {
            serverId = System.getProperty(
                    "runtime.symmetric.cluster.server.id", null);
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
        String hostName = System.getProperty("host.name", "unknown");
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (Exception ex) {
            log.warn(ex);
        }
        return hostName;
    }

    public static String getIpAddress() {
        String ipAddress = System.getProperty("ip.address", "unknown");
        try {
            ipAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (Exception ex) {
            log.warn(ex);
        }
        return ipAddress;
    }

    public static String replace(String prop, String replaceWith,
            String sourceString) {
        return StringUtils
                .replace(sourceString, "$(" + prop + ")", replaceWith);
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
     * Handy utility method to look up a SymmetricDS component given the bean
     * name.
     * 
     * @see Constants
     */
    @SuppressWarnings("unchecked")
    public static <T> T find(String name, ISymmetricEngine engine) {
        return (T) engine.getApplicationContext().getBean(name);
    }

    /**
     * @see #find(String, StandaloneSymmetricEngine)
     */
    @SuppressWarnings("unchecked")
    public static <T> T find(String name, SymmetricWebServer server) {
        return (T) server.getEngine().getApplicationContext().getBean(name);
    }

    /**
     * Use this method to create any needed temporary files for SymmetricDS.
     */
    public static File createTempFile(String token) throws IOException {
        return File.createTempFile("sym." + token + ".", ".tmp");
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
        int theirOffset = TimeZone.getTimeZone("GMT" + timezoneOffset)
                .getOffset(currentTime);
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
            log.warn("Message", e.getMessage());
        }
    }

    /**
     * Attempt to close all the connections to a database that a DataSource has.  This method 
     * should not be relied upon as it only works with certain {@link DataSource} implementations.
     */
    public static void resetDataSource(DataSource ds) {
        if (ds instanceof BasicDataSource) {
            BasicDataSource bds = (BasicDataSource) ds;
            try {
                bds.close();
            } catch (Exception ex) {
                log.warn(ex);
            }
        }
    }

}
