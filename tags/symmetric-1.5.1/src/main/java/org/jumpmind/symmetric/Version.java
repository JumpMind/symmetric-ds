/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>
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

package org.jumpmind.symmetric;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

/**
 * Follow the Apache versioning scheme documented <a
 * href="http://apr.apache.org/versioning.html">here</a>.
 */
final public class Version {

    static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(Version.class);

    private static final int MAJOR_INDEX = 0;

    private static final int MINOR_INDEX = 1;

    private static final int PATCH_INDEX = 2;

    public static String version() {
        InputStream is = Version.class
                .getResourceAsStream("/META-INF/maven/org.jumpmind.symmetric/symmetric/pom.properties");
        if (is != null) {
            Properties p = new Properties();
            try {
                p.load(is);
                return p.getProperty("version");
            } catch (IOException e) {
                log.warn(e, e);
            }
        }
        return "development";
    }

    public static int[] parseVersion(String version) {
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

    private static int parseVersionComponent(String versionComponent) {
        int version = 0;
        try {
            version = Integer.parseInt(versionComponent);
        } catch (NumberFormatException e) {
        }
        return version;
    }

    public static boolean isOlderMajorVersion(String version) {
        return isOlderMajorVersion(parseVersion(version));
    }

    public static boolean isOlderMajorVersion(int[] versions) {
        int[] softwareVersion = parseVersion(version());
        if (versions[MAJOR_INDEX] < softwareVersion[MAJOR_INDEX]) {
            return true;
        }
        return false;
    }

    public static boolean isOlderMinorVersion(String version) {
        return isOlderMinorVersion(parseVersion(version));
    }

    public static boolean isOlderMinorVersion(int[] versions) {
        int[] softwareVersion = parseVersion(version());
        if (versions[0] < softwareVersion[MAJOR_INDEX]) {
            return true;
        } else if (versions[MAJOR_INDEX] == softwareVersion[MAJOR_INDEX]
                && versions[MINOR_INDEX] < softwareVersion[MINOR_INDEX]) {
            return true;
        }
        return false;
    }
}
