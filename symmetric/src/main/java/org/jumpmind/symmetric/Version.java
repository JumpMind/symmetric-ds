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

import org.apache.commons.lang.StringUtils;

/**
 * Follow the Apache versioning scheme documented here:
 * http://apr.apache.org/versioning.html
 */
final public class Version {

    public static final int MAJOR = 1;

    public static final int MINOR = 1;

    public static final int PATCH = 0;

    public static final String VERSION = MAJOR + "." + MINOR + "." + PATCH;

    public static int[] parseVersion(String version) {
        int[] versions = new int[3];
        if (!StringUtils.isEmpty(version)) {
            String[] splitVersion = version.split("\\.");
            if (splitVersion.length >= 3) {
                versions[2] = Integer.parseInt(splitVersion[2]);
            }
            if (splitVersion.length >= 2) {
                versions[1] = Integer.parseInt(splitVersion[1]);
            }
            if (splitVersion.length >= 1) {
                versions[0] = Integer.parseInt(splitVersion[0]);
            }
        }
        return versions;
    }

}
