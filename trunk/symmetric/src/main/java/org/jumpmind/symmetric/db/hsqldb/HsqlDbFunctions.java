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
package org.jumpmind.symmetric.db.hsqldb;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.db.EmbeddedDbFunctions;

public class HsqlDbFunctions extends EmbeddedDbFunctions {

    private static Map<String, ThreadLocal<String>> sessionVariables = new HashMap<String, ThreadLocal<String>>();

    public static void setSession(String key, String value) {
        ThreadLocal<String> local = sessionVariables.get(key);
        if (local == null) {
            local = new ThreadLocal<String>();
            sessionVariables.put(key, local);
        }
        if (value != null) {
            local.set(value);
        } else {
            local.remove();
        }
    }

    public static String getSession(String key) {
        ThreadLocal<String> local = sessionVariables.get(key);
        if (local != null) {
            return local.get();
        } else {
            return null;
        }
    }

}
