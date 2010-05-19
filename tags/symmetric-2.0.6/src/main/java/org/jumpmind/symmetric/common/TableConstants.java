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
package org.jumpmind.symmetric.common;

import java.util.HashSet;
import java.util.Set;

public class TableConstants {

    public static final String SYM_TRIGGER = "trigger";
    public static final String SYM_TRIGGER_ROUTER = "trigger_router";
    public static final String SYM_ROUTER = "router";
    public static final String SYM_TRIGGER_HIST = "trigger_hist";
    public static final String SYM_NODE = "node";
    public static final String SYM_NODE_HOST = "node_host";
    public static final String SYM_NODE_SECURITY = "node_security";
    public static final String SYM_NODE_IDENTITY = "node_identity";
    public static final String SYM_NODE_CHANNEL_CTL = "node_channel_ctl";
    public static final String SYM_CHANNEL = "channel";
    public static final String SYM_NODE_GROUP = "node_group";
    public static final String SYM_NODE_GROUP_LINK = "node_group_link";

    public static String[] NODE_TABLES = { SYM_NODE, SYM_NODE_SECURITY, SYM_NODE_IDENTITY };

    public static final Set<String> getNodeTablesAsSet(String tablePrefix) {
        Set<String> tables = new HashSet<String>();
        tables.add(getTableName(tablePrefix, SYM_NODE));
        tables.add(getTableName(tablePrefix, SYM_NODE_SECURITY));
        tables.add(getTableName(tablePrefix, SYM_NODE_IDENTITY));
        tables.add(getTableName(tablePrefix, SYM_NODE_CHANNEL_CTL));
        return tables;
    }

    public static String getTableName(String tablePrefix, String tableSuffix) {
        return String.format("%s_%s", tablePrefix, tableSuffix);
    }
}
