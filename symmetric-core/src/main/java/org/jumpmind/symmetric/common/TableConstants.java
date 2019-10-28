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
package org.jumpmind.symmetric.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

/**
 * Constants that represent SymmetricDS tables
 */
public class TableConstants {

    public static final String SYM_PARAMETER = "parameter";
    public static final String SYM_LOCK = "lock";
    public static final String SYM_OUTGOING_BATCH = "outgoing_batch";
    public static final String SYM_EXTRACT_REQUEST = "extract_request";
    public static final String SYM_INCOMING_BATCH = "incoming_batch";
    public static final String SYM_TRIGGER = "trigger";
    public static final String SYM_ROUTER = "router";
    public static final String SYM_TRIGGER_HIST = "trigger_hist";
    public static final String SYM_NODE_GROUP = "node_group";
    public static final String SYM_NODE = "node";
    public static final String SYM_NODE_HOST = "node_host";
    public static final String SYM_DATA = "data";
    public static final String SYM_DATA_GAP = "data_gap";
    public static final String SYM_DATA_EVENT = "data_event";
    public static final String SYM_TRANSFORM_TABLE = "transform_table";
    public static final String SYM_LOAD_FILTER = "load_filter";
    public static final String SYM_TRANSFORM_COLUMN = "transform_column";
    public static final String SYM_TRIGGER_ROUTER = "trigger_router";
    public static final String SYM_CHANNEL = "channel";
    public static final String SYM_NODE_SECURITY = "node_security";
    public static final String SYM_NODE_IDENTITY = "node_identity";
    public static final String SYM_NODE_COMMUNICATION = "node_communication";
    public static final String SYM_NODE_GROUP_LINK = "node_group_link";
    public static final String SYM_NODE_HOST_STATS = "node_host_stats";
    public static final String SYM_NODE_HOST_JOB_STATS = "node_host_job_stats";
    public static final String SYM_REGISTRATION_REQUEST = "registration_request";
    public static final String SYM_REGISTRATION_REDIRECT = "registration_redirect";
    public static final String SYM_NODE_CHANNEL_CTL = "node_channel_ctl";
    public static final String SYM_CONFLICT = "conflict";
    public static final String SYM_NODE_GROUP_CHANNEL_WND = "node_group_channel_wnd";
    public static final String SYM_NODE_HOST_CHANNEL_STATS = "node_host_channel_stats";
    public static final String SYM_INCOMING_ERROR = "incoming_error";
    public static final String SYM_SEQUENCE = "sequence";
    public static final String SYM_TABLE_RELOAD_REQUEST = "table_reload_request";
    public static final String SYM_GROUPLET = "grouplet";
    public static final String SYM_GROUPLET_LINK = "grouplet_link";
    public static final String SYM_TRIGGER_ROUTER_GROUPLET = "trigger_router_grouplet";
    public static final String SYM_FILE_TRIGGER = "file_trigger";
    public static final String SYM_FILE_TRIGGER_ROUTER = "file_trigger_router";
    public static final String SYM_FILE_SNAPSHOT = "file_snapshot";
    public static final String SYM_FILE_INCOMING = "file_incoming";
    public static final String SYM_CONSOLE_USER = "console_user";
    public static final String SYM_CONSOLE_USER_HIST = "console_user_hist";
    public static final String SYM_CONSOLE_EVENT = "console_event";
    public static final String SYM_EXTENSION = "extension";
    public static final String SYM_MONITOR = "monitor";
    public static final String SYM_MONITOR_EVENT = "monitor_event";
    public static final String SYM_NOTIFICATION = "notification";
    public static final String SYM_CONTEXT = "context";
    public static final String SYM_JOB = "job";

    private static List<String> tablesWithPrefix;

    private static List<String> configTablesWithPrefix;

    private static List<String> tablesWithoutPrefix;

    public static final List<String> getTables(String tablePrefix) {
        if (tablesWithPrefix == null) {
            tablesWithPrefix = populateAllTables(tablePrefix);
        }
        return new ArrayList<String>(tablesWithPrefix);
    }

    public static final List<String> getConfigTables(String tablePrefix) {
        if (configTablesWithPrefix == null) {
            configTablesWithPrefix = populateConfigTables(tablePrefix);
        }
        return new ArrayList<String>(configTablesWithPrefix);
    }

    public static final List<String> getTablesWithoutPrefix() {
        if (tablesWithoutPrefix == null) {
            tablesWithoutPrefix = populateAllTables("");
        }
        return tablesWithoutPrefix;
    }

    protected static List<String> populateConfigTables(String tablePrefix) {
        List<String> configTables = new ArrayList<String>();
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_NODE_GROUP));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_NODE_GROUP_LINK));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_NODE));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_NODE_HOST));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_NODE_SECURITY));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_PARAMETER));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_CHANNEL));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_NODE_GROUP_CHANNEL_WND));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_TRIGGER));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_ROUTER));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_TRIGGER_ROUTER));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_TRANSFORM_TABLE));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_LOAD_FILTER));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_TRANSFORM_COLUMN));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_CONFLICT));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_TABLE_RELOAD_REQUEST));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_GROUPLET));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_GROUPLET_LINK));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_TRIGGER_ROUTER_GROUPLET));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_FILE_TRIGGER));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_FILE_TRIGGER_ROUTER));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_FILE_SNAPSHOT));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_NODE_IDENTITY));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_EXTENSION));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_MONITOR));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_MONITOR_EVENT));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_NOTIFICATION));
        configTables.add(getTableName(tablePrefix, TableConstants.SYM_JOB));
        return configTables;
    }

    protected static List<String> populateAllTables(String tablePrefix) {
        List<String> tables = new ArrayList<String>();
        tables.add(getTableName(tablePrefix, SYM_TRIGGER));
        tables.add(getTableName(tablePrefix, SYM_TRIGGER_ROUTER));
        tables.add(getTableName(tablePrefix, SYM_ROUTER));
        tables.add(getTableName(tablePrefix, SYM_TRANSFORM_TABLE));
        tables.add(getTableName(tablePrefix, SYM_LOAD_FILTER));
        tables.add(getTableName(tablePrefix, SYM_TRANSFORM_COLUMN));
        tables.add(getTableName(tablePrefix, SYM_TRIGGER_HIST));
        tables.add(getTableName(tablePrefix, SYM_CHANNEL));
        tables.add(getTableName(tablePrefix, SYM_NODE_GROUP));
        tables.add(getTableName(tablePrefix, SYM_NODE_GROUP_LINK));
        tables.add(getTableName(tablePrefix, SYM_NODE));
        tables.add(getTableName(tablePrefix, SYM_NODE_HOST));
        tables.add(getTableName(tablePrefix, SYM_NODE_SECURITY));
        tables.add(getTableName(tablePrefix, SYM_NODE_IDENTITY));
        tables.add(getTableName(tablePrefix, SYM_NODE_SECURITY));
        tables.add(getTableName(tablePrefix, SYM_NODE_CHANNEL_CTL));
        tables.add(getTableName(tablePrefix, SYM_NODE_GROUP_CHANNEL_WND));
        tables.add(getTableName(tablePrefix, SYM_PARAMETER));
        tables.add(getTableName(tablePrefix, SYM_NODE_HOST_CHANNEL_STATS));
        tables.add(getTableName(tablePrefix, SYM_NODE_HOST_STATS));
        tables.add(getTableName(tablePrefix, SYM_NODE_HOST_JOB_STATS));
        tables.add(getTableName(tablePrefix, SYM_REGISTRATION_REDIRECT));
        tables.add(getTableName(tablePrefix, SYM_REGISTRATION_REQUEST));
        tables.add(getTableName(tablePrefix, SYM_DATA));
        tables.add(getTableName(tablePrefix, SYM_DATA_GAP));
        tables.add(getTableName(tablePrefix, SYM_DATA_EVENT));
        tables.add(getTableName(tablePrefix, SYM_OUTGOING_BATCH));
        tables.add(getTableName(tablePrefix, SYM_INCOMING_BATCH));
        tables.add(getTableName(tablePrefix, SYM_EXTRACT_REQUEST));
        tables.add(getTableName(tablePrefix, SYM_LOCK));
        tables.add(getTableName(tablePrefix, SYM_CONFLICT));
        tables.add(getTableName(tablePrefix, SYM_INCOMING_ERROR));
        tables.add(getTableName(tablePrefix, SYM_SEQUENCE));
        tables.add(getTableName(tablePrefix, SYM_NODE_COMMUNICATION));
        tables.add(getTableName(tablePrefix, SYM_TABLE_RELOAD_REQUEST));
        tables.add(getTableName(tablePrefix, SYM_GROUPLET));
        tables.add(getTableName(tablePrefix, SYM_GROUPLET_LINK));
        tables.add(getTableName(tablePrefix, SYM_TRIGGER_ROUTER_GROUPLET));
        tables.add(getTableName(tablePrefix, TableConstants.SYM_FILE_TRIGGER));
        tables.add(getTableName(tablePrefix, TableConstants.SYM_FILE_TRIGGER_ROUTER));
        tables.add(getTableName(tablePrefix, TableConstants.SYM_FILE_SNAPSHOT));
        tables.add(getTableName(tablePrefix, TableConstants.SYM_FILE_INCOMING));
        tables.add(getTableName(tablePrefix, SYM_CONSOLE_USER));
        tables.add(getTableName(tablePrefix, SYM_CONSOLE_USER_HIST));
        tables.add(getTableName(tablePrefix, SYM_CONSOLE_EVENT));
        tables.add(getTableName(tablePrefix, SYM_EXTENSION));
        tables.add(getTableName(tablePrefix, SYM_MONITOR));
        tables.add(getTableName(tablePrefix, SYM_MONITOR_EVENT));
        tables.add(getTableName(tablePrefix, SYM_NOTIFICATION));
        tables.add(getTableName(tablePrefix, SYM_CONTEXT));
        tables.add(getTableName(tablePrefix, SYM_JOB));
        
        return tables;
    }

    public static final List<String> getTablesThatSync(String tablePrefix) {
        List<String> tables = new ArrayList<String>(getConfigTables(tablePrefix));
        tables.removeAll(getTablesThatDoNotSync(tablePrefix));
        return tables;
    }

    public static final List<String> getTablesThatDoNotSync(String tablePrefix) {
        List<String> tables = new ArrayList<String>(2);
        tables.add(getTableName(tablePrefix, SYM_NODE_IDENTITY));
        tables.add(getTableName(tablePrefix, SYM_NODE_CHANNEL_CTL));
        tables.add(getTableName(tablePrefix, SYM_CONSOLE_EVENT));
        return tables;
    }

    public static String getTableName(String tablePrefix, String tableSuffix) {
        return String.format("%s%s%s", tablePrefix, StringUtils.isNotBlank(tablePrefix) ? "_" : "",
                tableSuffix);
    }
}