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

package org.jumpmind.symmetric.common;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

/**
 * Constants that represent SymmetricDS tables
 */
public class TableConstants {

    public static final String SYM_PARAMETER = "parameter";
    public static final String SYM_LOCK = "lock";    
    public static final String SYM_OUTGOING_BATCH = "outgoing_batch";
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
    public static final String SYM_TRANSFORM_COLUMN = "transform_column";
    public static final String SYM_TRIGGER_ROUTER = "trigger_router";
    public static final String SYM_CHANNEL = "channel";
    public static final String SYM_NODE_SECURITY = "node_security";
    public static final String SYM_NODE_IDENTITY = "node_identity";
    public static final String SYM_NODE_GROUP_LINK = "node_group_link";
    public static final String SYM_NODE_HOST_STATS = "node_host_stats";
    public static final String SYM_NODE_HOST_JOB_STATS = "node_host_job_stats";
    public static final String SYM_REGISTRATION_REQUEST = "registration_request";
    public static final String SYM_REGISTRATION_REDIRECT = "registration_redirect";
    public static final String SYM_NODE_CHANNEL_CTL = "node_channel_ctl";
    public static final String SYM_CONFLICT_SETTINGS = "conflict_setting";
    public static final String SYM_NODE_GROUP_CHANNEL_WINDOW = "node_group_channel_window";
    public static final String SYM_NODE_HOST_CHANNEL_STATS = "node_host_channel_stats";
    public static final String SYM_INCOMING_ERROR = "incoming_error";

    private static Set<String> tablesWithPrefix;
    
    private static Set<String> configTablesWithPrefix;
    
    private static Set<String> tablesWithoutPrefix;

    public static String[] NODE_TABLES = { SYM_NODE, SYM_NODE_SECURITY, SYM_NODE_IDENTITY };

    public static final Set<String> getTables(String tablePrefix) {
        if (tablesWithPrefix == null) {
            tablesWithPrefix = populateAllTables(tablePrefix);
        }
        return tablesWithPrefix;
    }
    
    public static final Set<String> getConfigTables(String tablePrefix) {
        if (configTablesWithPrefix == null) {
            configTablesWithPrefix = populateConfigTables(tablePrefix);
        }
        return configTablesWithPrefix;
    }
    
    public static final Set<String> getTablesWithoutPrefix() {
        if (tablesWithoutPrefix == null) {
            tablesWithoutPrefix = populateAllTables("");
        }
        return tablesWithoutPrefix;
    }
    
    protected static Set<String> populateConfigTables(String tablePrefix) {
        HashSet<String> configTables = new HashSet<String>();
        configTables.add(getTableName(tablePrefix,
                TableConstants.SYM_NODE_GROUP));
        configTables.add(getTableName(tablePrefix,
                TableConstants.SYM_NODE_GROUP_LINK));
        configTables.add(getTableName(tablePrefix,
                TableConstants.SYM_NODE));
        configTables.add(getTableName(tablePrefix,
                TableConstants.SYM_NODE_HOST));
        configTables.add(getTableName(tablePrefix,
                TableConstants.SYM_NODE_SECURITY));
        configTables.add(getTableName(tablePrefix,
                TableConstants.SYM_PARAMETER));
        configTables.add(getTableName(tablePrefix,
                TableConstants.SYM_CHANNEL));
        configTables.add(getTableName(tablePrefix,
                TableConstants.SYM_NODE_CHANNEL_CTL));
        configTables.add(getTableName(tablePrefix,
                TableConstants.SYM_NODE_GROUP_CHANNEL_WINDOW));
        configTables.add(getTableName(tablePrefix,
                TableConstants.SYM_TRIGGER));
        configTables.add(getTableName(tablePrefix,
                TableConstants.SYM_ROUTER));
        configTables.add(getTableName(tablePrefix,
                TableConstants.SYM_TRIGGER_ROUTER));
        configTables.add(getTableName(tablePrefix,
                TableConstants.SYM_TRANSFORM_TABLE));
        configTables.add(getTableName(tablePrefix,
                TableConstants.SYM_TRANSFORM_COLUMN));
        configTables.add(getTableName(tablePrefix,
                TableConstants.SYM_CONFLICT_SETTINGS));
        configTables.add(getTableName(tablePrefix,
                TableConstants.SYM_NODE_IDENTITY));
        return configTables;
    }

    protected static Set<String> populateAllTables(String tablePrefix) {
        HashSet<String> tables = new HashSet<String>();
        tables.add(getTableName(tablePrefix, SYM_TRIGGER));
        tables.add(getTableName(tablePrefix, SYM_TRIGGER_ROUTER));
        tables.add(getTableName(tablePrefix, SYM_ROUTER));
        tables.add(getTableName(tablePrefix, SYM_TRANSFORM_TABLE));
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
        tables.add(getTableName(tablePrefix, SYM_NODE_GROUP_CHANNEL_WINDOW));
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
        tables.add(getTableName(tablePrefix, SYM_LOCK));
        tables.add(getTableName(tablePrefix, SYM_CONFLICT_SETTINGS));
        tables.add(getTableName(tablePrefix, SYM_INCOMING_ERROR));
        return tables;
    }

    public static final Set<String> getTablesThatDoNotSync(String tablePrefix) {
        Set<String> tables = new HashSet<String>(2);
        tables.add(getTableName(tablePrefix, SYM_NODE_IDENTITY));
        tables.add(getTableName(tablePrefix, SYM_NODE_CHANNEL_CTL));
        return tables;
    }

    public static String getTableName(String tablePrefix, String tableSuffix) {
        return String.format("%s%s%s", tablePrefix, StringUtils.isNotBlank(tablePrefix) ? "_" : "",
                tableSuffix);
    }
}