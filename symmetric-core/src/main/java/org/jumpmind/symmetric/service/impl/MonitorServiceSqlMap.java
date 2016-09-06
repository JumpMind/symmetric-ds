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
package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class MonitorServiceSqlMap extends AbstractSqlMap {

    public MonitorServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        // Monitors
        
        putSql("selectMonitorSql", 
                "select monitor_id, external_id, node_group_id, type, expression, enabled, threshold, run_period, run_count, " +
                "severity_level, create_time, last_update_by, last_update_time from $(monitor)");

        putSql("whereMonitorByNodeSql",
                "where (node_group_id = ? or node_group_id = 'ALL') and (external_id = ? or external_id = 'ALL') and enabled = 1");

        putSql("insertMonitorSql",
                "insert into $(monitor) " +
                "(monitor_id, external_id, node_group_id, type, expression, enabled, threshold, run_period, run_count, severity_level, " +
                "create_time, last_update_by, last_update_time) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        putSql("updateMonitorSql",
                "update $(monitor) " +
                "set external_id = ?, node_group_id = ?, type = ?, expression = ?, enabled = ?, threshold = ?, run_period = ?, run_count = ?," +
                " severity_level = ?, last_update_by = ?, last_update_time = ? where monitor_id = ?");

        putSql("deleteMonitorSql",
                "delete from $(monitor) where monitor_id = ?");

        // Monitor Events
        
        putSql("selectMonitorEventSql",
                "select monitor_id, node_id, event_time, type, event_value, threshold, severity_level, host_name, is_notified " +
                "from $(monitor_event) ");

        putSql("whereMonitorEventFilteredSql", "where severity_level >= ?");

        putSql("whereMonitorEventForNotificationBySeveritySql",
                "where is_notified = 0 and severity_level >= ?");

        putSql("insertMonitorEventSql",
                "insert into $(monitor_event) " +
                "(monitor_id, node_id, event_time, host_name, type, event_value, threshold, severity_level, is_notified) " + 
                "values (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        
        putSql("updateMonitorEventNotifiedSql",
                "update $(monitor_event) set is_notified = 1 where monitor_id = ? and node_id = ? and event_time = ?");

        putSql("deleteMonitorEventSql",
                "delete from $(monitor_event) where monitor_id = ? and node_id = ? and event_time = ?");

        // Notifications
        
        putSql("selectNotificationSql",
                "select notification_id, node_group_id, external_id, severity_level, type, expression, enabled, create_time, " +
                "last_update_by, last_update_time " +
                "from $(notification)");
        
        putSql("whereNotificationByNodeSql",
                "where (node_group_id = ? or node_group_id = 'ALL') and (external_id = ? or external_id = 'ALL') and enabled = 1");
                
        putSql("insertNotificationSql",
                "insert into $(notification) " +
                "(notification_id, node_group_id, external_id, severity_level, type, expression, enabled, create_time, " +
                "last_update_by, last_update_time) " +
                "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        
        putSql("updateNotificationSql",
                "update $(notification) " +
                "set node_group_id = ?, external_id = ?, severity_level = ?, type = ?, expression = ?, enabled = ?, create_time = ?, " +
                "last_update_by = ?, last_update_time = ? where notification_id = ?");
        
        putSql("deleteNotificationSql",
                "delete from $(notification) where notification_id = ?");

    }
}