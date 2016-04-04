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

public class NotificationServiceSqlMap extends AbstractSqlMap {

    public NotificationServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        putSql("selectNotificationSql", 
                "select notification_id, external_id, node_group_id, type, enabled, threshold, period, sample_minutes, severity_level, window_minutes, " + 
                "create_time, last_update_by, last_update_time from $(notification)");

        putSql("insertNotificationSql",
                "insert into $(notification) " +
                "(notification_id, external_id, node_group_id, type, enabled, threshold, period, sample_minutes, severity_level, window_minutes, " + 
                "create_time, last_update_by, last_update_time) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        putSql("updateNotificationSql",
                "update $(notification) " +
                "set external_id = ?, node_group_id = ?, type = ?, enabled = ?, threshold = ?, period = ?, sample_minutes = ?, severity_level = ?, " +
                "window_minutes = ?, last_update_by = ?, last_update_time = ? where notification_id = ?");

        putSql("deleteNotificationSql",
                "delete from $(notification) where notification_id = ?");

    }
}