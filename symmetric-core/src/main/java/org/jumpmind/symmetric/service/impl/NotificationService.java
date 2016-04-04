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

import java.util.List;
import java.util.Map;

import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Notification;
import org.jumpmind.symmetric.model.NotificationEvent;
import org.jumpmind.symmetric.notification.INotificationCheck;
import org.jumpmind.symmetric.notification.NotificationCheckCpu;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.INotificationService;
import org.jumpmind.symmetric.service.IParameterService;

public class NotificationService extends AbstractService implements INotificationService {

    protected IExtensionService extensionService;
    
    public NotificationService(IParameterService parameterService, ISymmetricDialect symmetricDialect, IExtensionService extensionService) {
        super(parameterService, symmetricDialect);
        setSqlMap(new NotificationServiceSqlMap(symmetricDialect.getPlatform(), createSqlReplacementTokens()));

        this.extensionService = extensionService;
        
        extensionService.addExtensionPoint("cpu", new NotificationCheckCpu());
    }
    
    @Override
    public synchronized void update() {
        Map<String, INotificationCheck> notificationChecks = extensionService.getExtensionPointMap(INotificationCheck.class);
        // TODO: cache notifications until cleared by ConfigurationChangedDataRouter
        List<Notification> notifications = getNotifications();

        for (Notification notification : notifications) {
            if (notification.isEnabled()) {
                INotificationCheck notificationCheck = notificationChecks.get(notification.getType());
                if (notificationCheck != null) {
                    long value = notificationCheck.check(notification);
                    
                    if (notificationCheck.requiresPeriod()) {
                        // TODO: accumulate average over period, then check threshold
                        
                    } else if (value >= notification.getThreshold()) {
                        // TODO: record sym_notification_event
                    }
                } else {
                    log.warn("Could not find notification of type '" + notification.getType() + "'");
                }
            }
        }
        
        // TODO: for each sym_notification_action, see if sym_notification_events exist to act upon 
    }

    @Override
    public List<Notification> getNotifications() {
        return sqlTemplate.query(getSql("selectNotificationSql"), new NotificationRowMapper());
    }

    @Override
    public List<NotificationEvent> getNotificationEvents() {
        return sqlTemplate.query(getSql("selectNotificationEventSql"), new NotificationEventRowMapper());
    }

    @Override
    public void deleteNotification(String notificationId) {
        sqlTemplate.update(getSql("deleteNotificationSql"), notificationId);
    }

    @Override
    public void saveNotification(Notification notification) {
        int count = sqlTemplate.update(getSql("updateNotificationSql"), notification.getExternalId(), notification.getNodeGroupId(),
                notification.getType(), notification.isEnabled(), notification.getThreshold(), notification.getPeriod(), notification.getSampleMinutes(),
                notification.getSeverityLevel(), notification.getWindowMinutes(), notification.getLastUpdateBy(), 
                notification.getLastUpdateTime(), notification.getNotificationId());
        if (count == 0) {
            sqlTemplate.update(getSql("insertNotificationSql"), notification.getNotificationId(), notification.getExternalId(), notification.getNodeGroupId(),
                    notification.getType(), notification.isEnabled(), notification.getThreshold(), notification.getPeriod(), notification.getSampleMinutes(),
                    notification.getSeverityLevel(), notification.getWindowMinutes(), notification.getCreateTime(), notification.getLastUpdateBy(), 
                    notification.getLastUpdateTime());
        }
    }

    class NotificationRowMapper implements ISqlRowMapper<Notification> {
        public Notification mapRow(Row row) {
            Notification n = new Notification();
            n.setNotificationId(row.getString("notification_id"));
            n.setExternalId(row.getString("external_id"));
            n.setNodeGroupId(row.getString("node_group_id"));
            n.setType(row.getString("type"));
            n.setEnabled(row.getBoolean("enabled"));
            n.setThreshold(row.getLong("threshold"));
            n.setPeriod(row.getInt("period"));
            n.setSampleMinutes(row.getInt("sample_minutes"));
            n.setSeverityLevel(row.getInt("severity_level"));
            n.setWindowMinutes(row.getInt("window_minutes"));
            n.setCreateTime(row.getDateTime("create_time"));
            n.setLastUpdateBy(row.getString("last_update_by"));
            n.setLastUpdateTime(row.getDateTime("last_update_time"));
            return n;
        }
    }
    
    class NotificationEventRowMapper implements ISqlRowMapper<NotificationEvent> {
        public NotificationEvent mapRow(Row row) {
            NotificationEvent n = new NotificationEvent();
            n.setNotificationId(row.getString("notification_id"));
            n.setNodeId(row.getString("node_id"));
            n.setHostName(row.getString("host_name"));
            n.setEventTime(row.getTime("event_time"));
            n.setValue(row.getLong("value"));
            n.setThreshold(row.getLong("threshold"));
            n.setPeriod(row.getLong("period"));
            n.setSeverityLevel(row.getInt("severity_level"));
            return n;
        }
    }
}
