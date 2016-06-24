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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.common.ContextConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Notification;
import org.jumpmind.symmetric.monitor.IMonitorType;
import org.jumpmind.symmetric.monitor.MonitorTypeBatchError;
import org.jumpmind.symmetric.monitor.MonitorTypeBatchUnsent;
import org.jumpmind.symmetric.monitor.MonitorTypeCpu;
import org.jumpmind.symmetric.monitor.MonitorTypeDataGap;
import org.jumpmind.symmetric.monitor.MonitorTypeDisk;
import org.jumpmind.symmetric.monitor.MonitorTypeMemory;
import org.jumpmind.symmetric.monitor.MonitorTypeUnrouted;
import org.jumpmind.symmetric.notification.INotificationType;
import org.jumpmind.symmetric.notification.NotificationTypeEmail;
import org.jumpmind.symmetric.notification.NotificationTypeLog;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IContextService;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.IMonitorService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;

public class MonitorService extends AbstractService implements IMonitorService {
    
    protected String hostName;
    
    protected INodeService nodeService;
    
    protected IExtensionService extensionService;
    
    protected IClusterService clusterService;
    
    protected IContextService contextService;

    protected long lastChecktime;
    
    protected Map<String, List<Long>> averagesByType = new HashMap<String, List<Long>>();

    public MonitorService(IParameterService parameterService, ISymmetricDialect symmetricDialect, INodeService nodeService,
            IExtensionService extensionService, IClusterService clusterService, IContextService contextService) {
        super(parameterService, symmetricDialect);
        setSqlMap(new MonitorServiceSqlMap(symmetricDialect.getPlatform(), createSqlReplacementTokens()));

        this.nodeService = nodeService;
        this.extensionService = extensionService;
        this.clusterService = clusterService;
        this.contextService = contextService;
        //hostName = AppUtils.getHostName();
        hostName = symmetricDialect.getEngineName();
        
        IMonitorType monitorExtensions[] = { new MonitorTypeBatchError(), new MonitorTypeBatchUnsent(), new MonitorTypeCpu(), 
                new MonitorTypeDataGap(), new MonitorTypeDisk(), new MonitorTypeMemory(), new MonitorTypeUnrouted() };
        for (IMonitorType ext : monitorExtensions) {
            extensionService.addExtensionPoint(ext.getName(), ext);    
        }
        
        INotificationType notificationExtensions[] = { new NotificationTypeLog(), new NotificationTypeEmail() };
        for (INotificationType ext : notificationExtensions) {
            extensionService.addExtensionPoint(ext.getName(), ext);    
        }
    }
    
    @Override
    public synchronized void update() {
        Map<String, IMonitorType> monitorTypes = extensionService.getExtensionPointMap(IMonitorType.class);
        // TODO: cache notifications until cleared by ConfigurationChangedDataRouter
        Node identity = nodeService.findIdentity();
        List<Monitor> activeMonitors = getActiveMonitorsForNode(identity.getNodeGroupId(), identity.getExternalId());

        for (Monitor monitor : activeMonitors) {
            IMonitorType monitorType = monitorTypes.get(monitor.getType());
            if (monitorType != null) {
                if ((System.currentTimeMillis() - lastChecktime) / 60000 > monitor.getRunPeriod()) {
                    updateMonitor(monitor, monitorType, identity);
                } else {
                    System.out.println("Not time to run " + monitorType.getName() + " yet");
                }
            } else {
                log.warn("Could not find monitor of type '" + monitor.getType() + "'");
            }
        }
        
        if (clusterService.lock(ClusterConstants.MONITOR)) {
            // TODO: using lock last time would avoid a database access on a non-cluster
            long clusterLastCheckTime = contextService.getLong(ContextConstants.NOTIFICATION_LAST_CHECK_TIME);
            
            for (Monitor monitor : activeMonitors) {
                IMonitorType monitorType = monitorTypes.get(monitor.getType());
                if (monitorType != null && monitorType.requiresClusterLock() && 
                        (System.currentTimeMillis() - clusterLastCheckTime) / 60000 > monitor.getRunPeriod()) {
                    updateMonitor(monitor, monitorType, identity);
                }
            }
            contextService.save(ContextConstants.NOTIFICATION_LAST_CHECK_TIME, String.valueOf(System.currentTimeMillis()));
            
            // TODO: cache these
            int severityLevel = 1000;
            List<Notification> notifications = getActiveNotificationsForNode(identity.getNodeGroupId(), identity.getExternalId());
            for (Notification notification : notifications) {
                if (notification.getSeverityLevel() < severityLevel) {
                    severityLevel = notification.getSeverityLevel();
                }
            }

            Map<String, INotificationType> notificationTypes = extensionService.getExtensionPointMap(INotificationType.class);
            List<MonitorEvent> monitorEvents = getMonitorEventsForNotification(severityLevel);
            for (MonitorEvent monitorEvent : monitorEvents) {
                for (Notification notification : notifications) {
                    if (monitorEvent.getSeverityLevel() >= notification.getSeverityLevel()) {
                        INotificationType notificationType = notificationTypes.get(notification.getType());
                        notificationType.notify(monitorEvent, notification);
                        updateMonitorEventAsNotified(monitorEvent);
                    }
                }
            }            
        }
    }
    
    protected void updateMonitor(Monitor monitor, IMonitorType monitorType, Node identity) {
        long value = monitorType.check(monitor);
        
        if (!monitorType.requiresClusterLock() && monitor.getRunCount() > 0) {
            List<Long> averages = averagesByType.get(monitor.getType());
            if (averages == null) {
                averages = new ArrayList<Long>();
                averagesByType.put(monitor.getType(), averages);
            }
            averages.add(value);
            while (averages.size() > monitor.getRunCount()) {
                averages.remove(0);
            }
            long accumValue = 0;
            System.out.print("Averages: ");
            for (Long oneValue : averages) {
                accumValue += oneValue;
                System.out.print(oneValue + ", ");
            }
            System.out.println("");
            value = accumValue / monitor.getRunCount();
        }

        System.out.println(hostName + "/ " + monitor.getType() + "=" + value);
        if (value >= monitor.getThreshold()) {
            MonitorEvent event = new MonitorEvent();
            event.setMonitorId(monitor.getMonitorId());
            event.setNodeId(identity.getNodeId());
            event.setEventTime(new Date());
            event.setHostName(hostName);
            event.setType(monitor.getType());
            event.setValue(value);
            event.setThreshold(monitor.getThreshold());
            event.setSeverityLevel(monitor.getSeverityLevel());
            saveMonitorEvent(event);
        }
    }

    @Override
    public List<Monitor> getMonitors() {
        return sqlTemplate.query(getSql("selectMonitorSql"), new MonitorRowMapper());
    }

    @Override
    public List<Monitor> getActiveMonitorsForNode(String nodeGroupId, String externalId) {
        return sqlTemplate.query(getSql("selectMonitorSql", "whereMonitorByNodeSql"), new MonitorRowMapper(),
                nodeGroupId, externalId);
    }

    @Override
    public void deleteMonitor(String monitorId) {
        sqlTemplate.update(getSql("deleteMonitorSql"), monitorId);
    }

    @Override
    public void saveMonitor(Monitor monitor) {
        int count = sqlTemplate.update(getSql("updateMonitorSql"), monitor.getExternalId(), monitor.getNodeGroupId(),
                monitor.getType(), monitor.getExpression(), monitor.isEnabled(), monitor.getThreshold(), monitor.getRunPeriod(), 
                monitor.getRunCount(), monitor.getSeverityLevel(), monitor.getLastUpdateBy(), 
                monitor.getLastUpdateTime(), monitor.getMonitorId());
        if (count == 0) {
            sqlTemplate.update(getSql("insertMonitorSql"), monitor.getMonitorId(), monitor.getExternalId(), 
                    monitor.getNodeGroupId(), monitor.getType(), monitor.getExpression(), monitor.isEnabled(), monitor.getThreshold(), 
                    monitor.getRunPeriod(), monitor.getRunCount(), monitor.getSeverityLevel(), 
                    monitor.getCreateTime(), monitor.getLastUpdateBy(), monitor.getLastUpdateTime());
        }
    }

    @Override
    public List<MonitorEvent> getMonitorEvents() {
        return sqlTemplate.query(getSql("selectMonitorEventSql"), new MonitorEventRowMapper());
    }

    @Override
    public List<MonitorEvent> getMonitorEventsFiltered(int limit, String type, int severityLevel, String nodeId) {
        return sqlTemplate.query(getSql("selectMonitorEventSql", "whereMonitorEventFilteredSql"), limit,
                new MonitorEventRowMapper(), type, type, severityLevel, nodeId, nodeId);
    }    

    protected List<MonitorEvent> getMonitorEventsForNotification(int severityLevel) {
        return sqlTemplate.query(getSql("selectMonitorEventSql", "whereMonitorEventForNotificationBySeveritySql"),
                new MonitorEventRowMapper(), severityLevel);
    }
    
    @Override
    public void saveMonitorEvent(MonitorEvent event) {
        sqlTemplate.update(getSql("insertMonitorEventSql"), event.getMonitorId(), event.getNodeId(),
                event.getEventTime(), event.getHostName(), event.getType(), event.getValue(), event.getThreshold(), 
                event.getSeverityLevel(), event.isNotified());
    }
    
    protected void updateMonitorEventAsNotified(MonitorEvent event) {
        sqlTemplate.update(getSql("updateMonitorEventNotifiedSql"), event.getMonitorId(), event.getNodeId(), event.getEventTime());
    }

    @Override
    public List<Notification> getNotifications() {
        return sqlTemplate.query(getSql("selectNotificationSql"), new NotificationRowMapper());
    }
    
    @Override
    public List<Notification> getActiveNotificationsForNode(String nodeGroupId, String externalId) {
        return sqlTemplate.query(getSql("selectNotificationSql", "whereNotificationByNodeSql"), new NotificationRowMapper(),
                nodeGroupId, externalId);
    }

    @Override
    public void saveNotification(Notification notification) {
        int count = sqlTemplate.update(getSql("updateNotificationSql"), 
                notification.getNodeGroupId(), notification.getExternalId(), 
                notification.getSeverityLevel(), notification.getType(), notification.getExpression(), notification.isEnabled(), 
                notification.getCreateTime(), notification.getLastUpdateBy(), 
                notification.getLastUpdateTime(), notification.getNotificationId());
        if (count == 0) {
            sqlTemplate.update(getSql("insertNotificationSql"), notification.getNotificationId(),
                    notification.getNodeGroupId(), notification.getExternalId(), 
                    notification.getSeverityLevel(), notification.getType(), notification.getExpression(), notification.isEnabled(), 
                    notification.getCreateTime(), notification.getLastUpdateBy(), 
                    notification.getLastUpdateTime());
        }
    }

    @Override
    public void deleteNotification(String notificationId) {
        sqlTemplate.update(getSql("deleteNotificationSql"), notificationId);
    }
    
    class MonitorRowMapper implements ISqlRowMapper<Monitor> {
        public Monitor mapRow(Row row) {
            Monitor m = new Monitor();
            m.setMonitorId(row.getString("monitor_id"));
            m.setExternalId(row.getString("external_id"));
            m.setNodeGroupId(row.getString("node_group_id"));
            m.setType(row.getString("type"));
            m.setExpression(row.getString("expression"));
            m.setEnabled(row.getBoolean("enabled"));
            m.setThreshold(row.getLong("threshold"));
            m.setRunPeriod(row.getInt("run_period"));
            m.setRunCount(row.getInt("run_count"));
            m.setSeverityLevel(row.getInt("severity_level"));
            m.setCreateTime(row.getDateTime("create_time"));
            m.setLastUpdateBy(row.getString("last_update_by"));
            m.setLastUpdateTime(row.getDateTime("last_update_time"));
            return m;
        }
    }
    
    class MonitorEventRowMapper implements ISqlRowMapper<MonitorEvent> {        
        public MonitorEvent mapRow(Row row) {
            MonitorEvent m = new MonitorEvent();
            m.setMonitorId(row.getString("monitor_id"));
            m.setNodeId(row.getString("node_id"));
            m.setEventTime(row.getDateTime("event_time"));
            m.setHostName(row.getString("host_name"));
            m.setType(row.getString("type"));
            m.setThreshold(row.getLong("threshold"));
            m.setValue(row.getLong("value"));
            m.setSeverityLevel(row.getInt("severity_level"));
            m.setNotified(row.getBoolean("is_notified"));
            return m;
        }
    }
    
    class NotificationRowMapper implements ISqlRowMapper<Notification> {
        public Notification mapRow(Row row) {
            Notification n = new Notification();
            n.setNotificationId(row.getString("notification_id"));
            n.setNodeGroupId(row.getString("node_group_id"));
            n.setExternalId(row.getString("external_id"));
            n.setSeverityLevel(row.getInt("severity_level"));
            n.setType(row.getString("type"));
            n.setExpression(row.getString("expression"));
            n.setEnabled(row.getBoolean("enabled"));
            n.setCreateTime(row.getDateTime("create_time"));
            n.setLastUpdateBy(row.getString("last_update_by"));
            n.setLastUpdateTime(row.getDateTime("last_update_time"));
            return n;
        }
    }
}
