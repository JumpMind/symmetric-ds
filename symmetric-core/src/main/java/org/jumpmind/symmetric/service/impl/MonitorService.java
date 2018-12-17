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
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Lock;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Notification;
import org.jumpmind.symmetric.monitor.IMonitorType;
import org.jumpmind.symmetric.monitor.MonitorTypeOfflineNodes;
import org.jumpmind.symmetric.monitor.MonitorTypeBatchError;
import org.jumpmind.symmetric.monitor.MonitorTypeBatchUnsent;
import org.jumpmind.symmetric.monitor.MonitorTypeCpu;
import org.jumpmind.symmetric.monitor.MonitorTypeDataGap;
import org.jumpmind.symmetric.monitor.MonitorTypeDisk;
import org.jumpmind.symmetric.monitor.MonitorTypeLog;
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
import org.jumpmind.util.AppUtils;

public class MonitorService extends AbstractService implements IMonitorService {
    
    protected String hostName;
    
    protected INodeService nodeService;
    
    protected IExtensionService extensionService;
    
    protected IClusterService clusterService;
    
    protected IContextService contextService;

    protected Map<String, Long> checkTimesByType = new HashMap<String, Long>();
    
    protected Map<String, List<Long>> averagesByType = new HashMap<String, List<Long>>();
    
    protected List<Monitor> activeMonitorCache;
    
    protected long activeMonitorCacheTime;
    
    protected List<Notification> activeNotificationCache;
    
    protected long activeNotificationCacheTime;

    public MonitorService(IParameterService parameterService, ISymmetricDialect symmetricDialect, INodeService nodeService,
            IExtensionService extensionService, IClusterService clusterService, IContextService contextService) {
        super(parameterService, symmetricDialect);
        setSqlMap(new MonitorServiceSqlMap(symmetricDialect.getPlatform(), createSqlReplacementTokens()));

        this.nodeService = nodeService;
        this.extensionService = extensionService;
        this.clusterService = clusterService;
        this.contextService = contextService;
        hostName = AppUtils.getHostName();
        
        IMonitorType monitorExtensions[] = { new MonitorTypeBatchError(), new MonitorTypeBatchUnsent(), new MonitorTypeCpu(), 
                new MonitorTypeDataGap(), new MonitorTypeDisk(), new MonitorTypeMemory(), new MonitorTypeUnrouted(), new MonitorTypeLog(), new MonitorTypeOfflineNodes() };
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
        Node identity = nodeService.findIdentity();
        if (identity != null) {
            List<Monitor> activeMonitors = getActiveMonitorsForNode(identity.getNodeGroupId(), identity.getExternalId());
            Map<String, MonitorEvent> unresolved = getMonitorEventsNotResolvedForNode(identity.getNodeId());
    
            for (Monitor monitor : activeMonitors) {
                IMonitorType monitorType = monitorTypes.get(monitor.getType());
                if (monitorType != null) {
                    if (!monitorType.requiresClusterLock()) {
                        Long lastCheckTimeLong = checkTimesByType.get(monitor.getMonitorId());
                        long lastCheckTime = lastCheckTimeLong != null ? lastCheckTimeLong : 0;
                        if (lastCheckTime == 0 || (System.currentTimeMillis() - lastCheckTime) / 1000 >= monitor.getRunPeriod()) {
                            checkTimesByType.put(monitor.getMonitorId(), System.currentTimeMillis());
                            updateMonitor(monitor, monitorType, identity, unresolved);
                        }
                    }
                } else {
                    log.warn("Could not find monitor of type '" + monitor.getType() + "'");
                }
            }
            
            if (clusterService.lock(ClusterConstants.MONITOR)) {
                Lock lock = clusterService.findLocks().get(ClusterConstants.MONITOR);
                long clusterLastCheckTime = lock.getLastLockTime() != null ? lock.getLastLockTime().getTime() : 0;
                
                try {
                    for (Monitor monitor : activeMonitors) {
                        IMonitorType monitorType = monitorTypes.get(monitor.getType());
                        if (monitorType != null && monitorType.requiresClusterLock() && 
                                (System.currentTimeMillis() - clusterLastCheckTime) / 1000 >= monitor.getRunPeriod()) {
                            updateMonitor(monitor, monitorType, identity, unresolved);
                        }
                    }
                    
                    int minSeverityLevel = Integer.MAX_VALUE;
                    List<Notification> notifications = getActiveNotificationsForNode(identity.getNodeGroupId(), identity.getExternalId());
                    if (notifications.size() > 0) {
                        for (Notification notification : notifications) {
                            if (notification.getSeverityLevel() < minSeverityLevel) {
                                minSeverityLevel = notification.getSeverityLevel();
                            }
                        }
        
                        Map<String, INotificationType> notificationTypes = extensionService.getExtensionPointMap(INotificationType.class);
                        List<MonitorEvent> allMonitorEvents = getMonitorEventsForNotification(minSeverityLevel);
                        for (Notification notification : notifications) {
                            List<MonitorEvent> monitorEvents = new ArrayList<MonitorEvent>();
                            for (MonitorEvent monitorEvent : allMonitorEvents) {
                                if (monitorEvent.getSeverityLevel() >= notification.getSeverityLevel()) {
                                    monitorEvents.add(monitorEvent);
                                }
                            }
                            if (monitorEvents.size() > 0) {
                                INotificationType notificationType = notificationTypes.get(notification.getType());
                                if (notificationType != null) {
                                    notificationType.notify(notification, monitorEvents);
                                    updateMonitorEventAsNotified(monitorEvents);
                                } else {
                                    log.warn("Could not find notification of type '" + notification.getType() + "'");
                                }
                            }
                        }                
                    }
                } finally {
                    clusterService.unlock(ClusterConstants.MONITOR);
                }
            }
        }
    }

    protected void updateMonitor(Monitor monitor, IMonitorType monitorType, Node identity, Map<String, MonitorEvent> unresolved) {
        MonitorEvent eventValue = monitorType.check(monitor);
        boolean readyToCompare = true;
        
        if (!monitorType.requiresClusterLock() && monitor.getRunCount() > 0) {
            List<Long> averages = averagesByType.get(monitor.getType());
            if (averages == null) {
                averages = new ArrayList<Long>();
                averagesByType.put(monitor.getType(), averages);
            }
            averages.add(eventValue.getValue());
            while (averages.size() > monitor.getRunCount()) {
                averages.remove(0);
            }
            
            if (averages.size() == monitor.getRunCount()) {
                long accumValue = 0;
                for (Long oneValue : averages) {
                    accumValue += oneValue;
                }
                eventValue.setValue(accumValue / monitor.getRunCount());
            } else {
                readyToCompare = false;
            }
        }

        if (readyToCompare) {
            MonitorEvent event = unresolved.get(monitor.getMonitorId());
            Date now = new Date((System.currentTimeMillis() / 1000) * 1000);
            if (event != null && eventValue.getValue() < monitor.getThreshold()) {
                event.setLastUpdateTime(now);
                updateMonitorEventAsResolved(event);
            } else if (eventValue.getValue()  >= monitor.getThreshold()) {
                if (event == null) {
                    event = new MonitorEvent();
                    event.setMonitorId(monitor.getMonitorId());
                    event.setNodeId(identity.getNodeId());
                    event.setEventTime(now);
                    event.setHostName(hostName);
                    event.setType(monitor.getType());
                    event.setValue(eventValue.getValue());
                    if (eventValue.getCount() == 0) {
                        event.setCount(1);
                    } else {
                        event.setCount(eventValue.getCount());
                    }
                    event.setThreshold(monitor.getThreshold());
                    event.setSeverityLevel(monitor.getSeverityLevel());
                    event.setLastUpdateTime(now);
                    event.setDetails(eventValue.getDetails());
                    insertMonitorEvent(event);
                } else {
                    event.setHostName(hostName);
                    event.setType(monitor.getType());
                    event.setValue(eventValue.getValue() );
                    if (eventValue.getCount() == 0) {
                        event.setCount(event.getCount() + 1);
                    } else {
                        event.setCount(eventValue.getCount());
                    }
                    event.setThreshold(monitor.getThreshold());
                    event.setSeverityLevel(monitor.getSeverityLevel());
                    event.setLastUpdateTime(now);
                    event.setDetails(eventValue.getDetails());
                    saveMonitorEvent(event);
                }
            }
        }
    }

    @Override
    public List<Monitor> getMonitors() {
        return sqlTemplate.query(getSql("selectMonitorSql"), new MonitorRowMapper());
    }

    @Override
    public List<Monitor> getActiveMonitorsForNode(String nodeGroupId, String externalId) {
        long cacheTimeout = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_MONITOR_IN_MS);
        if (activeMonitorCache == null || System.currentTimeMillis() - activeMonitorCacheTime > cacheTimeout) {
            activeMonitorCache = sqlTemplate.query(getSql("selectMonitorSql", "whereMonitorByNodeSql"), new MonitorRowMapper(),
                    nodeGroupId, externalId);
        }
        return activeMonitorCache;
    }
    
    @Override
    public List<Monitor> getActiveMonitorsUnresolvedForNode(String nodeGroupId, String externalId) {
        long cacheTimeout = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_MONITOR_IN_MS);
        if (activeMonitorCache == null || System.currentTimeMillis() - activeMonitorCacheTime > cacheTimeout) {
            activeMonitorCache = sqlTemplate.query(getSql("selectMonitorWhereNotResolved"), new MonitorRowMapper(),
                    nodeGroupId, externalId);
        }
        return activeMonitorCache;
    }

    @Override
    public void deleteMonitor(String monitorId) {
        sqlTemplate.update(getSql("deleteMonitorSql"), monitorId);
    }

    @Override
    public void saveMonitor(Monitor monitor) {
        int count = sqlTemplate.update(getSql("updateMonitorSql"), monitor.getExternalId(), monitor.getNodeGroupId(),
                monitor.getType(), monitor.getExpression(), monitor.isEnabled() ? 1 : 0, monitor.getThreshold(), monitor.getRunPeriod(), 
                monitor.getRunCount(), monitor.getSeverityLevel(), monitor.getLastUpdateBy(), 
                monitor.getLastUpdateTime(), monitor.getMonitorId());
        if (count == 0) {
            sqlTemplate.update(getSql("insertMonitorSql"), monitor.getMonitorId(), monitor.getExternalId(), 
                    monitor.getNodeGroupId(), monitor.getType(), monitor.getExpression(), monitor.isEnabled() ? 1 : 0, monitor.getThreshold(), 
                    monitor.getRunPeriod(), monitor.getRunCount(), monitor.getSeverityLevel(), 
                    monitor.getCreateTime(), monitor.getLastUpdateBy(), monitor.getLastUpdateTime());
        }
    }

    @Override
    public List<MonitorEvent> getMonitorEvents() {
        return sqlTemplate.query(getSql("selectMonitorEventSql"), new MonitorEventRowMapper());
    }

    protected Map<String, MonitorEvent> getMonitorEventsNotResolvedForNode(String nodeId) {
        List<MonitorEvent> list = sqlTemplate.query(getSql("selectMonitorEventSql", "whereMonitorEventNotResolvedSql"), 
                new MonitorEventRowMapper(), nodeId);
        Map<String, MonitorEvent> map = new HashMap<String, MonitorEvent>();
        for (MonitorEvent monitorEvent : list) {
            map.put(monitorEvent.getMonitorId(), monitorEvent);
        }
        return map;
    }

    @Override
    public List<MonitorEvent> getMonitorEventsFiltered(int limit, String type, int severityLevel, String nodeId, Boolean isResolved) {
        String sql = getSql("selectMonitorEventSql", "whereMonitorEventFilteredSql");
        ArrayList<Object> args = new ArrayList<Object>();
        args.add(severityLevel);
        if (isResolved != null) {
            sql += " and is_resolved = ?";
            args.add(isResolved ? 1 : 0);
        }
        if (type != null) {
            sql += " and type = ?";
            args.add(type);
        }
        if (nodeId != null) {
            sql += " and node_id = ?";
            args.add(nodeId);
        }
        sql += " order by event_time desc";
        return sqlTemplate.query(sql, limit, new MonitorEventRowMapper(), args.toArray());
    }    

    protected List<MonitorEvent> getMonitorEventsForNotification(int severityLevel) {
        return sqlTemplate.query(getSql("selectMonitorEventSql", "whereMonitorEventForNotificationBySeveritySql"),
                new MonitorEventRowMapper(), severityLevel);
    }
    
    @Override
    public void saveMonitorEvent(MonitorEvent event) {
        if (!updateMonitorEvent(event)) {
            insertMonitorEvent(event);
        }
    }

    protected void insertMonitorEvent(MonitorEvent event) {
        sqlTemplate.update(getSql("insertMonitorEventSql"), event.getMonitorId(), event.getNodeId(),
                event.getEventTime(), event.getHostName(), event.getType(), event.getValue(), event.getCount(), event.getThreshold(), 
                event.getSeverityLevel(), event.isResolved() ? 1 : 0, event.isNotified() ? 1 : 0, event.getDetails(), event.getLastUpdateTime());
    }

    protected boolean updateMonitorEvent(MonitorEvent event) {
        int count = sqlTemplate.update(getSql("updateMonitorEventSql"), event.getHostName(), event.getType(), event.getValue(), 
                event.getCount(), event.getThreshold(), event.getSeverityLevel(), event.getLastUpdateTime(),
                event.getDetails(), event.getMonitorId(), event.getNodeId(), event.getEventTime());
        return count != 0;
    }

    @Override
    public void deleteMonitorEvent(MonitorEvent event) {
        sqlTemplate.update(getSql("deleteMonitorEventSql"), event.getMonitorId(), event.getNodeId(), event.getEventTime());
    }

    protected void updateMonitorEventAsNotified(List<MonitorEvent> events) {
        for (MonitorEvent event : events) {
            updateMonitorEventAsNotified(event);
        }
    }

    protected void updateMonitorEventAsNotified(MonitorEvent event) {
        sqlTemplate.update(getSql("updateMonitorEventNotifiedSql"), event.getMonitorId(), event.getNodeId(), event.getEventTime());
    }

    @Override
    public void updateMonitorEventAsResolved(MonitorEvent event) {
        sqlTemplate.update(getSql("updateMonitorEventResolvedSql"), event.getLastUpdateTime(), event.getMonitorId(), 
                event.getNodeId(), event.getEventTime());
    }

    @Override
    public List<Notification> getNotifications() {
        return sqlTemplate.query(getSql("selectNotificationSql"), new NotificationRowMapper());
    }
    
    @Override
    public List<Notification> getActiveNotificationsForNode(String nodeGroupId, String externalId) {
        long cacheTimeout = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_NOTIFICATION_IN_MS);
        if (activeNotificationCache == null || System.currentTimeMillis() - activeNotificationCacheTime > cacheTimeout) {
            activeNotificationCache = sqlTemplate.query(getSql("selectNotificationSql", "whereNotificationByNodeSql"), 
                    new NotificationRowMapper(), nodeGroupId, externalId);
        }
        return activeNotificationCache;
    }

    @Override
    public void saveNotification(Notification notification) {
        int count = sqlTemplate.update(getSql("updateNotificationSql"), 
                notification.getNodeGroupId(), notification.getExternalId(), 
                notification.getSeverityLevel(), notification.getType(), notification.getExpression(), notification.isEnabled() ? 1 : 0, 
                notification.getCreateTime(), notification.getLastUpdateBy(), 
                notification.getLastUpdateTime(), notification.getNotificationId());
        if (count == 0) {
            sqlTemplate.update(getSql("insertNotificationSql"), notification.getNotificationId(),
                    notification.getNodeGroupId(), notification.getExternalId(), 
                    notification.getSeverityLevel(), notification.getType(), notification.getExpression(), notification.isEnabled() ? 1 : 0, 
                    notification.getCreateTime(), notification.getLastUpdateBy(), 
                    notification.getLastUpdateTime());
        }
    }

    @Override
    public void deleteNotification(String notificationId) {
        sqlTemplate.update(getSql("deleteNotificationSql"), notificationId);
    }

    @Override
    public void flushMonitorCache() {
        activeMonitorCache = null;
    }
    
    @Override
    public void flushNotificationCache() {
        activeNotificationCache = null;
    }

    static class MonitorRowMapper implements ISqlRowMapper<Monitor> {
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
    
    static class MonitorEventRowMapper implements ISqlRowMapper<MonitorEvent> {
        public MonitorEvent mapRow(Row row) {
            MonitorEvent m = new MonitorEvent();
            m.setMonitorId(row.getString("monitor_id"));
            m.setNodeId(row.getString("node_id"));
            m.setEventTime(row.getDateTime("event_time"));
            m.setHostName(row.getString("host_name"));
            m.setType(row.getString("type"));
            m.setThreshold(row.getLong("threshold"));
            m.setValue(row.getLong("event_value"));
            m.setCount(row.getInt("event_count"));
            m.setSeverityLevel(row.getInt("severity_level"));
            m.setResolved(row.getBoolean("is_resolved"));
            m.setNotified(row.getBoolean("is_notified"));
            m.setLastUpdateTime(row.getDateTime("last_update_time"));
            m.setDetails(row.getString("details"));
            return m;
        }
    }
    
    static class NotificationRowMapper implements ISqlRowMapper<Notification> {
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
