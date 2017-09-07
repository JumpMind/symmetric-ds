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
package org.jumpmind.symmetric.service;

import java.util.List;

import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.jumpmind.symmetric.model.Notification;

public interface IMonitorService {

    public void update();

    public List<Monitor> getMonitors();

    public List<Monitor> getActiveMonitorsForNode(String nodeGroupId, String externalId); 
    
    public void deleteMonitor(String notificationId);

    public void saveMonitor(Monitor monitor);
    
    public List<MonitorEvent> getMonitorEvents();
    
    public List<MonitorEvent> getMonitorEventsFiltered(int limit, String type, int severityLevel, String nodeId, Boolean isResolved);

    public void saveMonitorEvent(MonitorEvent notificationEvent);

    public void deleteMonitorEvent(MonitorEvent event);
    
    public void updateMonitorEventAsResolved(MonitorEvent event);

    public List<Notification> getNotifications();
    
    public List<Notification> getActiveNotificationsForNode(String nodeGroupId, String externalId);
    
    public void saveNotification(Notification notification);
    
    public void deleteNotification(String notificationId);
    
    public void flushMonitorCache();
    
    public void flushNotificationCache();

	public List<Monitor> getActiveMonitorsUnresolvedForNode(String string, String string2);
    
}
