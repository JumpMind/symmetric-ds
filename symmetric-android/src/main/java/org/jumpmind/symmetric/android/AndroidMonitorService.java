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
package org.jumpmind.symmetric.android;

import java.util.List;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.jumpmind.symmetric.model.Notification;
import org.jumpmind.symmetric.service.IMonitorService;

public class AndroidMonitorService implements IMonitorService {
    public AndroidMonitorService(ISymmetricEngine engine) {
    }

    @Override
    public void update() {
    }

    @Override
    public List<Monitor> getMonitors() {
        return null;
    }

    @Override
    public List<Monitor> getActiveMonitorsForNode(String nodeGroupId, String externalId) {
        return null;
    }

    @Override
    public List<Monitor> getActiveMonitorsForNodeFromDb(String nodeGroupId, String externalId) {
        return null;
    }

    @Override
    public void deleteMonitor(String notificationId) {
    }

    @Override
    public void saveMonitor(Monitor monitor) {
    }

    @Override
    public void saveMonitorAsCopy(Monitor monitor) {
    }

    @Override
    public void renameMonitor(String oldId, Monitor monitor) {
    }

    @Override
    public List<MonitorEvent> getMonitorEvents() {
        return null;
    }

    @Override
    public List<MonitorEvent> getMonitorEventsFiltered(int limit, String type, int severityLevel, String nodeId, Boolean isResolved) {
        return null;
    }

    @Override
    public void saveMonitorEvent(MonitorEvent notificationEvent) {
    }

    @Override
    public void deleteMonitorEvent(MonitorEvent event) {
    }

    @Override
    public void updateMonitorEventAsResolved(MonitorEvent event) {
    }

    @Override
    public List<Notification> getNotifications() {
        return null;
    }

    @Override
    public List<Notification> getActiveNotificationsForNode(String nodeGroupId, String externalId) {
        return null;
    }

    @Override
    public List<Notification> getActiveNotificationsForNodeFromDb(String nodeGroupId, String externalId) {
        return null;
    }

    @Override
    public void saveNotification(Notification notification) {
    }

    @Override
    public void saveNotificationAsCopy(Notification notification) {
    }

    @Override
    public void renameNotification(String oldId, Notification notification) {
    }

    @Override
    public void deleteNotification(String notificationId) {
    }

    @Override
    public void flushMonitorCache() {
    }

    @Override
    public void flushNotificationCache() {
    }

    @Override
    public List<Monitor> getActiveMonitorsUnresolvedForNode(String nodeGroupId, String externalId) {
        return null;
    }

    @Override
    public List<Monitor> getActiveMonitorsUnresolvedForNodeFromDb(String nodeGroupId, String externalId) {
        return null;
    }
}
