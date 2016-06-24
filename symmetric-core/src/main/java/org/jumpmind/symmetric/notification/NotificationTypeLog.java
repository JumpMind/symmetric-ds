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
package org.jumpmind.symmetric.notification;

import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.jumpmind.symmetric.model.Notification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationTypeLog implements INotificationType {

    private final Logger log = LoggerFactory.getLogger(getClass());
    
    public void notify(MonitorEvent monitorEvent, Notification notification) {
        String message = "Monitor " + monitorEvent.getType() + " on " + monitorEvent.getNodeId() + " recorded "
                + monitorEvent.getValue() + " at " + monitorEvent.getEventTime();
        if (monitorEvent.getSeverityLevel() >= Monitor.SEVERE) {
            log.error(message);
        } else if (monitorEvent.getSeverityLevel() >= Monitor.WARNING) {
            log.warn(message);
        } else {
            log.info(message);
        }
    }

    @Override
    public String getName() {
        return "log";
    }

}
