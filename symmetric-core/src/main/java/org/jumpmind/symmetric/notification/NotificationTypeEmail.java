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

import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Notification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationTypeEmail implements INotificationType, ISymmetricEngineAware, IBuiltInExtensionPoint {

    private final Logger log = LoggerFactory.getLogger(getClass());
    
    protected ISymmetricEngine engine;
    
    protected static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void notify(Notification notification, List<MonitorEvent> monitorEvents) {
        String subject = null;
        if (monitorEvents.size() == 1) {
            MonitorEvent event = monitorEvents.get(0);
            subject = "Monitor event for " + event.getType() + " from node " + event.getNodeId();
        } else {
            Set<String> nodeIds = new HashSet<String>();
            Set<String> types = new HashSet<String>();
            for (MonitorEvent event : monitorEvents) {
                nodeIds.add(event.getNodeId());
                types.add(event.getType());
            }
            StringBuilder typesString = new StringBuilder();
            Iterator<String> iter = types.iterator();
            while (iter.hasNext()) {
                typesString.append(iter.next());
                if (iter.hasNext()) {
                    typesString.append(", ");
                }
            }
            subject = "Monitor events for " + typesString + " from " + nodeIds.size() + " nodes"; 
        }

        Map<String, Node> nodes = engine.getNodeService().findAllNodesAsMap();
        StringBuilder text = new StringBuilder();
        for (MonitorEvent event : monitorEvents) {
            Node node = nodes.get(event.getNodeId());
            String nodeString = node != null ? node.toString() : event.getNodeId();
            text.append(DATE_FORMATTER.format(event.getEventTime())).append(" [");
            text.append(Monitor.getSeverityLevelNames().get(event.getSeverityLevel())).append("] [");
            text.append(nodeString).append("] [");
            text.append(event.getHostName()).append("] ");
            text.append("Monitor event for ").append(event.getType());
            text.append(" reached threshold of ").append(event.getThreshold());
            text.append(" with a value of ").append(event.getValue()).append("\n");
        }
        
        String recipients = notification.getExpression();
        if (recipients != null) {
            log.info("Sending email with subject '" + subject + "' to " + recipients);            
            engine.getMailService().sendEmail(subject, text.toString(), recipients);
        } else {
            log.warn("Notification " + notification.getNotificationId() + " has no email recipients configured.");
        }
    }

    @Override
    public String getName() {
        return "email";
    }

    @Override
    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.engine = engine;
    }

}
