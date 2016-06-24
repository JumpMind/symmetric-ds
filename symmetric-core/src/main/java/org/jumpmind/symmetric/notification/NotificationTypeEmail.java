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

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.jumpmind.symmetric.model.Notification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationTypeEmail implements INotificationType, ISymmetricEngineAware {

    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private ISymmetricEngine engine;

    public void notify(MonitorEvent monitorEvent, Notification notification) {
        String subject = "Monitor " + monitorEvent.getType() + " on " + monitorEvent.getNodeId() + " is " + monitorEvent.getValue();
        String text = "Monitor " + monitorEvent.getType() + " on " + monitorEvent.getNodeId() + " recorded "
                + monitorEvent.getValue() + " at " + monitorEvent.getEventTime();
        String recipients = notification.getExpression();
        
        if (recipients != null) {    
            log.debug("Sending email about monitor " + monitorEvent.getType() + " on " + monitorEvent.getNodeId() + " with value " 
                    + monitorEvent.getValue() + " to recipients " + recipients);
            
            engine.getMailService().sendEmail(subject, text, recipients);
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
