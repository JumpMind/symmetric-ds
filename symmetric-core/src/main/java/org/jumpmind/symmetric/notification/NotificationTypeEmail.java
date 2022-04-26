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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.jumpmind.symmetric.model.Notification;
import org.jumpmind.symmetric.model.Notification.EmailExpression;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.monitor.BatchErrorWrapper;
import org.jumpmind.symmetric.util.SymmetricUtils;
import org.jumpmind.util.FormatUtils;
import org.jumpmind.util.LogSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class NotificationTypeEmail implements INotificationType, ISymmetricEngineAware, IBuiltInExtensionPoint {
    private final Logger log = LoggerFactory.getLogger(getClass());
    protected ISymmetricEngine engine;

    public void notify(Notification notification, List<MonitorEvent> monitorEvents) {
        Map<String, String> eventListReplacements = SymmetricUtils.getReplacementsForMonitorEventList(engine,
                monitorEvents);
        EmailExpression expression = notification.getEmailExpression();
        String subject = FormatUtils.replaceTokens(expression.getSubject(), eventListReplacements, true);
        Map<String, String> templateMap = expression.getTemplateMap();
        StringBuilder text = new StringBuilder();
        if (!StringUtils.isBlank(expression.getBodyBefore())) {
            text.append(FormatUtils.replaceTokens(expression.getBodyBefore(), eventListReplacements, true) + "\n");
        }
        for (MonitorEvent event : monitorEvents) {
            try {
                Map<String, String> eventReplacements = SymmetricUtils.getReplacementsForMonitorEvent(engine, event);
                if (event.getType().equals("log")) {
                    eventReplacements.put("eventDetails", getLogDetails(event));
                } else if (event.getType().equals("batchError")) {
                    eventReplacements.put("eventDetails", getBatchDetails(event));
                } else if (event.getType().equals("offlineNodes")) {
                    eventReplacements.put("eventDetails", getOfflineDetails(event));
                }
                if (monitorEvents.indexOf(event) > 0) {
                    text.append("\n");
                }
                if (event.isResolved()) {
                    text.append(FormatUtils.replaceTokens(expression.getResolved(), eventReplacements, true));
                    continue;
                } else {
                    text.append(FormatUtils.replaceTokens(expression.getUnresolved(), eventReplacements, true));
                }
                String template = templateMap.get(event.getType());
                if (template == null) {
                    template = templateMap.get("default");
                }
                if (template != null) {
                    text.append("\n" + FormatUtils.replaceTokens(template, eventReplacements, true));
                }
            } catch (Exception e) {
                log.debug("", e);
            }
        }
        if (!StringUtils.isBlank(expression.getBodyAfter())) {
            text.append("\n" + FormatUtils.replaceTokens(expression.getBodyAfter(), eventListReplacements, true));
        }
        String recipients = String.join(",", expression.getEmails());
        if (recipients != null) {
            log.info("Sending email with subject '" + subject + "' to " + recipients);
            engine.getMailService().sendEmail(subject, text.toString(), recipients);
        } else {
            log.warn("Notification " + notification.getNotificationId() + " has no email recipients configured.");
        }
    }

    protected static String getOfflineDetails(MonitorEvent event) throws IOException {
        StringBuilder stackTrace = new StringBuilder();
        stackTrace.append("\n");
        for (String node : deserializeOfflineNodes(event)) {
            stackTrace.append("Node ").append(node).append(" is offline.").append("\n");
        }
        return stackTrace.toString();
    }

    protected static String getBatchDetails(MonitorEvent event) throws IOException {
        StringBuilder stackTrace = new StringBuilder();
        BatchErrorWrapper errors = deserializeBatches(event);
        if (errors != null) {
            List<OutgoingBatch> outErrors = errors.getOutgoingErrors();
            for (OutgoingBatch b : outErrors) {
                stackTrace.append("The outgoing batch ").append(b.getNodeBatchId());
                stackTrace.append(" failed: ").append(b.getSqlMessage()).append("\n");
            }
            List<IncomingBatch> inErrors = errors.getIncomingErrors();
            for (IncomingBatch b : inErrors) {
                stackTrace.append("The incoming batch ").append(b.getNodeBatchId());
                stackTrace.append(" failed: ").append(b.getSqlMessage()).append("\n");
            }
        }
        return stackTrace.toString();
    }

    protected static String getLogDetails(MonitorEvent event) throws IOException {
        StringBuilder stackTrace = new StringBuilder();
        int count = 0;
        for (LogSummary summary : deserializeLogSummary(event)) {
            if (summary.getMessage() != null) {
                stackTrace.append(summary.getMessage());
                count++;
            }
            if (summary.getStackTrace() != null) {
                stackTrace.append(summary.getStackTrace());
                count++;
            }
        }
        if (count > 0) {
            stackTrace.append("\n");
        }
        return stackTrace.toString();
    }

    protected static List<String> deserializeOfflineNodes(MonitorEvent event) throws IOException {
        List<String> nodes = null;
        if (event.getDetails() != null) {
            new Gson().fromJson(event.getDetails(), new TypeToken<List<String>>() {
            }.getType());
        }
        if (nodes == null) {
            nodes = Collections.emptyList();
        }
        return nodes;
    }

    protected static BatchErrorWrapper deserializeBatches(MonitorEvent event) {
        BatchErrorWrapper batches = null;
        if (event.getDetails() != null) {
            batches = new Gson().fromJson(event.getDetails(), BatchErrorWrapper.class);
        }
        return batches;
    }

    protected static List<LogSummary> deserializeLogSummary(MonitorEvent event) throws IOException {
        List<LogSummary> summaries = null;
        if (event.getDetails() != null) {
            summaries = new Gson().fromJson(event.getDetails(), new TypeToken<List<LogSummary>>() {
            }.getType());
        }
        if (summaries == null) {
            summaries = Collections.emptyList();
        }
        return summaries;
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
