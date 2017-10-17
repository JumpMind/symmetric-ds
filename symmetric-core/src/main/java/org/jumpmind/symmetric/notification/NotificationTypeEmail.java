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
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Notification;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.monitor.BatchErrorWrapper;
import org.jumpmind.util.LogSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class NotificationTypeEmail implements INotificationType, ISymmetricEngineAware, IBuiltInExtensionPoint {

    private final Logger log = LoggerFactory.getLogger(getClass());
    
    protected ISymmetricEngine engine;
    
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
            SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String nodeString = node != null ? node.toString() : event.getNodeId();
            text.append(dateFormatter.format(event.getEventTime())).append(" [");
            text.append(Monitor.getSeverityLevelNames().get(event.getSeverityLevel())).append("] [");
            text.append(nodeString).append("] [");
            text.append(event.getHostName()).append("] ");
            text.append("Monitor event for ").append(event.getType());
            text.append(" reached threshold of ").append(event.getThreshold());
            text.append(" with a value of ").append(event.getValue()).append("\n");

            try {
                StringBuilder stackTrace = new StringBuilder();
                if (event.getType().equals("log")) {
                    stackTrace = getLogDetails(event);
                } else if (event.getType().equals("batchError")) {
                    stackTrace = getBatchDetails(event);
                } else if (event.getType().equals("offlineNodes")) {
                    stackTrace = getOfflineDetails(event);
                } else if (event.getType().equals("batchUnsent")) {
                    stackTrace.append(event.getValue()).append(" batches unsent.").append("\n");
                } else if (event.getType().equals("dataUnrouted")) {
                    stackTrace.append(event.getValue()).append(" unrouted data rows.").append("\n");
                } else if (event.getType().equals("dataGap")) {
                    stackTrace.append(event.getValue()).append(" data gap(s) recorded.").append("\n");
                } else if (event.getType().equals("cpu")) {
                    stackTrace.append("CPU usage is at ").append(event.getValue()).append("%\n");
                } else if (event.getType().equals("memory")) {
                    stackTrace.append("Memory usage is at ").append(event.getValue()).append("%\n");
                } else if (event.getType().equals("disk")) {
                    stackTrace.append("Disk usage is at ").append(event.getValue()).append("%\n");
                }
                if (!stackTrace.toString().isEmpty()) {
                    text.append("\nDetails: ");
                    text.append(stackTrace.toString());
                    text.append("\n");
                }
            } catch (Exception e) {
                log.debug("", e);
            }
        }
            
        
        String recipients = notification.getExpression();
        if (recipients != null) {
            log.info("Sending email with subject '" + subject + "' to " + recipients);            
            engine.getMailService().sendEmail(subject, text.toString(), recipients);
        } else {
            log.warn("Notification " + notification.getNotificationId() + " has no email recipients configured.");
        }
    }
    
    protected static StringBuilder getOfflineDetails(MonitorEvent event) throws JsonParseException, JsonMappingException, IOException {
        StringBuilder stackTrace = new StringBuilder();
        stackTrace.append("\n");
        for(String node: deserializeOfflineNodes(event)) {
            stackTrace.append("Node ").append(node).append(" is offline.").append("\n");
        }
        return stackTrace;
    }
    
    protected static StringBuilder getBatchDetails(MonitorEvent event) throws JsonParseException, JsonMappingException, IOException {
        StringBuilder stackTrace = new StringBuilder();
        BatchErrorWrapper errors = deserializeBatches(event);
        if(errors != null) {
            List<OutgoingBatch> outErrors = errors.getOutgoingErrors();
            for(OutgoingBatch b: outErrors) {
                stackTrace.append("The outgoing batch ").append(b.getNodeBatchId());
                stackTrace.append(" failed: ").append(b.getSqlMessage()).append("\n");
            }
            List<IncomingBatch> inErrors = errors.getIncomingErrors();
            for(IncomingBatch b: inErrors) {
                stackTrace.append("The incoming batch ").append(b.getNodeBatchId());
                stackTrace.append(" failed: ").append(b.getSqlMessage()).append("\n");
            }
        }
        return stackTrace;
    }
    
    protected static StringBuilder getLogDetails(MonitorEvent event) throws JsonParseException, JsonMappingException, IOException {
        StringBuilder stackTrace = new StringBuilder();
        int count = 0;
        for (LogSummary summary : deserializeLogSummary(event)) {
            if(summary.getMessage() != null) {
                stackTrace.append(summary.getMessage());
                count++;
            }
            if(summary.getStackTrace() != null) {
                stackTrace.append(summary.getStackTrace());
                count++;
            }
        }
        if(count > 0) {
            stackTrace.append("\n");
        }
        return stackTrace;
    }
    
    protected static List<String> deserializeOfflineNodes(MonitorEvent event) throws JsonParseException, JsonMappingException, IOException {
        List<String> nodes = null;
        if(event.getDetails() != null) {
            ObjectMapper mapper= new ObjectMapper();
            nodes = mapper.readValue(event.getDetails(), new TypeReference<List<String>>() {
            });
        }
        if(nodes == null) {
            nodes = Collections.emptyList();
        }
        return nodes;
    }
    
    protected static BatchErrorWrapper deserializeBatches(MonitorEvent event) throws JsonParseException, JsonMappingException, IOException {
        BatchErrorWrapper batches = null;
        if(event.getDetails() != null) {
            ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            batches = mapper.readValue(event.getDetails(), BatchErrorWrapper.class);
        }
        return batches;
    }
    
    protected static List<LogSummary> deserializeLogSummary(MonitorEvent event) throws JsonParseException, JsonMappingException, IOException {
        List<LogSummary> summaries = null;
        if (event.getDetails() != null) {
            ObjectMapper mapper = new ObjectMapper();
            mapper.addMixIn(Throwable.class, ThrowableMixIn.class);
            mapper.addMixIn(LogSummary.class, LogSummaryMixIn.class);
            summaries = mapper.readValue(event.getDetails(), new TypeReference<List<LogSummary>>() {
            });
        }

        if (summaries == null) {
            summaries = Collections.emptyList();
        }
        return summaries;
    }

    protected interface ThrowableMixIn {
        @JsonIgnore
        Throwable getCause();
    }
    
    protected interface LogSummaryMixIn {
        @JsonIgnore
        Level getLevel();
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
