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
package org.jumpmind.symmetric.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.symmetric.common.ParameterConstants;

import com.google.gson.Gson;

public class Notification implements IModelObject {
    private static final long serialVersionUID = 1L;
    private String notificationId;
    private String nodeGroupId;
    private String externalId;
    private int severityLevel;
    private String type;
    private String expression;
    private boolean enabled;
    private Date createTime;
    private String lastUpdateBy;
    private Date lastUpdateTime;
    private transient String targetNode;

    public String getNotificationId() {
        return notificationId;
    }

    public void setNotificationId(String notificationId) {
        this.notificationId = notificationId;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    public String getNodeGroupId() {
        return nodeGroupId;
    }

    public void setNodeGroupId(String nodeGroupId) {
        this.nodeGroupId = nodeGroupId;
    }

    public int getSeverityLevel() {
        return severityLevel;
    }

    public void setSeverityLevel(int severityLevel) {
        this.severityLevel = severityLevel;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getLastUpdateBy() {
        return lastUpdateBy;
    }

    public void setLastUpdateBy(String lastUpdateBy) {
        this.lastUpdateBy = lastUpdateBy;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getExpression() {
        return expression;
    }

    public LogExpression getLogExpression() {
        if (type != null && type.equals("log") && expression != null && expression.contains("{")) {
            return new Gson().fromJson(expression, LogExpression.class);
        }
        return new LogExpression();
    }

    public EmailExpression getEmailExpression() {
        if (type != null && type.equals("email") && expression != null && expression.contains("{")) {
            return new Gson().fromJson(expression, EmailExpression.class);
        }
        EmailExpression emailExpression = new EmailExpression();
        if (!StringUtils.isEmpty(expression)) {
            emailExpression.setEmails(Arrays.asList(expression.split(",", -1)));
        }
        return emailExpression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public void setExpression(LogExpression expression) {
        this.expression = new Gson().toJson(expression);
    }

    public void setExpression(EmailExpression expression) {
        this.expression = new Gson().toJson(expression);
    }

    public String getSeverityLevelName() {
        String name = Monitor.severityLevelNames.get(severityLevel);
        if (name == null) {
            name = Monitor.INFO_NAME;
        }
        return name;
    }

    public String getTargetNode() {
        if (targetNode == null) {
            if (externalId != null && !externalId.equals(ParameterConstants.ALL)) {
                targetNode = externalId + " only";
            } else {
                targetNode = nodeGroupId;
            }
        }
        return targetNode;
    }

    public void setTargetNode(String targetNode) {
        this.targetNode = targetNode;
    }

    public class LogExpression {
        private String unresolved = "Monitor $(eventType) on $(eventNodeId) reached threshold of"
                + " $(eventThreshold) with a value of $(eventValue)";
        private String resolved = "Monitor $(eventType) on $(eventNodeId) is resolved";

        public String getUnresolved() {
            return unresolved;
        }

        public void setUnresolved(String unresolved) {
            this.unresolved = unresolved;
        }

        public String getResolved() {
            return resolved;
        }

        public void setResolved(String resolved) {
            this.resolved = resolved;
        }
    }

    public class EmailExpression {
        private List<String> emails = new ArrayList<String>();
        private String subject = "Monitor events for $(eventTypes) from nodes $(eventNodeIds)";
        private String bodyBefore = "";
        private String bodyAfter = "";
        private String unresolved = "Monitor event for $(eventType) reached threshold of"
                + " $(eventThreshold) with a value of $(eventValue)";
        private String resolved = "Monitor event for $(eventType) is resolved";
        private List<Template> templates = new ArrayList<Template>();

        public EmailExpression() {
            templates.add(new Template("log", "Details: \n$(eventDetails)"));
            templates.add(new Template("batchError", "Details: \n$(eventDetails)"));
            templates.add(new Template("offlineNodes", "Details: \n$(eventDetails)"));
            templates.add(new Template("batchUnsent", "$(eventValue) batches unsent."));
            templates.add(new Template("dataUnrouted", "$(eventValue) unrouted data rows."));
            templates.add(new Template("dataGap", "$(eventValue) data gap(s) recorded"));
            templates.add(new Template("cpu", "Details: \n$(eventDetails)"));
            templates.add(new Template("memory", "Details: \n$(eventDetails)"));
            templates.add(new Template("disk", "Disk usage is at $(eventValue)%"));
            templates.add(new Template("block", "Details: \n$(eventDetails)"));
            templates.add(new Template("default", "Details: \n$(eventDetails)"));
        }

        public List<String> getEmails() {
            return emails;
        }

        public void setEmails(List<String> emails) {
            this.emails = emails;
        }

        public String getSubject() {
            return subject;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }

        public String getBodyBefore() {
            return bodyBefore;
        }

        public void setBodyBefore(String bodyBefore) {
            this.bodyBefore = bodyBefore;
        }

        public String getBodyAfter() {
            return bodyAfter;
        }

        public void setBodyAfter(String bodyAfter) {
            this.bodyAfter = bodyAfter;
        }

        public String getUnresolved() {
            return unresolved;
        }

        public void setUnresolved(String unresolved) {
            this.unresolved = unresolved;
        }

        public String getResolved() {
            return resolved;
        }

        public void setResolved(String resolved) {
            this.resolved = resolved;
        }

        public List<Template> getTemplates() {
            return templates;
        }

        public Map<String, String> getTemplateMap() {
            Map<String, String> templateMap = new HashMap<String, String>();
            for (Template template : templates) {
                templateMap.put(template.getName(), template.getTemplate());
            }
            return templateMap;
        }

        public void setTemplates(List<Template> templates) {
            this.templates = templates;
        }

        public void setTemplates(Map<String, String> templateMap) {
            templates = new ArrayList<Template>();
            for (String name : templateMap.keySet()) {
                templates.add(new Template(name, templateMap.get(name)));
            }
        }
    }

    public class Template {
        private String name;
        private String template;

        public Template(String name, String template) {
            this.name = name;
            this.template = template;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getTemplate() {
            return template;
        }

        public void setTemplate(String template) {
            this.template = template;
        }
    }
}
