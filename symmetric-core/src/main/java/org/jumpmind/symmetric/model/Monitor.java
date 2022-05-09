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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.common.ParameterConstants;

public class Monitor implements IModelObject {
    private static final long serialVersionUID = 1L;
    public static final int INFO = 100;
    public static final int WARNING = 200;
    public static final int SEVERE = 300;
    public static final String INFO_NAME = "INFO";
    public static final String WARNING_NAME = "WARNING";
    public static final String SEVERE_NAME = "SEVERE";
    public static Map<Integer, String> severityLevelNames;
    protected String monitorId;
    protected String nodeGroupId;
    protected String externalId;
    protected String type;
    protected String expression;
    protected long threshold;
    protected int runPeriod;
    protected int runCount;
    protected int severityLevel;
    protected boolean enabled;
    protected Date createTime;
    protected String lastUpdateBy;
    protected Date lastUpdateTime;
    protected transient String targetNode;
    static {
        severityLevelNames = new HashMap<Integer, String>();
        severityLevelNames.put(INFO, "INFO");
        severityLevelNames.put(WARNING, "WARNING");
        severityLevelNames.put(SEVERE, "SEVERE");
    }

    public Monitor() {
    }

    public String getMonitorId() {
        return monitorId;
    }

    public void setMonitorId(String monitorId) {
        this.monitorId = monitorId;
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

    public long getThreshold() {
        return threshold;
    }

    public void setThreshold(long threshhold) {
        this.threshold = threshhold;
    }

    public int getRunPeriod() {
        return runPeriod;
    }

    public void setRunPeriod(int runPeriod) {
        this.runPeriod = runPeriod;
    }

    public int getRunCount() {
        return runCount;
    }

    public void setRunCount(int runCount) {
        this.runCount = runCount;
    }

    public int getSeverityLevel() {
        return severityLevel;
    }

    public void setSeverityLevel(int severityLevel) {
        this.severityLevel = severityLevel;
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

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public static Map<Integer, String> getSeverityLevelNames() {
        return severityLevelNames;
    }

    public String getSeverityLevelName() {
        String name = severityLevelNames.get(severityLevel);
        if (name == null) {
            name = INFO_NAME;
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((monitorId == null) ? 0 : monitorId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Monitor other = (Monitor) obj;
        if (monitorId == null) {
            if (other.monitorId != null)
                return false;
        } else if (!monitorId.equals(other.monitorId))
            return false;
        return true;
    }
}
