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

import org.apache.commons.lang3.math.NumberUtils;

public class JobDefinition {
    public enum JobType {
        BUILT_IN, BSH, JAVA, SQL
    }

    private String jobName;
    private JobType jobType;
    private boolean requiresRegistration;
    private String jobExpression;
    private String description;
    private String createBy;
    private Date createTime;
    private String lastUpdateBy;
    private Date lastUpdateTime;
    private boolean defaultAutomaticStartup;
    private String defaultSchedule;
    private String nodeGroupId;
    private transient boolean automaticStartup = true;
    private transient String schedule;

    public boolean isCronSchedule() {
        return !isPeriodicSchedule();
    }

    public boolean isPeriodicSchedule() {
        return NumberUtils.isDigits(schedule);
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public JobType getJobType() {
        return jobType;
    }

    public void setJobType(JobType jobType) {
        this.jobType = jobType;
    }

    public boolean isRequiresRegistration() {
        return requiresRegistration;
    }

    public void setRequiresRegistration(boolean requiresRegistration) {
        this.requiresRegistration = requiresRegistration;
    }

    public String getJobExpression() {
        return jobExpression;
    }

    public void setJobExpression(String jobExpression) {
        this.jobExpression = jobExpression;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCreateBy() {
        return createBy;
    }

    public void setCreateBy(String createBy) {
        this.createBy = createBy;
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

    public boolean isDefaultAutomaticStartup() {
        return defaultAutomaticStartup;
    }

    public void setDefaultAutomaticStartup(boolean defaultAutomaticStartup) {
        this.defaultAutomaticStartup = defaultAutomaticStartup;
    }

    public String getDefaultSchedule() {
        return defaultSchedule;
    }

    public void setDefaultSchedule(String defaultSchedule) {
        this.defaultSchedule = defaultSchedule;
    }

    public String getNodeGroupId() {
        return nodeGroupId;
    }

    public void setNodeGroupId(String nodeGroupId) {
        this.nodeGroupId = nodeGroupId;
    }

    public String getSchedule() {
        return schedule;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
    }

    public boolean isAutomaticStartup() {
        return automaticStartup;
    }

    public void setAutomaticStartup(boolean automaticStartup) {
        this.automaticStartup = automaticStartup;
    }

    public static String getJobNameParameter(String name) {
        if (name != null) {
            return name.toLowerCase().replace(' ', '.');
        } else {
            return null;
        }
    }

    public String getStartParameter() {
        return String.format("start.%s.job", getJobNameParameter(jobName));
    }

    public String getPeriodicParameter() {
        return String.format("job.%s.period.time.ms", getJobNameParameter(jobName));
    }

    public String getCronParameter() {
        return String.format("job.%s.cron", getJobNameParameter(jobName));
    }
}
