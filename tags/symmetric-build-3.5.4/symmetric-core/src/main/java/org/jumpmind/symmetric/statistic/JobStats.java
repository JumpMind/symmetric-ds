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
package org.jumpmind.symmetric.statistic;

import java.util.Date;

public class JobStats extends AbstractNodeHostStats {

    private String jobName;
    private long processedCount;

    public JobStats() {
    }

    public JobStats(String nodeId, String hostName, Date startTime, Date endTime, String jobName) {
        super(nodeId, hostName, startTime, endTime);
        this.jobName = jobName;
    }

    public JobStats(String nodeId, String hostName, Date startTime, Date endTime, String jobName,
            long processedCount) {
        this(nodeId, hostName, startTime, endTime, jobName);
        this.processedCount = processedCount;
    }
    
    public JobStats(String jobName, long startTime, long endTime, long processedCount) {
        this(null, null, new Date(startTime), new Date(endTime), jobName);
        this.processedCount = processedCount;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String channelId) {
        this.jobName = channelId;
    }

    public long getProcessedCount() {
        return processedCount;
    }

    public void setProcessedCount(long processedCount) {
        this.processedCount = processedCount;
    }

}