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

import java.io.Serializable;
import java.util.Date;

public class ExtractRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum ExtractStatus {
        NE, OK
    };

    private long requestId;
    private String nodeId;
    private ExtractStatus status;
    private long startBatchId;
    private long endBatchId;
    private String triggerId;
    private String routerId;
    private TriggerRouter triggerRouter;
    private Date lastUpdateTime;
    private Date createTime;
    private String queue;
    
    public long getRequestId() {
        return requestId;
    }

    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public ExtractStatus getStatus() {
        return status;
    }

    public void setStatus(ExtractStatus status) {
        this.status = status;
    }

    public long getStartBatchId() {
        return startBatchId;
    }

    public void setStartBatchId(long startBatchId) {
        this.startBatchId = startBatchId;
    }

    public long getEndBatchId() {
        return endBatchId;
    }

    public void setEndBatchId(long endBatchId) {
        this.endBatchId = endBatchId;
    }

    public TriggerRouter getTriggerRouter() {
        return triggerRouter;
    }

    public void setTriggerRouter(TriggerRouter triggerRouter) {
        this.triggerRouter = triggerRouter;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getTriggerId() {
        return triggerId;
    }

    public void setTriggerId(String triggerId) {
        this.triggerId = triggerId;
    }

    public String getRouterId() {
        return routerId;
    }

    public void setRouterId(String routerId) {
        this.routerId = routerId;
    }

}
