/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
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

public class OutgoingLoadSummary implements Serializable {

    private static final long serialVersionUID = 1L;

    private long loadId;
    private String nodeId;
    private boolean inError;
    private int finishedBatchCount;
    private int pendingBatchCount;
    private long currentBatchId;
    private String currentTable;
    private long currentDataEventCount;
    private String createBy;
    private Date createTime;
    private Date lastUpdateTime;
    private int reloadBatchCount;

    public boolean isActive() {
        return pendingBatchCount > 0;
    }

    public void setInError(boolean inError) {
        this.inError = inError;
    }

    public boolean isInError() {
        return inError;
    }

    public long getLoadId() {
        return loadId;
    }

    public void setLoadId(long loadId) {
        this.loadId = loadId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public int getFinishedBatchCount() {
        return finishedBatchCount;
    }

    public void setFinishedBatchCount(int finishedBatchCount) {
        this.finishedBatchCount = finishedBatchCount;
    }

    public int getPendingBatchCount() {
        return pendingBatchCount;
    }

    public void setPendingBatchCount(int pendingBatchCount) {
        this.pendingBatchCount = pendingBatchCount;
    }

    public long getCurrentBatchId() {
        return currentBatchId;
    }

    public void setCurrentBatchId(long currentBatchId) {
        this.currentBatchId = currentBatchId;
    }

    public String getCurrentTable() {
        return currentTable;
    }

    public void setCurrentTable(String currentTable) {
        this.currentTable = currentTable;
    }

    public long getCurrentDataEventCount() {
        return currentDataEventCount;
    }

    public void setCurrentDataEventCount(long currentDataEventCount) {
        this.currentDataEventCount = currentDataEventCount;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public int getReloadBatchCount() {
        return reloadBatchCount;
    }

    public void setReloadBatchCount(int reloadBatchCount) {
        this.reloadBatchCount = reloadBatchCount;
    }

    public void setCreateBy(String createBy) {
        this.createBy = createBy;
    }

    public String getCreateBy() {
        return createBy;
    }

}
