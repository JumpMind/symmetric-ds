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

import org.jumpmind.symmetric.model.ProcessInfoKey.ProcessType;

public class ProcessInfo implements Serializable, Comparable<ProcessInfo> {

    private static final long serialVersionUID = 1L;

    public static enum Status {
        NEW, EXTRACTING, LOADING, TRANSFERRING, ACKING, DONE, ERROR
    };

    private ProcessInfoKey key;

    private Status status = Status.NEW;

    private long dataCount;

    private long batchCount;

    private long currentBatchId;

    private String currentChannelId;

    private String currentTableName;

    private transient Thread thread;

    private Date startTime = new Date();

    private Date lastStatusChangeTime = new Date();

    private Date endTime;

    public ProcessInfo() {
        this(new ProcessInfoKey("", "", ProcessInfoKey.ProcessType.TEST));
    }

    public ProcessInfo(ProcessInfoKey key) {
        this.key = key;
        thread = Thread.currentThread();
    }

    public String getSourceNodeId() {
        return this.key.getSourceNodeId();
    }

    public String getTargetNodeId() {
        return this.key.getTargetNodeId();
    }

    public ProcessType getProcessType() {
        return this.key.getProcessType();
    }

    public ProcessInfoKey getKey() {
        return key;
    }

    public void setKey(ProcessInfoKey key) {
        this.key = key;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
        this.lastStatusChangeTime = new Date();
        if (status == Status.DONE || status == Status.ERROR) {
            this.endTime = new Date();
        }
    }

    public long getDataCount() {
        return dataCount;
    }

    public void setDataCount(long dataCount) {
        this.dataCount = dataCount;
    }

    public long getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(long batchCount) {
        this.batchCount = batchCount;
    }

    public void incrementDataCount() {
        this.dataCount++;
    }

    public void incrementBatchCount() {
        this.batchCount++;
    }

    public long getCurrentBatchId() {
        return currentBatchId;
    }

    public void setCurrentBatchId(long currentBatchId) {
        this.currentBatchId = currentBatchId;
    }

    public String getCurrentChannelId() {
        return currentChannelId;
    }

    public void setCurrentChannelId(String currentChannelId) {
        this.currentChannelId = currentChannelId;
    }

    public Thread getThread() {
        return thread;
    }

    public void setThread(Thread thread) {
        this.thread = thread;
    }

    public Date getStartTime() {
        return startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public void setCurrentTableName(String currentTableName) {
        this.currentTableName = currentTableName;
    }

    public String getCurrentTableName() {
        return currentTableName;
    }

    public Date getLastStatusChangeTime() {
        return lastStatusChangeTime;
    }

    @Override
    public String toString() {
        return String.format("%s,status=%s,startTime=%s", key.toString(), status.toString(),
                startTime.toString());
    }

    public int compareTo(ProcessInfo o) {
        if (status == Status.ERROR && o.status == Status.DONE) {
            return -1;
        } else if (status == Status.DONE && o.status == Status.ERROR) {
            return 1;
        } else if ((status != Status.DONE && status != Status.ERROR)
                && (o.status == Status.DONE || o.status == Status.ERROR)) {
            return -1;
        } else if ((o.status != Status.DONE && o.status != Status.ERROR)
                && (status == Status.DONE || status == Status.ERROR)) {
            return 1;
        } else {
            return startTime.compareTo(o.startTime);
        }
    }
}
