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
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.model.ProcessInfoKey.ProcessType;

public class ProcessInfo implements Serializable, Comparable<ProcessInfo>, Cloneable {

    private static final long serialVersionUID = 1L;

    public static enum Status {
        NEW, QUERYING, EXTRACTING, LOADING, TRANSFERRING, ACKING, PROCESSING, OK, ERROR, CREATING;

        public String toString() {
            switch (this) {
                case NEW:
                    return "New";
                case QUERYING:
                    return "Querying";
                case EXTRACTING:
                    return "Extracting";
                case LOADING:
                    return "Loading";
                case TRANSFERRING:
                    return "Transferring";
                case ACKING:
                    return "Acking";
                case PROCESSING:
                    return "Processing";
                case OK:
                    return "Ok";
                case ERROR:
                    return "Error";
                case CREATING:
                    return "Creating";

                default:
                    return name();
            }
        }
    };

    private ProcessInfoKey key;

    private Status status = Status.NEW;

    private long currentDataCount;
    
    private long dataCount = -1;

    private long batchCount;

    private long currentBatchId;

    private long currentBatchCount;
    
    private String currentChannelId;

    private boolean threadPerChannel;
    
    private String currentTableName;

    private transient Thread thread;
    
    private Date currentBatchStartTime;
    
    private long currentLoadId;

    private Date startTime = new Date();

    private Date lastStatusChangeTime = new Date();

    private Map<Status, ProcessInfo> statusHistory;
    
    private Date endTime;

    public ProcessInfo() {
        this(new ProcessInfoKey("", "", null));
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
        if (statusHistory == null) {
        	statusHistory = new HashMap<Status, ProcessInfo>();
        }
    	statusHistory.put(this.status, this.copy());
        statusHistory.put(status, this);
        
    	this.status = status;
        
        this.lastStatusChangeTime = new Date();
        if (status == Status.OK || status == Status.ERROR) {
            this.endTime = new Date();
        }
    }

    public long getCurrentDataCount() {
        return currentDataCount;
    }

    public void setCurrentDataCount(long dataCount) {
        this.currentDataCount = dataCount;
    }

    public long getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(long batchCount) {
        this.batchCount = batchCount;
    }

    public void incrementCurrentDataCount() {
        this.currentDataCount++;
    }

    public void incrementBatchCount() {
        this.batchCount++;
    }
    
    public void incrementCurrentBatchCount() {
        this.currentBatchCount++;
    }
    
    public long getCurrentBatchCount() {
		return currentBatchCount;
	}

	public void setCurrentBatchCount(long currentBatchCount) {
		this.currentBatchCount = currentBatchCount;
	}

	public long getCurrentBatchId() {
        return currentBatchId;
    }

    public void setCurrentBatchId(long currentBatchId) {
        this.currentBatchId = currentBatchId;
        this.currentBatchStartTime = new Date();
        this.currentDataCount = 0;
    }
    
    public void setCurrentLoadId(long loadId) {
        this.currentLoadId = loadId;
    }
    
    public long getCurrentLoadId() {
        return currentLoadId;
    }

    public String getCurrentChannelThread() {
    	if (getKey().getChannelId() != null && getKey().getChannelId().length() > 0) {
    		return getKey().getChannelId();
    	}
        return "";
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
    
    public void setDataCount(long dataCount) {
        this.dataCount = dataCount;
    }
    
    public long getDataCount() {
        return dataCount;
    }
    
    public boolean isThreadPerChannel() {
		return threadPerChannel;
	}

	public void setThreadPerChannel(boolean threadPerChannel) {
		this.threadPerChannel = threadPerChannel;
	}

	public Date getCurrentBatchStartTime() {
        if (currentBatchStartTime == null) {
            return startTime;
        } else {
            return currentBatchStartTime;
        }
    }
    
    public void setCurrentBatchStartTime(Date currentBatchStartTime) {
        this.currentBatchStartTime = currentBatchStartTime;
    }

    public Map<Status, ProcessInfo> getStatusHistory() {
    	return this.statusHistory;
    }
    
    public ProcessInfo getStatusHistory(Status status) {
    	return this.statusHistory == null ? null : this.statusHistory.get(status);
    }
    
    @Override
    public String toString() {
        return String.format("%s,status=%s,startTime=%s", key.toString(), status.toString(),
                startTime.toString());
    }
    
    public String showInError(String identityNodeId) {
        if (status == Status.ERROR) {
        switch (key.getProcessType()) {
            case MANUAL_LOAD:
                return null;
            case PUSH_JOB:
                return key.getTargetNodeId();
            case PULL_JOB:
                return key.getSourceNodeId();
            case PUSH_HANDLER:
                return key.getSourceNodeId();
            case PULL_HANDLER:
                return key.getTargetNodeId();
            case ROUTER_JOB:
                return key.getSourceNodeId();
            case ROUTER_READER:
                return key.getSourceNodeId();
            case GAP_DETECT:
                return key.getSourceNodeId();
            default:
                return null;
        }
        } else {
            return null;
        }
    }

    public int compareTo(ProcessInfo o) {
        if (status == Status.ERROR && o.status != Status.ERROR) {
            return -1;
        } else if (o.status == Status.ERROR && status != Status.ERROR) {
            return 1;
        } else if (status != Status.OK && o.status == Status.OK) {
            return -1;
        } else if (o.status != Status.OK && status == Status.OK) {
            return 1;
        } else {
            return o.startTime.compareTo(startTime);
        }
    }    

    public ThreadData getThreadData() {
        if (thread != null && thread.isAlive()) {
            return getThreadData(thread.getId());
        } else {
            return null;
        }
    }

    public static ThreadData getThreadData(long threadId) {
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        ThreadInfo info = threadBean.getThreadInfo(threadId, 100);
        if (info != null) {
            String threadName = info.getThreadName();
            StringBuilder formattedTrace = new StringBuilder();
            StackTraceElement[] trace = info.getStackTrace();
            for (StackTraceElement stackTraceElement : trace) {
                formattedTrace.append(stackTraceElement.getClassName());
                formattedTrace.append(".");
                formattedTrace.append(stackTraceElement.getMethodName());
                formattedTrace.append("()");
                int lineNumber = stackTraceElement.getLineNumber();
                if (lineNumber > 0) {
                    formattedTrace.append(": ");
                    formattedTrace.append(stackTraceElement.getLineNumber());
                }
                formattedTrace.append("\n");
            }

            return new ThreadData(threadName, formattedTrace.toString());
        } else {
            return null;
        }
    }

    static public class ThreadData {

        public ThreadData(String threadName, String stackTrace) {
            this.threadName = threadName;
            this.stackTrace = stackTrace;
        }

        private String threadName;
        private String stackTrace;

        public String getStackTrace() {
            return stackTrace;
        }

        public String getThreadName() {
            return threadName;
        }
    }
    
    public ProcessInfo copy() {
        try {
            return (ProcessInfo)this.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
