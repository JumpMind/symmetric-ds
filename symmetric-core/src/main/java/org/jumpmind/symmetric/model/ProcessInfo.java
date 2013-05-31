package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Date;

import org.jumpmind.symmetric.model.ProcessInfoKey.ProcessType;

public class ProcessInfo implements Serializable, Comparable<ProcessInfo> {

    private static final long serialVersionUID = 1L;

    public static enum Status {
        NEW, QUERYING, EXTRACTING, LOADING, TRANSFERRING, ACKING, PROCESSING, DONE, ERROR;

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
                case DONE:
                    return "Done";
                case ERROR:
                    return "Error";

                default:
                    return name();
            }
        }
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
}
