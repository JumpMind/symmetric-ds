package org.jumpmind.symmetric.model;

import java.io.Serializable;

public class ProcessInfoKey implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum ProcessType {
        PUSH_JOB, PULL_JOB, PUSH_HANDLER, PULL_HANDLER, ROUTER_JOB, GAP_DETECT, ROUTER_READER, MANUAL_LOAD, FILE_SYNC_PULL_JOB, FILE_SYNC_PUSH_JOB, FILE_SYNC_PULL_HANDLER, FILE_SYNC_PUSH_HANDLER;

        public String toString() {
            switch (this) {
                case MANUAL_LOAD:
                    return "Manual Load";
                case PUSH_JOB:
                    return "Push";
                case PULL_JOB:
                    return "Pull";
                case PUSH_HANDLER:
                    return "Service Push";
                case PULL_HANDLER:
                    return "Service Pull";
                case ROUTER_JOB:
                    return "Routing";
                case ROUTER_READER:
                    return "Routing Reader";
                case GAP_DETECT:
                    return "Gap Detection";
                case FILE_SYNC_PULL_JOB:
                    return "File Sync Pull";
                case FILE_SYNC_PUSH_JOB:
                    return "File Sync Push";
                case FILE_SYNC_PULL_HANDLER:
                    return "Service File Sync Pull";
                case FILE_SYNC_PUSH_HANDLER:
                    return "Service File Sync Push";
                default:
                    return name();
            }
        }
    };

    private String sourceNodeId;

    private String targetNodeId;

    private ProcessType processType;

    public ProcessInfoKey(String sourceNodeId, String targetNodeId, ProcessType processType) {
        this.sourceNodeId = sourceNodeId;
        this.targetNodeId = targetNodeId;
        this.processType = processType;
    }

    public String getSourceNodeId() {
        return sourceNodeId;
    }

    public String getTargetNodeId() {
        return targetNodeId;
    }

    public ProcessType getProcessType() {
        return processType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((processType == null) ? 0 : processType.hashCode());
        result = prime * result + ((sourceNodeId == null) ? 0 : sourceNodeId.hashCode());
        result = prime * result + ((targetNodeId == null) ? 0 : targetNodeId.hashCode());
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
        ProcessInfoKey other = (ProcessInfoKey) obj;
        if (processType != other.processType)
            return false;
        if (sourceNodeId == null) {
            if (other.sourceNodeId != null)
                return false;
        } else if (!sourceNodeId.equals(other.sourceNodeId))
            return false;
        if (targetNodeId == null) {
            if (other.targetNodeId != null)
                return false;
        } else if (!targetNodeId.equals(other.targetNodeId))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return String.format("processType=%s,sourceNodeId=%s,targetNodeId=%s",
                processType.toString(), sourceNodeId, targetNodeId);
    }

}
