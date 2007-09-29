package org.jumpmind.symmetric.model;

public class BatchInfo {
    public static final String OK = "OK";
    
    public static final int UNDEFINED_ERROR_LINE_NUMBER = 0;

    private String batchId;
    
    private boolean isOk;

    private long errorLine;

    public BatchInfo(String batchId) {
        this.batchId = batchId;
        isOk = true;
    }

    public BatchInfo(String batchId, long errorLineNumber) {
        this.batchId = batchId;
        isOk = false;
        errorLine = errorLineNumber;
    }

    public String getBatchId() {
        return batchId;
    }

    public long getErrorLine() {
        return errorLine;
    }

    public boolean isOk() {
        return isOk;
    }

}
