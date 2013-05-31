package org.jumpmind.symmetric.model;

import java.io.Serializable;

/**
 * Status of a batch acknowledgement
 */
public class BatchAck  implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private long batchId;

    private String nodeId;

    private boolean isOk;

    private long errorLine;

    private long networkMillis;

    private long filterMillis;

    private long databaseMillis;

    private long byteCount;

    private String sqlState;

    private int sqlCode;
    
    private boolean ignored = false;

    private String sqlMessage;

    public BatchAck(long batchId) {
        this.batchId = batchId;
        isOk = true;
    }

    public BatchAck(long batchId, long errorLineNumber) {
        this.batchId = batchId;
        isOk = false;
        errorLine = errorLineNumber;
    }

    public long getBatchId() {
        return batchId;
    }

    public long getErrorLine() {
        return errorLine;
    }

    public boolean isOk() {
        return isOk;
    }

    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }

    public void setErrorLine(long errorLine) {
        this.errorLine = errorLine;
    }

    public void setOk(boolean isOk) {
        this.isOk = isOk;
    }

    public long getByteCount() {
        return byteCount;
    }

    public void setByteCount(long byteCount) {
        this.byteCount = byteCount;
    }

    public long getDatabaseMillis() {
        return databaseMillis;
    }

    public void setDatabaseMillis(long databaseMillis) {
        this.databaseMillis = databaseMillis;
    }

    public long getFilterMillis() {
        return filterMillis;
    }

    public void setFilterMillis(long filterMillis) {
        this.filterMillis = filterMillis;
    }

    public long getNetworkMillis() {
        return networkMillis;
    }

    public void setNetworkMillis(long networkMillis) {
        this.networkMillis = networkMillis;
    }

    public int getSqlCode() {
        return sqlCode;
    }

    public void setSqlCode(int sqlCode) {
        this.sqlCode = sqlCode;
    }

    public String getSqlMessage() {
        return sqlMessage;
    }

    public void setSqlMessage(String sqlMessage) {
        this.sqlMessage = sqlMessage;
    }

    public String getSqlState() {
        return sqlState;
    }

    public void setSqlState(String sqlState) {
        this.sqlState = sqlState;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
    
    public void setIgnored(boolean ignored) {
        this.ignored = ignored;
    }
    
    public boolean isIgnored() {
        return ignored;
    }

}