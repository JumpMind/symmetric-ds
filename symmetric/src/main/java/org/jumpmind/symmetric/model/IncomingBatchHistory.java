package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderStatistics;

public class IncomingBatchHistory implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Status {
        OK, ER, SK;
    }

    private String batchId;

    private String clientId;

    private Status status;

    private static String hostName;

    private long statementCount;

    private long fallbackInsertCount;

    private long fallbackUpdateCount;

    private long missingDeleteCount;
    
    private long failedRowNumber;

    private Date startTime;

    private Date endTime;

    static {
        InetAddress address = null;
        try {
            address = InetAddress.getLocalHost();
            hostName = address.getHostName();
        } catch (UnknownHostException e) {
            hostName = "UNKNOWN";
        }
    }

    public IncomingBatchHistory() {
    }

    public IncomingBatchHistory(IDataLoaderContext context) {
        batchId = context.getBatchId();
        clientId = context.getClientId();
        status = Status.OK;
        startTime = new Date();
    }
    
    public void setValues(IDataLoaderStatistics statistics, boolean isSuccess) {
        statementCount = statistics.getStatementCount();
        fallbackInsertCount = statistics.getFallbackInsertCount();
        fallbackUpdateCount = statistics.getFallbackUpdateCount();
        missingDeleteCount = statistics.getMissingDeleteCount();
        endTime = new Date();
        if (! isSuccess) {
            status = Status.ER;
            failedRowNumber = statistics.getLineCount();
        }
    }    

    public String getClientBatchId() {
        return clientId + "-" + batchId;
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public long getFailedRowNumber() {
        return failedRowNumber;
    }

    public void setFailedRowNumber(long failedRowNumber) {
        this.failedRowNumber = failedRowNumber;
    }

    public long getFallbackInsertCount() {
        return fallbackInsertCount;
    }

    public void setFallbackInsertCount(long fallbackInsertCount) {
        this.fallbackInsertCount = fallbackInsertCount;
    }

    public long getFallbackUpdateCount() {
        return fallbackUpdateCount;
    }

    public void setFallbackUpdateCount(long fallbackUpdateCount) {
        this.fallbackUpdateCount = fallbackUpdateCount;
    }

    public String getHostName() {
        return hostName;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public long getStatementCount() {
        return statementCount;
    }

    public void setStatementCount(long statementCount) {
        this.statementCount = statementCount;
    }

    public long getMissingDeleteCount() {
        return missingDeleteCount;
    }

    public void setMissingDeleteCount(long missingDeleteCount) {
        this.missingDeleteCount = missingDeleteCount;
    }

}
