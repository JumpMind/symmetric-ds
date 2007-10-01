
package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

public class OutgoingBatchHistory implements Serializable
{
    private static final long serialVersionUID = 1L;

    public enum Status
    {
        OK, ER, SK;
    }

    private int batchId;

    private Status status;
    
    private long dataEventCount;
    
    private long failedDataId;
    
    private Date eventTime;

    public OutgoingBatchHistory()
    {
    }

    public int getBatchId()
    {
        return batchId;
    }

    public void setBatchId(int batchId)
    {
        this.batchId = batchId;
    }

    public Date getEventTime()
    {
        return eventTime;
    }

    public void setEventTime(Date complete)
    {
        this.eventTime = complete;
    }

    public long getDataEventCount()
    {
        return dataEventCount;
    }

    public void setDataEventCount(long dataEventCount)
    {
        this.dataEventCount = dataEventCount;
    }

    public long getFailedDataId()
    {
        return failedDataId;
    }

    public void setFailedDataId(long failedDataId)
    {
        this.failedDataId = failedDataId;
    }

    public Status getStatus()
    {
        return status;
    }

    public void setStatus(Status status)
    {
        this.status = status;
    }
    
    

}
