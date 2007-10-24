/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */


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
