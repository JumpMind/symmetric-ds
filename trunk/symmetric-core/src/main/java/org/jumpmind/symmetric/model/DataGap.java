package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

public class DataGap implements Serializable {
    
    private static final long serialVersionUID = 1L;

    public enum Status {GP,SK,OK};
    
    private long startId;
    private long endId;
    private Date createTime;

    public DataGap(long startId, long endId) {
        this.startId = startId;
        this.endId = endId;
        this.createTime = new Date();
    }
    
    public DataGap(long startId, long endId, Date createTime) {
        this.startId = startId;
        this.endId = endId;
        this.createTime = createTime;
    }

    public void setEndId(long endId) {
        this.endId = endId;
    }
    
    public long getEndId() {
        return endId;
    }
    
    public void setStartId(long startId) {
        this.startId = startId;
    }
    
    public long getStartId() {
        return startId;
    }
    
    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }
    
    public Date getCreateTime() {
        return createTime;
    }
    
    public boolean contains(DataGap gap) {
        return startId <= gap.startId && endId >= gap.endId;
    }
    
    public long gapSize() {
    	return endId-startId;
    }
    
}