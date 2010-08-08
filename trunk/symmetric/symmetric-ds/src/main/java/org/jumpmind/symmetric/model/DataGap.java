package org.jumpmind.symmetric.model;

import java.util.Date;

public class DataGap {

    public enum STATUS {GP,SK,FL};
    
    public final static long OPEN_END_ID = -1;
    
    long startId;
    long endId;
    Date createTime;

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
    
}
