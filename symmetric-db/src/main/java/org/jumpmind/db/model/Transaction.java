package org.jumpmind.db.model;

import java.util.Date;

public class Transaction {

    String id;
    
    String username;
    
    String remoteIp;
    
    String remoteHost;
    
    String status;
    
    int reads;
    
    int writes;
    
    String blockingId;
    
    Date startTime;
    
    String text;
    
    public Transaction(String id, String username, String blockingId, Date startTime, String text) {
        this.id = id;
        this.username = username;
        this.blockingId = blockingId == null ? "" : blockingId;
        this.startTime = startTime;
        this.text = text;
        this.remoteIp = this.remoteHost = this.status = "";
        this.reads = this.writes = -1;
    }
    
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
    
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
    
    public String getRemoteIp() {
        return remoteIp;
    }

    public void setRemoteIp(String remoteIp) {
        this.remoteIp = remoteIp;
    }
    
    public String getRemoteHost() {
        return remoteHost;
    }

    public void setRemoteHost(String remoteHost) {
        this.remoteHost = remoteHost;
    }
    
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
    
    public int getReads() {
        return reads;
    }

    public void setReads(int reads) {
        this.reads = reads;
    }
    
    public int getWrites() {
        return writes;
    }

    public void setWrites(int writes) {
        this.writes = writes;
    }
    
    public String getBlockingId() {
        return blockingId;
    }

    public void setBlockingId(String blockingId) {
        this.blockingId = blockingId;
    }
    
    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }
    
    public long getDuration() {
        Date now = new Date();
        return now.getTime() - startTime.getTime();
    }
    
    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
    
}
