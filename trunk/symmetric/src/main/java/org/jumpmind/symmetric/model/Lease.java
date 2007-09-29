package org.jumpmind.symmetric.model;

import java.util.Calendar;

public class Lease {
    private String resourceId;

    private String leaseType;

    private Calendar leased;

    private Calendar leaseExpires;

    // public getters

    public Calendar getLeased() {
        return leased;
    }

    public Calendar getLeaseExpires() {
        return leaseExpires;
    }

    public String getResourceId() {
        return resourceId;
    }

    public String getLeaseType() {
        return leaseType;
    }

    // protected setters

    public void setLeased(Calendar leased) {
        this.leased = leased;
    }

    public void setLeaseExpires(Calendar leaseExpires) {
        this.leaseExpires = leaseExpires;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public void setLeaseType(String leaseType) {
        this.leaseType = leaseType;
    }

}
