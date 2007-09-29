package org.jumpmind.symmetric.model;

import java.util.Calendar;


public class SimpleLease extends Lease {
    // package only visibility
    SimpleLease() {
    }

    public void setLeased(Calendar leased) {
        super.setLeased(leased);
    }

    public void setLeaseExpires(Calendar leaseExpires) {
        super.setLeaseExpires(leaseExpires);
    }

    public void setResourceId(String id) {
        super.setResourceId(id);
    }

    public void setLeaseType(String leaseType) {
        super.setLeaseType(leaseType);
    }
}
