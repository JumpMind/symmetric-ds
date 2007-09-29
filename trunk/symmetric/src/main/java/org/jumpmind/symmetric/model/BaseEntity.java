package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

import org.jumpmind.symmetric.util.IAuditableEntity;

public class BaseEntity implements Serializable, IAuditableEntity {

    private static final long serialVersionUID = -790076314026711028L;

    private Date createdOn;

    private Date lastModifiedTime;

    private String updatedBy;

    public Date getCreatedOn() {
        return createdOn;
    }

    public Date getLastModifiedTime() {
        return lastModifiedTime;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setCreatedOn(Date createdOn) {
        this.createdOn = createdOn;
    }

    public void setLastModifiedTime(Date lastModifiedOn) {
        this.lastModifiedTime = lastModifiedOn;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }
}
