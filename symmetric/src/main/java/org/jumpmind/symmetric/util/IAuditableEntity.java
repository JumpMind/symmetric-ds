package org.jumpmind.symmetric.util;

import java.util.Date;

/**
 * Interface for tracking modifications on entities
 * @author jkrajewski
 */
public interface IAuditableEntity {
    public Date getCreatedOn();

    public Date getLastModifiedTime();

    public String getUpdatedBy();

    public void setCreatedOn(Date createdOn);

    public void setLastModifiedTime(Date lastModifiedOn);

    public void setUpdatedBy(String updatedBy);
}
