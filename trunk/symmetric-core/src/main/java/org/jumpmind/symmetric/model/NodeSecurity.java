
package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

/**
 * Represents the status of a node.
 */
public class NodeSecurity implements Serializable {

    private static final long serialVersionUID = 1L;

    private String nodeId;

    private String nodePassword;

    private boolean registrationEnabled;

    private Date registrationTime;
    
    private boolean initialLoadEnabled;

    private Date initialLoadTime;
    
    private long initialLoadId;
    
    private String initialLoadCreateBy;
    
    private boolean revInitialLoadEnabled;
    
    private Date revInitialLoadTime;
    
    private long revInitialLoadId;
    
    private String revInitialLoadCreateBy;
    
    private String createdAtNodeId;

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodePassword() {
        return nodePassword;
    }

    public void setNodePassword(String password) {
        this.nodePassword = password;
    }

    public boolean isRegistrationEnabled() {
        return registrationEnabled;
    }

    public void setRegistrationEnabled(boolean registrationEnabled) {
        this.registrationEnabled = registrationEnabled;
    }

    public Date getRegistrationTime() {
        return registrationTime;
    }

    public void setRegistrationTime(Date registrationTime) {
        this.registrationTime = registrationTime;
    }

    public boolean isInitialLoadEnabled() {
        return initialLoadEnabled;
    }

    public void setInitialLoadEnabled(boolean initialLoadEnabled) {
        this.initialLoadEnabled = initialLoadEnabled;
    }

    public Date getInitialLoadTime() {
        return initialLoadTime;
    }

    public void setInitialLoadTime(Date initialLoadTime) {
        this.initialLoadTime = initialLoadTime;
    }
    
    public void setRevInitialLoadEnabled(boolean reverseInitialLoadEnabled) {
        this.revInitialLoadEnabled = reverseInitialLoadEnabled;
    }
    
    public Date getRevInitialLoadTime() {
        return revInitialLoadTime;
    }
    
    public void setRevInitialLoadTime(Date reverseInitialLoadTime) {
        this.revInitialLoadTime = reverseInitialLoadTime;
    }
    
    public boolean isRevInitialLoadEnabled() {
        return revInitialLoadEnabled;
    }

    public String getCreatedAtNodeId() {
        return createdAtNodeId;
    }

    public void setCreatedAtNodeId(String createdByNodeId) {
        this.createdAtNodeId = createdByNodeId;
    }
    
    public boolean hasRegistered() {
        return this.registrationTime  != null;
    }
    
    public boolean hasInitialLoaded() {
        return this.initialLoadTime != null;
    }
    
    public boolean hasReverseInitialLoaded() {
        return this.revInitialLoadTime != null;
    }
    
    public void setInitialLoadCreateBy(String initialLoadCreateBy) {
        this.initialLoadCreateBy = initialLoadCreateBy;
    }
    
    public String getInitialLoadCreateBy() {
        return initialLoadCreateBy;
    }
    
    public void setInitialLoadId(long initialLoadId) {
        this.initialLoadId = initialLoadId;
    }
   
    public long getInitialLoadId() {
        return initialLoadId;
    }
    
    public void setRevInitialLoadCreateBy(String revInitialLoadCreateBy) {
        this.revInitialLoadCreateBy = revInitialLoadCreateBy;
    }
    
    public String getRevInitialLoadCreateBy() {
        return revInitialLoadCreateBy;
    }
    
    public void setRevInitialLoadId(long revInitialLoadId) {
        this.revInitialLoadId = revInitialLoadId;
    }
    
    public long getRevInitialLoadId() {
        return revInitialLoadId;
    }

}