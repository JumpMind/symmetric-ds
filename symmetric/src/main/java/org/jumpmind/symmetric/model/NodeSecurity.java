package org.jumpmind.symmetric.model;

import java.util.Date;

public class NodeSecurity {

    private static final long serialVersionUID = 1L;
    
    private String nodeId;
    
    private String password;
    
    private boolean registrationEnabled;
    
    private Date registrationTime;

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String clientId) {
        this.nodeId = clientId;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
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
    

}
