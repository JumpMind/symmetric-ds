package org.jumpmind.symmetric.web.rest;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class RegistrationInfo {

    String nodeGroupId;
    String externalId;

    public RegistrationInfo() {
    }

    public RegistrationInfo(String groupId, String externalId) {
        this.nodeGroupId = groupId;
        this.externalId = externalId;
    }

    public String getNodeGroupId() {
        return nodeGroupId;
    }

    public void setNodeGroupId(String groupId) {
        this.nodeGroupId = groupId;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

}
