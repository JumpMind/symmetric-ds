package org.jumpmind.symmetric.model;

import java.io.Serializable;

public class DatabaseParameter implements Serializable {

    private static final long serialVersionUID = 1L;

    private String key;
    private String value;
    private String externalId;
    private String nodeGroupId;

    public DatabaseParameter() {
    }

    public DatabaseParameter(String key, String value, String externalId, String nodeGroupId) {
        this.key = key;
        this.value = value;
        this.externalId = externalId;
        this.nodeGroupId = nodeGroupId;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    public String getNodeGroupId() {
        return nodeGroupId;
    }

    public void setNodeGroupId(String nodeGroupId) {
        this.nodeGroupId = nodeGroupId;
    }

}
