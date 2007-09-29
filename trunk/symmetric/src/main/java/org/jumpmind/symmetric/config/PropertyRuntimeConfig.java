package org.jumpmind.symmetric.config;

import java.util.Properties;

public class PropertyRuntimeConfig implements IRuntimeConfig {

    String groupId;

    String externalId;

    String registrationUrlString;

    String myUrlString;
    
    String schemaVersion;

    Properties properties = new Properties();

    public String getNodeGroupId() {
        return groupId;
    }

    public String getRegistrationUrl() {
        return registrationUrlString;
    }

    public Properties getParameters() {
        return properties;
    }

    public void setGroupId(String domainName) {
        this.groupId = domainName;
    }

    public void setRegistrationUrlString(String registrationUrlString) {
        this.registrationUrlString = registrationUrlString;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String domainId) {
        this.externalId = domainId;
    }

    public void setMyUrlString(String myUrlString) {
        this.myUrlString = myUrlString;
    }

    public String getMyUrl() {
        return myUrlString;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

}
