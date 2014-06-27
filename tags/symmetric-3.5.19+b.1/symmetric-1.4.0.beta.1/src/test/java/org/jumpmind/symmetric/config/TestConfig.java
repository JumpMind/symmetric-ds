package org.jumpmind.symmetric.config;

@SuppressWarnings("deprecation")
public class TestConfig implements IRuntimeConfig {

    static final String SCHEMA_VERSION = "1000000";

    public String getExternalId() {
        return "00000";
    }

    public String getMyUrl() {
        return "http://hello.world";
    }

    public String getNodeGroupId() {
        return "sam.adams";
    }

    public String getRegistrationUrl() {
        return getMyUrl();
    }

    public String getSchemaVersion() {
        return SCHEMA_VERSION;
    }

}
