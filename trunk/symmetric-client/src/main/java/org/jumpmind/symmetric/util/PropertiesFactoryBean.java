package org.jumpmind.symmetric.util;

public class PropertiesFactoryBean extends
        org.springframework.beans.factory.config.PropertiesFactoryBean {

    public PropertiesFactoryBean() {
        this.setLocalOverride(true);
    }

}