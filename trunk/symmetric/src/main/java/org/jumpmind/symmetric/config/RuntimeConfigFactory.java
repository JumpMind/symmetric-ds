package org.jumpmind.symmetric.config;

import org.springframework.beans.factory.FactoryBean;

public class RuntimeConfigFactory implements FactoryBean {

    private String className;

    private IRuntimeConfig defaultInstance;

    public Object getObject() throws Exception {
        if (className != null && className.trim().length() > 0) {
            return Class.forName(className).newInstance();
        } else {
            return defaultInstance;
        }
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public void setDefaultInstance(IRuntimeConfig defaultInstance) {
        this.defaultInstance = defaultInstance;
    }

    public Class<IRuntimeConfig> getObjectType() {
        return IRuntimeConfig.class;
    }

    public boolean isSingleton() {
        return true;
    }

}
