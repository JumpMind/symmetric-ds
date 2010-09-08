package org.jumpmind.symmetric.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

public class PropertiesFactoryBean extends
        org.springframework.beans.factory.config.PropertiesFactoryBean {

    protected DynamicPropertiesFiles dynamicPropertiesFiles;
    
    private static Properties localProperties;

    public PropertiesFactoryBean() {
        this.setLocalOverride(true);
        if (localProperties != null) {
            this.setProperties(localProperties);
        }
    }
    
    public static void setLocalProperties(Properties localProperties) {
        PropertiesFactoryBean.localProperties = localProperties;
    }
    
    public static void clearLocalProperties() {
        PropertiesFactoryBean.localProperties = null;
    }
    
    @Override
    public void setLocations(Resource[] locations) {
        List<Resource> resources = new ArrayList<Resource>();
        resources.addAll(Arrays.asList(locations));
        if (dynamicPropertiesFiles != null) {
            for (String resource : dynamicPropertiesFiles) {
                resources
                        .add(new DefaultResourceLoader().getResource(resource));
            }
        }

        super.setLocations(resources.toArray(new Resource[resources.size()]));
    }    
    

    public void setDynamicPropertiesFiles(
            DynamicPropertiesFiles dynamicPropertiesFiles) {
        this.dynamicPropertiesFiles = dynamicPropertiesFiles;
    }
}
