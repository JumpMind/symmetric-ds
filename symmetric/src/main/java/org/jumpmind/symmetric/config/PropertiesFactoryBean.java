package org.jumpmind.symmetric.config;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

public class PropertiesFactoryBean extends
        org.springframework.beans.factory.config.PropertiesFactoryBean {

    @Override
    public void setLocations(Resource[] locations) {
        List<Resource> resources = new ArrayList<Resource>();
        resources.addAll(Arrays.asList(locations));
        File file = new File(System.getProperty("user.dir"),
                "symmetric.properties");
        if (file.exists() && file.isFile()) {
            resources.add(new FileSystemResource(file));
        }
        if (!StringUtils.isBlank(System
                .getProperty("symmetric.override.properties.file.1"))) {
            resources.add(new DefaultResourceLoader().getResource(System
                    .getProperty("symmetric.override.properties.file.1")));
        }
        if (!StringUtils.isBlank(System
                .getProperty("symmetric.override.properties.file.2"))) {
            resources.add(new DefaultResourceLoader().getResource(System
                    .getProperty("symmetric.override.properties.file.2")));
        }
        super.setLocations(resources.toArray(new Resource[resources.size()]));
    }
}
