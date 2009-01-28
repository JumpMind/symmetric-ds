package org.jumpmind.symmetric.config;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DynamicPropertiesFiles extends ArrayList<String> {

    private static final long serialVersionUID = 1L;
    private static Log logger = LogFactory.getLog(DynamicPropertiesFiles.class);

    public DynamicPropertiesFiles() {
        File file = new File(System.getProperty("user.dir"),
                "symmetric.properties");
        if (file.exists() && file.isFile()) {
            try {
                add(file.toURL().toExternalForm());
            } catch (MalformedURLException e) {
                logger.error(e, e);
            }
        }
        if (!StringUtils.isBlank(System
                .getProperty("symmetric.override.properties.file.1"))) {
            add(System.getProperty("symmetric.override.properties.file.1"));
        }
        if (!StringUtils.isBlank(System
                .getProperty("symmetric.override.properties.file.2"))) {
            add(System.getProperty("symmetric.override.properties.file.2"));
        }
    }
}
