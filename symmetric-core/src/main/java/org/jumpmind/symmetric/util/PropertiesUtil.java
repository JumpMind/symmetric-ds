package org.jumpmind.symmetric.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.jumpmind.symmetric.ITypedPropertiesFactory;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.util.AppUtils;

public class PropertiesUtil {
    public static final String PROPERTIES_FACTORY_CLASS_NAME = "properties.factory.class.name";

    public static String getEnginesDir() {
        String enginesDir = System.getProperty(SystemConstants.SYSPROP_ENGINES_DIR, AppUtils.getSymHome() + "/engines");
        new File(enginesDir).mkdirs();
        return enginesDir;
    }

    public static File findPropertiesFileForEngineWithName(String engineName) {
        File[] files = findEnginePropertiesFiles();
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            Properties properties = new Properties();
            try (FileInputStream is = new FileInputStream(file)) {
                properties.load(is);
                if (engineName.equals(properties.getProperty(ParameterConstants.ENGINE_NAME))) {
                    return file;
                }
            } catch (IOException ex) {
            }
        }
        return null;
    }

    public static File[] findEnginePropertiesFiles() {
        List<File> propFiles = new ArrayList<>();
        File enginesDir = new File(getEnginesDir());
        File[] files = enginesDir.listFiles();
        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                File file = files[i];
                if (file.getName().endsWith(".properties")) {
                    propFiles.add(file);
                }
            }
        }
        return propFiles.toArray(new File[propFiles.size()]);
    }

    public static ITypedPropertiesFactory createTypedPropertiesFactory(File propFile, Properties prop) {
        String propFactoryClassName = System.getProperties().getProperty(PROPERTIES_FACTORY_CLASS_NAME);
        ITypedPropertiesFactory factory = null;
        if (propFactoryClassName != null) {
            try {
                factory = (ITypedPropertiesFactory) Class.forName(propFactoryClassName).getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            factory = new TypedPropertiesFactory();
        }
        factory.init(propFile, prop);
        return factory;
    }
}
