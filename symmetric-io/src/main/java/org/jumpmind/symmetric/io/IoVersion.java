package org.jumpmind.symmetric.io;

import org.jumpmind.util.AbstractVersion;

public class IoVersion extends AbstractVersion {

    private static IoVersion version = new IoVersion();
    
    @Override
    protected String getPropertiesFileLocation() {
        return "/META-INF/maven/org.jumpmind.symmetric/symmetric-io/pom.properties";
    }
    
    public static IoVersion getVersion() {
        return version;
    }

}
