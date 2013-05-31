package org.jumpmind.symmetric;

import org.jumpmind.util.AbstractVersion;

/**
 * Follow the Apache versioning scheme documented <a
 * href="http://apr.apache.org/versioning.html">here</a>.
 */
final public class Version {

    private static AbstractVersion version = new AbstractVersion() {
        @Override
        protected String getPropertiesFileLocation() {
            return "/META-INF/maven/org.jumpmind.symmetric/symmetric-core/pom.properties";
        }
    };

    public static String version() {
        return version.version();
    }

    public static String versionWithUnderscores() {
        return version.versionWithUnderscores();
    }

    public static int[] parseVersion(String version) {
        return Version.version.parseVersion(version);
    }    

    public static boolean isOlderVersion(String version) {
        return isOlderThanVersion(version, version());
    }

    public static boolean isOlderThanVersion(String checkVersion, String targetVersion) {
        return version.isOlderThanVersion(checkVersion, targetVersion);
    }
       
    public static boolean hasOlderMinorVersion(String version) {
        return Version.version.isOlderMinorVersion(version);
    }



}