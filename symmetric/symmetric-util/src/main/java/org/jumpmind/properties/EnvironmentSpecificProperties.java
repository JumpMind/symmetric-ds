package org.jumpmind.properties;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

/**
 * This extension to {@link Properties} reads in a properties file and looks for
 * any properties whose prefix matches any combination of supplied environment
 * tokens. Any matches it finds, it removes the prefix and keeps a reference to
 * the new property.
 * <p/>
 */
public class EnvironmentSpecificProperties extends TypedProperties {

    private static final long serialVersionUID = 1L;

    protected Set<String> propertiesForEnvironment;

    protected String systemPropertyName;

    protected Properties original;

    public EnvironmentSpecificProperties(String systemPropertyName) {
        this(systemPropertyName, null);
    }    

    public EnvironmentSpecificProperties(String systemPropertyName, String[] propertiesForEnv) {
        this.propertiesForEnvironment = new HashSet<String>();
        this.systemPropertyName = systemPropertyName;
        if (propertiesForEnv != null) {
            for (String property : propertiesForEnv) {
                if (property != null) {
                    this.propertiesForEnvironment.add(property);
                }
            }
        }
    }

    @Override
    public synchronized void load(InputStream inStream) throws IOException {
        this.original = new Properties();
        this.original.load(inStream);
        activate();
    }

    @Override
    public synchronized void load(Reader reader) throws IOException {
        this.original = new Properties();
        this.original.load(reader);
        activate();
    }

    protected void activate() {
        this.clear();
        if (this.original != null) {
            Set<String> properties = new HashSet<String>(this.propertiesForEnvironment);
            if (StringUtils.isNotBlank(this.systemPropertyName)) {
                String additionalActivationKeys = System.getProperty(systemPropertyName);
                if (StringUtils.isBlank(additionalActivationKeys)) {
                    additionalActivationKeys = this.original.getProperty(systemPropertyName);
                }

                if (StringUtils.isNotBlank(additionalActivationKeys)) {
                    String[] tokens = additionalActivationKeys.split(",");
                    for (String token : tokens) {
                        properties.add(token);
                    }
                }
            }

            Set<Object> keys = original.keySet();
            for (Object originalKey : keys) {
                String keyName = originalKey.toString();
                boolean foundMatch = true;
                while (foundMatch) {
                    foundMatch = false;
                    for (String property : properties) {
                        if (keyName.startsWith(property + ".")) {
                            keyName = keyName.substring(property.length() + 1, keyName.length());
                            foundMatch = true;
                        }
                    }
                }

                if (!keyName.equals(originalKey.toString())) {
                    setProperty(keyName, original.getProperty(originalKey.toString()));
                }
            }

        }
    }

    protected Set<String> getPropertyKeysThatBeginWith(String prefix) {
        HashSet<String> set = new HashSet<String>();
        Set<Object> keys = keySet();
        for (Object key : keys) {
            if (key.toString().startsWith(prefix)) {
                set.add(key.toString());
            }
        }
        return set;
    }

}
