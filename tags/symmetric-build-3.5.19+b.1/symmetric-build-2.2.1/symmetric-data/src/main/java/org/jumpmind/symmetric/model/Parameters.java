package org.jumpmind.symmetric.model;

import java.util.HashMap;

public class Parameters extends HashMap<String, String> {

    private static final long serialVersionUID = 1L;

    public long getLong(String key, long defaultValue) {
        long returnValue = defaultValue;
        String value = get(key);
        if (value != null) {
            try {
                returnValue = Long.parseLong(value);
            } catch (NumberFormatException ex) {
                 // TODO log error
            }
        }
        return returnValue;
    }

}
