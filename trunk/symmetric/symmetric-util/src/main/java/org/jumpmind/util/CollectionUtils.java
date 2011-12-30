package org.jumpmind.util;

import java.util.HashMap;
import java.util.Map;

public class CollectionUtils {

    public static <T> Map<String, T> toMap(String[] keyNames, T[] values) {
        if (values != null && keyNames != null && values.length >= keyNames.length) {
            Map<String, T> map = new HashMap<String, T>(keyNames.length);
            for (int i = 0; i < keyNames.length; i++) {
                map.put(keyNames[i], values[i]);
            }
            return map;
        } else {
            return new HashMap<String, T>(0);
        }
    }

}
