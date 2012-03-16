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
    
    /**
     * Translate an array of {@link Object} to an array of {@link String} by
     * creating a new array of {@link String} and putting each of the objects
     * into the array by calling {@link Object#toString()}
     * 
     * @param orig
     *            the original array
     * @return a newly constructed string array
     */
    public static String[] toStringArray(Object[] orig) {
        String[] array = null;
        if (orig != null) {
            array = new String[orig.length];
            for (int i = 0; i < orig.length; i++) {
                if (orig[i] != null) {
                    array[i] = orig[i].toString();
                }
            }
        }
        return array;
    }

}
