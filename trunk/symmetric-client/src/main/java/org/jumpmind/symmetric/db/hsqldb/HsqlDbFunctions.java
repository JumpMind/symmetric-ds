package org.jumpmind.symmetric.db.hsqldb;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.db.EmbeddedDbFunctions;

/**
 * 
 */
public class HsqlDbFunctions extends EmbeddedDbFunctions {

    private static Map<String, ThreadLocal<String>> sessionVariables = new HashMap<String, ThreadLocal<String>>();

    public static void setSession(String key, String value) {
        ThreadLocal<String> local = sessionVariables.get(key);
        if (local == null) {
            local = new ThreadLocal<String>();
            sessionVariables.put(key, local);
        }
        if (value != null) {
            local.set(value);
        } else {
            local.remove();
        }
    }

    public static String getSession(String key) {
        ThreadLocal<String> local = sessionVariables.get(key);
        if (local != null) {
            return local.get();
        } else {
            return null;
        }
    }

}