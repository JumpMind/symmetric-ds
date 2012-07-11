package org.jumpmind.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Context {

    protected Map<String, Object> context = new HashMap<String, Object>();    

    public void put(String key, Object value) {
        context.put(key, value);
    }

    public Object get(String key) {
        return context.get(key);
    }    
    
    public Object remove(String key) {
        return context.remove(key);
    }
    
    public Set<String> keySet() {
        return context.keySet();
    }
    
}
