package org.jumpmind.symmetric.ext;

import java.util.Map;

public interface ICacheContext {

    public Map<String, Object> getContextCache();
    
    public String getNodeId();
}
