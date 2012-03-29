package org.jumpmind.symmetric.core.process;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.core.common.BinaryEncoding;
import org.jumpmind.symmetric.core.model.Parameters;

public class DataContext {

    public static final String KEY_SQL_TRANSACTION = "sql.transaction";

    protected Map<String, Object> context = new HashMap<String, Object>();

    protected Parameters parameters;
    
    protected BinaryEncoding binaryEncoding;
    
    protected String sourceNodeId;

    public DataContext(Parameters parameters) {
        this.parameters = parameters == null ? new Parameters() : parameters;
    }

    public DataContext() {
    }

    public Parameters getParameters() {
        return parameters;
    }

    public void setParameters(Parameters parameters) {
        this.parameters = parameters;
    }

    public Map<String, Object> getContext() {
        return context;
    }

    public void put(String key, Object value) {
        context.put(key, value);
    }

    public Object get(String key) {
        return context.get(key);
    }

    public void setBinaryEncoding(BinaryEncoding binaryEncoding) {
        this.binaryEncoding = binaryEncoding;
    }
    
    public BinaryEncoding getBinaryEncoding() {
        return binaryEncoding;
    }
    
    public void setSourceNodeId(String nodeId) {
        this.sourceNodeId = nodeId;
    }
    
    public String getSourceNodeId() {
        return sourceNodeId;
    }
    
}
