package org.jumpmind.symmetric.core.process;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.core.common.BinaryEncoding;
import org.jumpmind.symmetric.core.model.Parameters;

public class DataContext {
    
    public static final String KEY_SQL_TRANSACTION = "sql.transaction";

    protected BinaryEncoding binaryEncoding = BinaryEncoding.NONE;

    protected Map<String, Object> context = new HashMap<String, Object>();

    protected Parameters parameters;

    public DataContext(Parameters parameters) {
        this.parameters = parameters == null ? new Parameters() : parameters;
    }

    public DataContext() {
        this(null);
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

    public void setBinaryEncoding(BinaryEncoding binaryEncoding) {
        this.binaryEncoding = binaryEncoding;
    }

    public BinaryEncoding getBinaryEncoding() {
        return binaryEncoding;
    }

}
