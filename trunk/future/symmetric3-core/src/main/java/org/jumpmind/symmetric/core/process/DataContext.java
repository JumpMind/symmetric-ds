package org.jumpmind.symmetric.core.process;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.core.common.BinaryEncoding;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Table;

public class DataContext {

    protected Batch batch;

    protected Table table;

    protected BinaryEncoding binaryEncoding = BinaryEncoding.NONE;

    protected Map<String, Object> context = new HashMap<String, Object>();

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
