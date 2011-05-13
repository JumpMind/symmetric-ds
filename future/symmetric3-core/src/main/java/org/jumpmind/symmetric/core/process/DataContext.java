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

    public void setBatch(Batch batch) {
        this.batch = batch;
    }

    public Batch getBatch() {
        return batch;
    }

    public Map<String, Object> getContext() {
        return context;
    }

    public void setContext(Map<String, Object> context) {
        this.context = context;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

    public void setBinaryEncoding(BinaryEncoding binaryEncoding) {
        this.binaryEncoding = binaryEncoding;
    }

    public BinaryEncoding getBinaryEncoding() {
        return binaryEncoding;
    }

}
