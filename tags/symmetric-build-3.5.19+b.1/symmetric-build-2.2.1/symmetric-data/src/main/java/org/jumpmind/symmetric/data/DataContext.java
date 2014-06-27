package org.jumpmind.symmetric.data;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.model.Batch;

public class DataContext {

    protected Batch batch;
    
    protected Table sourceTable;
    
    protected Map<String,Object> context = new HashMap<String, Object>();
    
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
    
    public void setSourceTable(Table table) {
        this.sourceTable = table;
    }
    
    public Table getSourceTable() {
        return sourceTable;
    }
    
    
}
