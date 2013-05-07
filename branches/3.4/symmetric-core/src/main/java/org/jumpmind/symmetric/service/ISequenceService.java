package org.jumpmind.symmetric.service;

import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.model.Sequence;

public interface ISequenceService {

    public long nextVal(String name);

    public long nextVal(ISqlTransaction transaction, String name);

    public long currVal(String name);

    public long currVal(ISqlTransaction transaction, String name);

    public void create(Sequence sequence);
    
    public void init();
    
}
