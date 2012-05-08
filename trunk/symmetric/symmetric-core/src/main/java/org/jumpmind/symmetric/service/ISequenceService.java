package org.jumpmind.symmetric.service;

import org.jumpmind.symmetric.model.Sequence;

public interface ISequenceService {

    public long nextVal(String name);

    public long currVal(String name);

    public void create(Sequence sequence);

    public Sequence get(String name);
    
}
