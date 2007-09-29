package org.jumpmind.symmetric.load;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

import org.springframework.transaction.annotation.Transactional;

public interface IDataLoader extends Cloneable {

    public void open(BufferedReader in) throws IOException;
    
    public void open(BufferedReader in, List<IDataLoaderFilter> filters) throws IOException;

    public boolean hasNext() throws IOException;

    @Transactional
    public void load() throws IOException;
    
    public void skip() throws IOException;

    public void close();
    
    public IDataLoader clone();

    public IDataLoaderContext getContext();
    
    public IDataLoaderStatistics getStatistics();
    
    public boolean isEnableFallbackInsert();

    public boolean isEnableFallbackUpdate();

    public boolean isAllowMissingDelete();

    public void setEnableFallbackInsert(boolean enableFallbackInsert);

    public void setEnableFallbackUpdate(boolean enableFallbackUpdate);

    public void setAllowMissingDelete(boolean allowMissingDelete);
}
