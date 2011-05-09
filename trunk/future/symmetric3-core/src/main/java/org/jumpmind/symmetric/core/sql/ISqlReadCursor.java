package org.jumpmind.symmetric.core.sql;

public interface ISqlReadCursor<T> {

    public T next();
    public void close();
    
}
