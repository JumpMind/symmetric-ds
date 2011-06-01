package org.jumpmind.symmetric.core.db;

public interface ISqlReadCursor<T> {

    public T next();

    public void close();

}
