package org.jumpmind.symmetric.db;

public interface ISqlReadCursor<T> {

    public T next();

    public void close();

}
