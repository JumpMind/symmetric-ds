package org.jumpmind.symmetric.db.sql;

public interface ISqlReadCursor<T> {

    public T next();

    public void close();

}
