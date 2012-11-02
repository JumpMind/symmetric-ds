package org.jumpmind.db.sql;

public interface ISqlReadCursor<T> {

    public T next();

    public void close();

}
