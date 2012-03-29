package org.jumpmind.symmetric.core.db;


public interface ISqlRowMapper<T> {
    public T mapRow(Row row);
}
