package org.jumpmind.symmetric.db;


public interface ISqlRowMapper<T> {
    public T mapRow(Row row);
}
