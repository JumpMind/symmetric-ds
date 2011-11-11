package org.jumpmind.symmetric.db.sql;


public interface ISqlRowMapper<T> {
    public T mapRow(Row row);
}
