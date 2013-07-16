package org.jumpmind.db.sql;


public interface ISqlRowMapper<T> {
    public T mapRow(Row rs);
}
