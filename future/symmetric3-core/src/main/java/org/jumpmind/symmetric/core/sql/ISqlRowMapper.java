package org.jumpmind.symmetric.core.sql;

import java.util.Map;

public interface ISqlRowMapper<T> {
    public T mapRow(Map<String, Object> row);
}
