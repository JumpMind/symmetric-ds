package org.jumpmind.symmetric.core.db.mapper;

import java.util.Map;

import org.jumpmind.symmetric.core.db.ISqlRowMapper;

public class StringMapper implements ISqlRowMapper<String> {

    public String mapRow(Map<String, Object> row) {     
        return (String)row.values().iterator().next();
    }
}
