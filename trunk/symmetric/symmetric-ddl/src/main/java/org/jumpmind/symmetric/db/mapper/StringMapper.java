package org.jumpmind.symmetric.db.mapper;

import org.jumpmind.symmetric.db.ISqlRowMapper;
import org.jumpmind.symmetric.db.Row;

public class StringMapper implements ISqlRowMapper<String> {

    public String mapRow(Row row) {     
        return row.stringValue();
    }
}
