package org.jumpmind.symmetric.core.db.mapper;

import org.jumpmind.symmetric.core.db.ISqlRowMapper;
import org.jumpmind.symmetric.core.db.Row;

public class StringMapper implements ISqlRowMapper<String> {

    public String mapRow(Row row) {     
        return row.stringValue();
    }
}
