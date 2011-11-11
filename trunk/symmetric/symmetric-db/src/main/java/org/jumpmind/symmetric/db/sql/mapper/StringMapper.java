package org.jumpmind.symmetric.db.sql.mapper;

import org.jumpmind.symmetric.db.sql.ISqlRowMapper;
import org.jumpmind.symmetric.db.sql.Row;

public class StringMapper implements ISqlRowMapper<String> {

    public String mapRow(Row row) {     
        return row.stringValue();
    }
}
