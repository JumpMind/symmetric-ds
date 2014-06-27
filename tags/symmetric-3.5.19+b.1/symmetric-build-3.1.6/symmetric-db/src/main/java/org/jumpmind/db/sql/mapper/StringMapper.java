package org.jumpmind.db.sql.mapper;

import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;

public class StringMapper implements ISqlRowMapper<String> {

    public String mapRow(Row row) {     
        return row.stringValue();
    }
}
