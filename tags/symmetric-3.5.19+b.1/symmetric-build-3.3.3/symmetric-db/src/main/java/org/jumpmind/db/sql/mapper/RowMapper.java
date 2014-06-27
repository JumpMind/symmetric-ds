package org.jumpmind.db.sql.mapper;

import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;

public class RowMapper implements ISqlRowMapper<Row> {

    public Row mapRow(Row rs) {
        return rs;
    }
    
}
