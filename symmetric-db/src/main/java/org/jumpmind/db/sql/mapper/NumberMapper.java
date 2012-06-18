package org.jumpmind.db.sql.mapper;

import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;

public class NumberMapper implements ISqlRowMapper<Number> {
    public Number mapRow(Row rs) {
        return rs.numberValue();
    }
}
