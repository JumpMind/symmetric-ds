package org.jumpmind.db.sql.mapper;

import java.util.Date;

import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;

public class DateMapper implements ISqlRowMapper<Date> {
    
    public Date mapRow(Row rs) {
        return rs.dateValue();
    }

}
