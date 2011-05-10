package org.jumpmind.symmetric.core.sql.mapper;

import java.util.Map;

import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.sql.ISqlRowMapper;

public class DataMapper implements ISqlRowMapper<Data> {

    public Data mapRow(Map<String, Object> row) {
        Data data = new Data();
        data.setRowData((String)row.get("ROW_DATA"));
        return data;
    }
    
}
