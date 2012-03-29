package org.jumpmind.symmetric.core.db.mapper;

import org.jumpmind.symmetric.core.db.ISqlRowMapper;
import org.jumpmind.symmetric.core.db.Row;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.DataEventType;

public class DataMapper implements ISqlRowMapper<Data> {

    public Data mapRow(Row row) {
        Data data = new Data();
        data.setRowData((String) row.get("ROW_DATA"));
        data.setEventType(DataEventType.getEventType((String) row.get("EVENT_TYPE")));
        return data;
    }

}
