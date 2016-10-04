package org.jumpmind.db.sql;

public class RowMapper implements ISqlRowMapper<Row> {

    public RowMapper() {
    }
    
    @Override
    public Row mapRow(Row row) {
        return row;
    }

}
