package org.jumpmind.db.sql;

public class TableNotFoundException extends SqlException {

    private static final long serialVersionUID = 1L;

    public TableNotFoundException(String columnName) {
        super(columnName);
    }

}
