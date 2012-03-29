package org.jumpmind.symmetric.core.db;

public class ColumnNotFoundException extends SqlException {

    private static final long serialVersionUID = 1L;

    public ColumnNotFoundException(String columnName) {
        super(columnName);
    }
}
