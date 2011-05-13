package org.jumpmind.symmetric.core.db;

import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.sql.SqlException;

public class TableNotFoundException extends SqlException {

    private static final long serialVersionUID = 1L;

    public TableNotFoundException(String tableName) {
        this(new Table(tableName));
    }

    public TableNotFoundException(String catalogName, String schemaName, String tableName) {
        this(new Table(catalogName, schemaName, tableName));
    }

    public TableNotFoundException(Table table) {
        super(table.getFullyQualifiedTableName());
    }

}
