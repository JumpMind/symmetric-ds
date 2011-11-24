package org.jumpmind.db;

import java.sql.Connection;
import java.sql.SQLException;

import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;

public interface IDdlReader {

    public Database getDatabase(Connection connection, String catalog, String schema,
            String[] tableTypes) throws SQLException;

    public Table readTable(Connection connection, String catalog, String schema, String tableName)
            throws SQLException;
}
