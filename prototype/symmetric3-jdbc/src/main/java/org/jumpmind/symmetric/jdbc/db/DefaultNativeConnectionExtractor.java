package org.jumpmind.symmetric.jdbc.db;

import java.sql.Connection;
import java.sql.SQLException;

public class DefaultNativeConnectionExtractor implements INativeConnectionExtractor {

    public Connection getNativeConnection(Connection con) throws SQLException {
        return con;
    }

}
