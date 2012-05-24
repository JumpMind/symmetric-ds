package org.jumpmind.db.util;

import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;

public class ConnectionPool extends BasicDataSource {

    public ConnectionPool() {
        setAccessToUnderlyingConnectionAllowed(true);
    }

    @Override
    public synchronized void close() throws SQLException {
        try {
            super.close();
        } finally {
            closed = false;
        }

    }

}
