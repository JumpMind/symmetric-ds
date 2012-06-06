package org.jumpmind.db.util;

import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;

/**
 * A subclass of {@link BasicDataSource} which allows for a data source to be
 * closed (all underlying connections are closed) and then allows new
 * connections to be created.
 */
public class ResettableBasicDataSource extends BasicDataSource {

    public ResettableBasicDataSource() {
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
