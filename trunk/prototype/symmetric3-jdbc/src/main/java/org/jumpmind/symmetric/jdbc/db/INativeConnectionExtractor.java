package org.jumpmind.symmetric.jdbc.db;

import java.sql.Connection;
import java.sql.SQLException;

public interface INativeConnectionExtractor {

    /**
     * Retrieve the underlying native JDBC Connection for the given Connection.
     * Supposed to return the given Connection if not capable of unwrapping.
     * 
     * @param con
     *            the Connection handle, potentially wrapped by a connection
     *            pool
     * @return the underlying native JDBC Connection, if possible; else, the
     *         original Connection
     * @throws SQLException
     *             if thrown by JDBC methods
     */
    Connection getNativeConnection(Connection con) throws SQLException;
}
