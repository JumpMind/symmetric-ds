package org.jumpmind.symmetric.jdbc.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface ILobHandler {

    /**
     * Set the given content as bytes on the given statement, using the given
     * parameter index. Might simply invoke
     * <code>PreparedStatement.setBytes</code> or create a Blob instance for it,
     * depending on the database and driver.
     * 
     * @param ps
     *            the PreparedStatement to the set the content on
     * @param paramIndex
     *            the parameter index to use
     * @param content
     *            the content as byte array, or <code>null</code> for SQL NULL
     * @throws SQLException
     *             if thrown by JDBC methods
     * @see java.sql.PreparedStatement#setBytes
     */
    void setBlobAsBytes(PreparedStatement ps, int paramIndex, byte[] content) throws SQLException;

    /**
     * Set the given content as String on the given statement, using the given
     * parameter index. Might simply invoke
     * <code>PreparedStatement.setString</code> or create a Clob instance for
     * it, depending on the database and driver.
     * 
     * @param ps
     *            the PreparedStatement to the set the content on
     * @param paramIndex
     *            the parameter index to use
     * @param content
     *            the content as String, or <code>null</code> for SQL NULL
     * @throws SQLException
     *             if thrown by JDBC methods
     * @see java.sql.PreparedStatement#setBytes
     */
    void setClobAsString(PreparedStatement ps, int paramIndex, String content) throws SQLException;
}
