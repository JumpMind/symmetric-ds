package org.jumpmind.symmetric.db.h2;

import java.sql.Connection;
import java.sql.SQLException;

import org.jumpmind.symmetric.db.AbstractEmbeddedTrigger;

public class H2Trigger extends AbstractEmbeddedTrigger implements org.h2.api.Trigger {

    /**
     * This method is called by the database engine once when initializing the
     * trigger.
     * 
     * @param conn
     *            a connection to the database
     * @param schemaName
     *            the name of the schema
     * @param triggerName
     *            the name of the trigger used in the CREATE TRIGGER statement
     * @param tableName
     *            the name of the table
     * @param before
     *            whether the fire method is called before or after the
     *            operation is performed
     * @param type
     *            the operation type: INSERT, UPDATE, or DELETE
     */
    public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type)
            throws SQLException {
        this.init(conn, triggerName, schemaName, tableName);
    }
    
    public void close() throws SQLException {
    }
    
    public void remove() throws SQLException {
    }

}