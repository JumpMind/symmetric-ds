package org.jumpmind.symmetric.db.hsqldb;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.hsqldb.DatabaseManager;
import org.hsqldb.Trigger;
import org.jumpmind.symmetric.db.AbstractEmbeddedTrigger;

/**
 * 
 */
public class HsqlDbTrigger extends AbstractEmbeddedTrigger implements Trigger {

    protected String triggerName;
    protected Map<String, String> templates = new HashMap<String, String>();

    public void fire(int type, String triggerName, String tableName, Object[] oldRow,
            Object[] newRow) {
        Connection conn = findConnection(triggerName);
        if (conn != null) {
            try {
                init(conn, triggerName, null, tableName);
                fire(conn, oldRow, newRow);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                try {
                    conn.close();
                } catch (SQLException ex) {
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected Connection findConnection(String triggerName) {
        Connection c = null;
        Vector<String> uris = DatabaseManager.getDatabaseURIs();
        for (String uri : uris) {
            Connection testCon = null;
            try {
                testCon = DriverManager.getConnection("jdbc:hsqldb:" + uri, new Properties());
                if (uris.size() > 1) {
                    Statement stmt = testCon.createStatement();
                    ResultSet rs = stmt.executeQuery(String.format("select count(*) from %s%s",
                            triggerName, TEMPLATE_TABLE_SUFFIX));
                    rs.close();
                    stmt.close();
                }
                c = testCon;
                break;
            } catch (SQLException e) {
                try {
                    if (testCon != null) {
                        testCon.close();
                    }
                } catch (SQLException e1) {
                }
            }
        }
        return c;
    }

    @Override
    protected Object appendVirtualTableStringValue(Object value, StringBuilder out) {
        if (value != null && (value.getClass().getName().equals("org.hsqldb.types.Binary")
                || value.getClass().getName().equals("org.hsqldb.types.BinaryData"))) {
            out.append("'");
            Method getBytes;
            try {
                getBytes = value.getClass().getMethod("getBytes");
                value = HsqlDbFunctions.encodeBase64((byte[]) getBytes.invoke(value));
                out.append(escapeString(value));
                out.append("'");
            } catch (RuntimeException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        } else {
            return super.appendVirtualTableStringValue(value, out);
        }

        return value;
    }

}