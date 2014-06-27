/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.db.hsqldb;

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
import org.hsqldb.types.Binary;
import org.jumpmind.symmetric.db.AbstractEmbeddedTrigger;

public class HsqlDbTrigger extends AbstractEmbeddedTrigger implements Trigger {

    protected String triggerName;
    protected Map<String, String> templates = new HashMap<String, String>();

    public void fire(int type, String triggerName, String tableName, Object[] oldRow, Object[] newRow) {
        Connection conn = findConnection(triggerName);        
        if (conn != null) {
            try {
                init(conn, triggerName, tableName);
                fire(conn, oldRow, newRow);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                try {
                conn.close();
                } catch (SQLException ex) {}
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
                    ResultSet rs = stmt.executeQuery(String.format("select count(*) from %s%s", triggerName,
                            TEMPLATE_TABLE_SUFFIX));
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
        if (value instanceof Binary) {
            out.append("'");
            value = HsqlDbFunctions.encodeBase64(((Binary) value).getBytes());
            out.append(escapeString(value));
            out.append("'");
        } else {
            return super.appendVirtualTableStringValue(value, out);
        }

        return value;
    }

}
