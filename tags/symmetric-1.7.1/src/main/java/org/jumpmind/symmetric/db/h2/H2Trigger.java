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
package org.jumpmind.symmetric.db.h2;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.h2.util.ByteUtils;
import org.h2.util.IOUtils;

public class H2Trigger implements org.h2.api.Trigger {

    protected static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
    static final String KEY_CONDITION_SQL = "CONDITION_SQL";
    static final String KEY_INSERT_DATA_SQL = "INSERT_DATA_SQL";
    static final String KEY_INSERT_DATA_EVENT_SQL = "INSERT_DATA_EVENT_SQL";

    protected String triggerName;
    protected Map<String, String> templates = new HashMap<String, String>();

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
        this.triggerName = triggerName;
        this.templates = getTemplates(conn);
        if (templates == null || templates.size() == 0) {
            throw new IllegalStateException(String.format(
                    "The '%s' SymmetricDS trigger is in an invalid state.  It needs to be dropped.", triggerName));
        }
    }

    /**
     * This method is called for each triggered action.
     * 
     * @param conn
     *            a connection to the database
     * @param oldRow
     *            the old row, or null if no old row is available (for INSERT)
     * @param newRow
     *            the new row, or null if no new row is available (for DELETE)
     * @throws SQLException
     *             if the operation must be undone
     */
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
        Statement stmt = conn.createStatement();
        String sql = fillVirtualTableSql(templates.get(KEY_CONDITION_SQL), oldRow, newRow);
        ResultSet rs = stmt.executeQuery(sql);
        if (rs.next() && rs.getInt(1) > 0) {
            rs.close();
            sql = fillVirtualTableSql(templates.get(KEY_INSERT_DATA_SQL), oldRow, newRow);
            int count = stmt.executeUpdate(sql);
            if (count > 0) {
                sql = fillVirtualTableSql(templates.get(KEY_INSERT_DATA_EVENT_SQL), oldRow, newRow);
                stmt.executeUpdate(sql);
            }
        }
        stmt.close();
    }

    /**
     * TODO Try this as a preparedstatement
     */
    protected String fillVirtualTableSql(String sql, Object[] oldRow, Object[] newRow) throws SQLException {
        int columnCount = oldRow != null ? oldRow.length : newRow.length;
        StringBuilder out = new StringBuilder();
        String[] tokens = sql.split("\\?");
        int tokenIndex = 0;
        tokenIndex = forEachColumn(columnCount, newRow, out, tokenIndex, tokens);
        tokenIndex = forEachColumn(columnCount, oldRow, out, tokenIndex, tokens);
        out.append(tokens[tokenIndex]);
        return out.toString();
    }

    private int forEachColumn(int columnCount, Object[] data, StringBuilder out, int tokenIndex, String[] tokens) {
        for (int i = 0; i < columnCount; i++) {
            out.append(tokens[tokenIndex++]);
            if (data != null) {
                data[i] = appendVirtualTableStringValue(data[i], out);
            } else {
                out.append("null");
            }
        }
        return tokenIndex;
    }

    private Object appendVirtualTableStringValue(Object value, StringBuilder out) {
        if (value == null) {
            out.append("null");
        } else if (value instanceof String || value instanceof Boolean || value instanceof BufferedReader) {
            if (value instanceof BufferedReader) {
                try {
                    BufferedReader reader = (BufferedReader) value;
                    value = IOUtils.readStringAndClose(reader, -1);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            out.append("'");
            out.append(escapeString(value));
            out.append("'");
        } else if (value instanceof Number) {
            out.append(value);
        } else if (value instanceof ByteArrayInputStream || value instanceof BufferedInputStream) {
            out.append("'");
            try {
                value = ByteUtils.convertBytesToString(IOUtils.readBytesAndClose((InputStream) value, -1));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            out.append(escapeString(value));
            out.append("'");

        } else if (value instanceof Date) {
            out.append("'");
            synchronized (DATE_FORMATTER) {
                out.append(DATE_FORMATTER.format(value));
            }
            out.append("'");
        } else {
            throw new IllegalStateException(String.format("Type not supported: %s", value.getClass().getName()));
        }
        return value;
    }
    
    protected String escapeString(Object val) {
        return val == null ? null : val.toString().replaceAll("'", "''");
    }

    protected Map<String, String> getTemplates(Connection conn) throws SQLException {
        Map<String, String> templates = new HashMap<String, String>();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(String.format("select * from %s_VIEW", triggerName));
        if (rs.next()) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                templates.put(metaData.getColumnName(i), rs.getString(i));
            }
            return templates;
        } else {
            throw new SQLException(String.format("%s is in an invalid state.  %s_VIEW did not return a row.",
                    triggerName, triggerName));
        }
    }

}
