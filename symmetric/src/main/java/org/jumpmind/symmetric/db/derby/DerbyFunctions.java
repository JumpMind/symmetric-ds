package org.jumpmind.symmetric.db.derby;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

import org.apache.commons.codec.binary.Base64;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.jdbc.EmbedConnection;

public class DerbyFunctions {

    private static final String CURRENT_CONNECTION_URL = "jdbc:default:connection";

    private static final int MAX_STRING_LENGTH = 32672;

    // Base64 will output roughly 1.37% size of input
    private static final int MAX_BINARY_LENGTH = 23700;

    private static Hashtable<String, Boolean> syncDisabledTable = new Hashtable<String, Boolean>();

    public static String getTransactionId() throws SQLException {
        return getLanguageConnection().getTransactionExecute().getTransactionIdString();
    }

    public static String getSessionId() throws SQLException {
        return Integer.toString(getLanguageConnection().getInstanceNumber());
    }

    public static int isSyncDisabled() throws SQLException {
        return syncDisabledTable.get(getSessionId()) != null ? 1 : 0;
    }

    public static int setSyncDisabled(int disabledIndicator) throws SQLException {
        if (disabledIndicator == 0) {
            syncDisabledTable.remove(getSessionId());
            return 0;
        } else {
            syncDisabledTable.put(getSessionId(), Boolean.TRUE);
            return 1;
        }
    }

    public static void insertData(String schemaName, String prefixName, String tableName, String channelName,
            String dmlType, long triggerHistId, String transactionId, String targetGroupId, String nodeSelectWhere,
            String pkData, String rowData) throws SQLException {
        if (((dmlType.equals("I") || dmlType.equals("U")) && rowData != null)
                || (dmlType.equals("D") && pkData != null)) {
            Connection conn = DriverManager.getConnection(CURRENT_CONNECTION_URL);
            String sql = "insert into " + schemaName + prefixName + "_data "
                    + "(table_name, event_type, trigger_hist_id, pk_data, row_data, create_time) "
                    + "values (?, ?, ?, ?, ?, current_timestamp)";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, tableName);
            ps.setString(2, dmlType);
            ps.setLong(3, triggerHistId);
            ps.setString(4, pkData);
            ps.setString(5, rowData);
            ps.executeUpdate();
            ps.close();
            sql = "insert into " + schemaName + prefixName
                    + "_data_event (node_id, data_id, channel_id, transaction_id) "
                    + "select node_id, IDENTITY_VAL_LOCAL(),'" + channelName + "','" + transactionId + "' from "
                    + prefixName + "_node c where (c.node_group_id = ? and c.sync_enabled = 1) " + nodeSelectWhere;
            ps = conn.prepareStatement(sql);
            ps.setString(1, targetGroupId);
            ps.executeUpdate();
            ps.close();
            conn.close();
        }
    }

    public static String blobToString(String columnName, String tableName, String whereClause) throws SQLException {
        Connection conn = DriverManager.getConnection(CURRENT_CONNECTION_URL);
        String sql = "select " + columnName + " from " + tableName + " where " + whereClause;
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        String str = null;
        if (rs.next()) {
            Blob blob = rs.getBlob(1);
            if (blob != null && blob.length() > 0) {
                str = new String(Base64.encodeBase64(blob.getBytes(1, MAX_BINARY_LENGTH)));
            }
        }
        ps.close();
        conn.close();
        return str == null ? "" : "\"" + str + "\"";
    }

    public static String clobToString(String columnName, String tableName, String whereClause) throws SQLException {
        Connection conn = DriverManager.getConnection(CURRENT_CONNECTION_URL);
        String sql = "select " + columnName + " from " + tableName + " where " + whereClause;
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        String str = null;
        if (rs.next()) {
            Clob clob = rs.getClob(1);
            if (clob != null && clob.length() > 0) {
                str = clob.getSubString(1, MAX_STRING_LENGTH);
            }
        }
        ps.close();
        conn.close();
        return str == null ? "" : "\"" + str + "\"";
    }

    private static LanguageConnectionContext getLanguageConnection() throws SQLException {
        EmbedConnection conn = (EmbedConnection) DriverManager.getConnection(CURRENT_CONNECTION_URL);
        return conn.getLanguageConnection();
    }
}
