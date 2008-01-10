package org.jumpmind.symmetric.db.derby;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Hashtable;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.jdbc.EmbedConnection;

public class DerbyFunctions {

    private static final String CURRENT_CONNECTION_URL = "jdbc:default:connection";
    
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
            String dmlType, long triggerHistId, String transactionId, String targetGroupId,
            String nodeSelectWhere, String pkData, String rowData) throws SQLException {
        if (((dmlType.equals("I") || dmlType.equals("U")) && rowData != null) || 
                (dmlType.equals("D") && pkData != null)) {
            Connection conn = DriverManager.getConnection(CURRENT_CONNECTION_URL);
            String sql = "insert into "
                    + schemaName
                    + prefixName
                    + "_data "
                    + "(table_name, channel_id, event_type, trigger_hist_id, transaction_id, pk_data, row_data, create_time) "
                    + "values (?, ?, ?, ?, ?, ?, ?, current_timestamp)";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, tableName);
            ps.setString(2, channelName);
            ps.setString(3, dmlType);
            ps.setLong(4, triggerHistId);
            ps.setString(5, transactionId);
            ps.setString(6, pkData);
            ps.setString(7, rowData);
            ps.executeUpdate();
            ps.close();
            sql = "insert into " + schemaName + prefixName + "_data_event (node_id, data_id) "
                    + "select node_id, IDENTITY_VAL_LOCAL() from " + prefixName
                    + "_node c where (c.node_group_id = ? and c.sync_enabled = 1) " + nodeSelectWhere;
            ps = conn.prepareStatement(sql);
            ps.setString(1, targetGroupId);
            ps.executeUpdate();
            ps.close();
            conn.close();
        }
    }
    
    private static LanguageConnectionContext getLanguageConnection() throws SQLException {
        EmbedConnection conn = (EmbedConnection) DriverManager.getConnection(CURRENT_CONNECTION_URL);
        return conn.getLanguageConnection();
    }    
}
