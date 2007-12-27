package org.jumpmind.symmetric.db.derby;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Hashtable;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.TransactionInfo;
import org.apache.derby.impl.jdbc.EmbedConnection;

public class DerbyFunctions {

    private static final String CURRENT_CONNECTION_URL = "jdbc:default:connection";

    private static Hashtable<String, Boolean> syncDisabledTable = new Hashtable<String, Boolean>();

    public static String getTransactionId() throws SQLException {
        return getLanguageConnection().getTransactionExecute().getTransactionIdString();
    }

    public static int isSyncDisabled() throws SQLException {
        return syncDisabledTable.get(getTransactionId()) != null ? 1 : 0;
    }

    public static int setSyncDisabled(int disabledIndicator) throws SQLException {
        if (disabledIndicator == 0) {
            syncDisabledTable.remove(getTransactionId());
            return 0;
        } else {
            syncDisabledTable.put(getTransactionId(), Boolean.TRUE);
            return 1;
        }
    }

    private static LanguageConnectionContext getLanguageConnection() throws SQLException {
        EmbedConnection conn = (EmbedConnection) DriverManager.getConnection(CURRENT_CONNECTION_URL);
        return conn.getLanguageConnection();
    }
    
    public static void clean() throws SQLException {
        LanguageConnectionContext lcc = getLanguageConnection();
        HashSet<String> currentTransactions = new HashSet<String>();

        for (TransactionInfo info : lcc.getTransactionExecute().getAccessManager().getTransactionInfo()) {
            currentTransactions.add(info.getTransactionIdString());
        }
        for (String xid : syncDisabledTable.keySet()) {
            if (!currentTransactions.contains(xid)) {
                syncDisabledTable.remove(xid);
            }
        }
    }
}
