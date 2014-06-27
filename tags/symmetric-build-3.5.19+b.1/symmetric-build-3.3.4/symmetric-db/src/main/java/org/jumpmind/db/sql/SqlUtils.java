package org.jumpmind.db.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class SqlUtils {
    
    private static Logger log = LoggerFactory.getLogger(SqlUtils.class);

    private static boolean captureOwner = false;

    private static List<ISqlTransaction> sqlTransactions = new ArrayList<ISqlTransaction>();

    private static List<ISqlReadCursor<?>> sqlReadCursors = new ArrayList<ISqlReadCursor<?>>();

    private static Map<ISqlTransaction, Exception> sqlTransactionsOwnerMap = new HashMap<ISqlTransaction, Exception>();

    private static Map<ISqlReadCursor<?>, Exception> sqlReadCursorsOwnerMap = new HashMap<ISqlReadCursor<?>, Exception>();

    protected static void addSqlTransaction(ISqlTransaction transaction) {
        sqlTransactions.add(transaction);
        if (captureOwner) {
            sqlTransactionsOwnerMap.put(transaction, new Exception());
        }
    }

    protected static void addSqlReadCursor(ISqlReadCursor<?> cursor) {
        sqlReadCursors.add(cursor);
        if (captureOwner) {
            sqlReadCursorsOwnerMap.put(cursor, new Exception());
        }
    }

    protected static void removeSqlReadCursor(ISqlReadCursor<?> cursor) {
        sqlReadCursors.remove(cursor);
        if (captureOwner) {
            sqlReadCursorsOwnerMap.remove(cursor);
        }
    }

    protected static void removeSqlTransaction(ISqlTransaction transaction) {
        sqlTransactions.remove(transaction);
        if (captureOwner) {
            sqlTransactionsOwnerMap.remove(transaction);
        }
    }

    public static List<ISqlTransaction> getOpenTransactions() {
        return new ArrayList<ISqlTransaction>(sqlTransactions);
    }

    public static List<ISqlReadCursor<?>> getOpenSqlReadCursors() {
        return new ArrayList<ISqlReadCursor<?>>(sqlReadCursors);
    }

    
    public static void logOpenResources() {
        List<ISqlReadCursor<?>> cursors = SqlUtils.getOpenSqlReadCursors();
        for (ISqlReadCursor<?> cursor : cursors) {
            Exception ex = sqlReadCursorsOwnerMap.get(cursor);
            if (ex != null) {
                log.error("The following stack contains the owner of an open read cursor", ex);
            }
        }
        
        List<ISqlTransaction> transactions = SqlUtils.getOpenTransactions();
        for (ISqlTransaction transaction : transactions) {
            Exception ex = sqlTransactionsOwnerMap.get(transaction);
            if (ex != null) {
                log.error("The following stack contains the owner of an open database transaction", ex);
            }
        }
    }
    

    public static void setCaptureOwner(boolean captureOwner) {
        SqlUtils.captureOwner = captureOwner;
    }

}
