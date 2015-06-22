package org.jumpmind.symmetric.db;

import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.dbcp.DelegatingPreparedStatement;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.oracle.OracleDbDialect;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.support.JdbcUtils;

public class JdbcBatchPreparedStatementCallback implements PreparedStatementCallback<Integer> {

    static final ILog log = LogFactory.getLog(JdbcBatchPreparedStatementCallback.class);
    static private boolean firstInitialization = true;
    BatchPreparedStatementSetter pss;
    IDbDialect dbDialect;
    int executeBatchSize;

    public JdbcBatchPreparedStatementCallback(IDbDialect dbDialect,
            BatchPreparedStatementSetter pss, int executeBatchSize) {
        this.pss = pss;
        this.dbDialect = dbDialect;
        this.executeBatchSize = executeBatchSize;
    }

    protected boolean setupForOracleBatching(PreparedStatement ps) {
        boolean oracleStyle = false;
        if (ps instanceof DelegatingPreparedStatement) {
            DelegatingPreparedStatement dps = (DelegatingPreparedStatement) ps;
            if (dbDialect instanceof OracleDbDialect) {
                try {
                    Class<?> clazz = Class.forName("oracle.jdbc.OraclePreparedStatement");
                    Statement delegate = dps.getDelegate();
                    if (clazz.isInstance(delegate)) {
                        Method method = clazz.getMethod("setExecuteBatch", int.class);
                        method.invoke(delegate, executeBatchSize);
                        oracleStyle = true;
                        if (firstInitialization) {
                            log.info("OracleBatchingUsed", executeBatchSize);
                            firstInitialization = false;
                        }
                    }
                } catch (Exception ex) {
                    log.warn(ex);
                }
            }
        }
        return oracleStyle;
    }

    public Integer doInPreparedStatement(PreparedStatement ps) throws SQLException,
            DataAccessException {
        int rowsAffected = 0;
        boolean oracleStyle = setupForOracleBatching(ps);
        int batchSize = pss.getBatchSize();
        if (JdbcUtils.supportsBatchUpdates(ps.getConnection())) {
            for (int i = 0; i < batchSize; i++) {
                pss.setValues(ps, i);
                if (oracleStyle) {
                    rowsAffected += ps.executeUpdate();
                } else {
                    ps.addBatch();
                    if (i % executeBatchSize == 0 || i == batchSize-1) {
                        int[] results = ps.executeBatch();
                        for (int j : results) {
                            rowsAffected += j;
                        }
                    }
                }
            }
            return rowsAffected;
        } else {
            for (int i = 0; i < batchSize; i++) {
                pss.setValues(ps, i);
                rowsAffected += ps.executeUpdate();
            }
            return rowsAffected;
        }
    }

}