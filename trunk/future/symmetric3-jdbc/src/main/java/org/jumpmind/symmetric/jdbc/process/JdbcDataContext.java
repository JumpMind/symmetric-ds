package org.jumpmind.symmetric.jdbc.process;

import java.sql.Connection;

import org.jumpmind.symmetric.core.process.DataContext;

public class JdbcDataContext extends DataContext {

    protected Connection connection;

    protected boolean oldAutoCommitValue;

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setOldAutoCommitValue(boolean oldAutoCommitValue) {
        this.oldAutoCommitValue = oldAutoCommitValue;
    }

    public boolean isOldAutoCommitValue() {
        return oldAutoCommitValue;
    }
}
