package org.jumpmind.symmetric.data.jdbc;

import java.sql.Connection;

import org.jumpmind.symmetric.data.DataContext;

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
