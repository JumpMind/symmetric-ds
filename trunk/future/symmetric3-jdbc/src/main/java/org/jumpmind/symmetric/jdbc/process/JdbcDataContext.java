package org.jumpmind.symmetric.jdbc.process;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.core.db.DbException;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.DataEventType;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.jdbc.sql.StatementBuilder;

public class JdbcDataContext extends DataContext {

    protected Connection connection;

    protected boolean oldAutoCommitValue;

    protected Table targetTable;

    protected long uncommittedRows = 0;

    protected Map<String, Map<DataEventType, StatementBuilder>> statements = new HashMap<String, Map<DataEventType, StatementBuilder>>();

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

    public void setTargetTable(Table targetTable) {
        this.targetTable = targetTable;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public void incrementUncommittedRows() {
        uncommittedRows++;
    }

    public void commit() {
        try {
            if (connection != null) {
                connection.commit();
                uncommittedRows = 0;
            }
        } catch (SQLException ex) {
            throw new DbException(ex);
        }
    }

    public void close() {
        try {
            if (connection != null) {
                commit();
                connection.close();
                connection = null;
            }
        } catch (SQLException ex) {
            throw new DbException(ex);
        }
    }

    public StatementBuilder getStatementBuilder(Table table, Data data) {
        StatementBuilder builder = null;
        Map<DataEventType, StatementBuilder> byDmlForTable = statements.get(table
                .getFullyQualifiedTableName());
        if (byDmlForTable != null) {
            builder = byDmlForTable.get(data.getEventType());
        }
        return builder;
    }

    public void putStatementBuilder(Table table, Data data, StatementBuilder builder) {
        String tableName = table.getFullyQualifiedTableName();
        Map<DataEventType, StatementBuilder> byDmlForTable = statements.get(tableName);
        if (byDmlForTable == null) {
            byDmlForTable = new HashMap<DataEventType, StatementBuilder>();
            statements.put(tableName, byDmlForTable);
        }
        byDmlForTable.put(data.getEventType(), builder);
    }
}
