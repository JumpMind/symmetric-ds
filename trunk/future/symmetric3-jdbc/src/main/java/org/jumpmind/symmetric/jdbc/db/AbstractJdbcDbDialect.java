package org.jumpmind.symmetric.jdbc.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.common.NotImplementedException;
import org.jumpmind.symmetric.core.db.AbstractDbDialect;
import org.jumpmind.symmetric.core.db.ISqlTemplate;
import org.jumpmind.symmetric.core.model.Database;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;

abstract public class AbstractJdbcDbDialect extends AbstractDbDialect implements IJdbcDbDialect {

    protected DataSource dataSource;

    protected JdbcTableReader jdbcModelReader;

    private Boolean supportsBatchUpdates;

    public AbstractJdbcDbDialect(DataSource dataSource, Parameters parameters) {
        super(parameters);
        this.dataSource = dataSource;
    }

    final public ISqlTemplate getSqlTemplate() {
        return getJdbcSqlConnection();
    }

    public JdbcSqlTemplate getJdbcSqlConnection() {
        return new JdbcSqlTemplate(this);
    }

    public Database findDatabase(String catalogName, String schemaName) {
        return jdbcModelReader.getDatabase(catalogName, schemaName, null);
    }

    @Override
    protected Table readTable(String catalogName, String schemaName, String tableName,
            boolean caseSensitive, boolean makeAllColumnsPKsIfNoneFound) {
        return jdbcModelReader.readTable(catalogName, schemaName, tableName, caseSensitive,
                makeAllColumnsPKsIfNoneFound);
    }

    public java.util.List<Table> findTables(String catalog, String schema, boolean useCached) {
        throw new NotImplementedException();
    };

    protected void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public boolean isDataIntegrityException(Exception ex) {
        boolean integrityError = false;
        if (ex instanceof SQLException) {
            int sqlErrorCode = ((SQLException) ex).getErrorCode();
            String sqlState = ((SQLException) ex).getSQLState();
            int[] codes = getDataIntegritySqlErrorCodes();
            for (int i : codes) {
                if (sqlErrorCode == i) {
                    integrityError = true;
                    break;
                } else if (sqlState.equals(Integer.toString(i))) {
                    integrityError = true;
                    break;
                }
            }
        }
        return integrityError;
    }

    public ILobHandler getLobHandler() {
        return null;
    }

    abstract protected int[] getDataIntegritySqlErrorCodes();

    /**
     * Return whether the given JDBC driver supports JDBC 2.0 batch updates.
     * <p>
     * Typically invoked right before execution of a given set of statements: to
     * decide whether the set of SQL statements should be executed through the
     * JDBC 2.0 batch mechanism or simply in a traditional one-by-one fashion.
     * <p>
     * Logs a warning if the "supportsBatchUpdates" methods throws an exception
     * and simply returns <code>false</code> in that case.
     * 
     * @param con
     *            the Connection to check
     * @return whether JDBC 2.0 batch updates are supported
     * @see java.sql.DatabaseMetaData#supportsBatchUpdates()
     */
    public boolean supportsBatchUpdates() {
        if (supportsBatchUpdates == null) {
            try {
                supportsBatchUpdates = getJdbcSqlConnection().execute(
                        new IConnectionCallback<Boolean>() {
                            public Boolean execute(Connection con) throws SQLException {
                                DatabaseMetaData dbmd = con.getMetaData();
                                if (dbmd != null) {
                                    if (dbmd.supportsBatchUpdates()) {
                                        return true;
                                    }
                                }
                                return false;
                            }
                        });
            } catch (Exception ex) {
                supportsBatchUpdates = false;
            }
        }
        return supportsBatchUpdates;
    }

}
