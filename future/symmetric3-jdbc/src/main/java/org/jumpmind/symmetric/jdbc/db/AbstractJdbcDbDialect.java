package org.jumpmind.symmetric.jdbc.db;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.db.AbstractDbDialect;
import org.jumpmind.symmetric.core.model.Database;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.sql.ISqlTemplate;
import org.jumpmind.symmetric.jdbc.sql.IConnectionCallback;
import org.jumpmind.symmetric.jdbc.sql.ILobHandler;
import org.jumpmind.symmetric.jdbc.sql.JdbcSqlTemplate;

abstract public class AbstractJdbcDbDialect extends AbstractDbDialect implements IJdbcDbDialect {

    protected DataSource dataSource;

    protected JdbcTableReader jdbcModelReader;

    private Boolean supportsBatchUpdates;

    public AbstractJdbcDbDialect(DataSource dataSource, Parameters parameters) {
        super(parameters);
        this.dataSource = dataSource;
    }

    public ISqlTemplate getSqlConnection() {
        return getJdbcSqlConnection();
    }

    public JdbcSqlTemplate getJdbcSqlConnection() {
        return new JdbcSqlTemplate(this);
    }

    public Database findDatabase(String catalogName, String schemaName) {
        return jdbcModelReader.getDatabase(catalogName, schemaName, null);
    }

    public String getAlterScriptFor(Table... tables) {
        StringWriter writer = new StringWriter();
        tableBuilder.setWriter(writer);
        Database desiredModel = new Database();
        desiredModel.addTables(tables);

        Database currentModel = new Database();
        for (Table table : tables) {
            currentModel.addTable(jdbcModelReader.readTable(table.getCatalogName(),
                    table.getSchemaName(), table.getTableName(), false, false));
        }

        tableBuilder.alterDatabase(currentModel, desiredModel);

        return writer.toString();
    }

    public Table findTable(String tableName) {
        return findTable(null, null, tableName, false);
    }

    public Table findTable(String catalogName, String schemaName, String tableName, boolean useCache) {
        Table cachedTable = cachedModel.findTable(catalogName, schemaName, tableName);
        if (cachedTable == null || !useCache) {
            Table justReadTable = jdbcModelReader.readTable(catalogName, schemaName, tableName,
                    !parameters.is(Parameters.DB_METADATA_IGNORE_CASE, true),
                    parameters.is(Parameters.DB_USE_ALL_COLUMNS_AS_PK_IF_NONE_FOUND, false));

            if (cachedTable != null) {
                cachedModel.removeTable(cachedTable);
            }

            if (justReadTable != null) {
                cachedModel.addTable(justReadTable);
            }

            cachedTable = justReadTable;
        }
        return cachedTable;
    }

    public java.util.List<Table> findTables(String catalog, String schema) {
        return null;
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
            int[] codes = getDataIntegritySqlErrorCodes();
            for (int i : codes) {
                if (sqlErrorCode == i) {
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
