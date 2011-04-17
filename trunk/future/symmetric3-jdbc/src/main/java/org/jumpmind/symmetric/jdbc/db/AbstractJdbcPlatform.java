package org.jumpmind.symmetric.jdbc.db;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.db.AbstractPlatform;
import org.jumpmind.symmetric.core.model.Table;

abstract public class AbstractJdbcPlatform extends AbstractPlatform {

    protected DataSource dataSource;

    protected JdbcModelReader jdbcModelReader;

    @Override
    public Table findTable(String catalog, String schema, String tableName) {
        return null;
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

    @Override
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

    abstract protected int[] getDataIntegritySqlErrorCodes();

}
