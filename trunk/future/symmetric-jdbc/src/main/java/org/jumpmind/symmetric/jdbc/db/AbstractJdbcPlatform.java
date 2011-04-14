package org.jumpmind.symmetric.jdbc.db;

import javax.sql.DataSource;

import org.jumpmind.symmetric.data.db.AbstractPlatform;
import org.jumpmind.symmetric.data.model.Table;

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
    
}
