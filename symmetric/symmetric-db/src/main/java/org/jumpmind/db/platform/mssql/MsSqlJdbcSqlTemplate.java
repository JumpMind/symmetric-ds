package org.jumpmind.db.platform.mssql;

import javax.sql.DataSource;

import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.jdbc.JdbcSqlTemplate;

public class MsSqlJdbcSqlTemplate extends JdbcSqlTemplate {

    public MsSqlJdbcSqlTemplate(DataSource dataSource) {
        super(dataSource);
    }
    
    @Override
    public ISqlTransaction startSqlTransaction() {
        return new MsSqlJdbcSqlTransaction(this);
    }

}
