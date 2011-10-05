package org.jumpmind.symmetric.jdbc.db.postgres;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.db.ISqlTransaction;
import org.jumpmind.symmetric.jdbc.db.IJdbcDbDialect;
import org.jumpmind.symmetric.jdbc.db.JdbcSqlTemplate;

public class PostgresJdbcSqlTemplate extends JdbcSqlTemplate {

    public PostgresJdbcSqlTemplate(DataSource dataSource) {
        super(dataSource);
    }
    
    public PostgresJdbcSqlTemplate(IJdbcDbDialect platform) {
        super(platform);
    }

    @Override
    public ISqlTransaction startSqlTransaction() {
        return new PostgresJdbcSqlTransaction(dbDialect);
    }


}
