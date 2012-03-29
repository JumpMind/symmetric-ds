package org.jumpmind.symmetric.jdbc.db;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.db.IDbDialect;

public interface IJdbcDbDialect extends IDbDialect {

    public ILobHandler getLobHandler();

    public DataSource getDataSource();

    public JdbcSqlTemplate getJdbcSqlConnection();

}
