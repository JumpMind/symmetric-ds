package org.jumpmind.symmetric.jdbc.db;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.db.IDbPlatform;
import org.jumpmind.symmetric.jdbc.sql.ILobHandler;
import org.jumpmind.symmetric.jdbc.sql.JdbcSqlConnection;

public interface IJdbcPlatform extends IDbPlatform {

    public ILobHandler getLobHandler();  
    
    public DataSource getDataSource();
    
    public JdbcSqlConnection getJdbcSqlConnection();
    
}
