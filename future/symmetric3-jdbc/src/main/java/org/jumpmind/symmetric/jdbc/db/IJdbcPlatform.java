package org.jumpmind.symmetric.jdbc.db;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.db.IPlatform;
import org.jumpmind.symmetric.jdbc.sql.ILobHandler;

public interface IJdbcPlatform extends IPlatform {

    public ILobHandler getLobHandler();  
    
    public DataSource getDataSource();
    
}
