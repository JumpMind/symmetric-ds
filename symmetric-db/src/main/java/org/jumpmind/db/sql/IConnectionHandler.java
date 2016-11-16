package org.jumpmind.db.sql;

import java.sql.Connection;

public interface IConnectionHandler {
    
    public void before(Connection connection);
    
    public void after(Connection connection);
    
}
