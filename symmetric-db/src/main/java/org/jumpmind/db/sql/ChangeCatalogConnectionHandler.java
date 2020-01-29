package org.jumpmind.db.sql;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeCatalogConnectionHandler implements IConnectionHandler {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private String previousCatalog;
    
    private String changeCatalog;
    
    public ChangeCatalogConnectionHandler(String newCatalog) {
        changeCatalog = newCatalog;
    }
    
    @Override
    public void before(Connection connection) {
        if (changeCatalog != null) {
            try {
                previousCatalog = connection.getCatalog();
                connection.setCatalog(changeCatalog);
            } catch (SQLException e) {
                log.warn("Unable to switch to catalog '{}': ", changeCatalog, e.getMessage());
                if (changeCatalog != null) {
                    try {
                        connection.setCatalog(previousCatalog);
                    } catch (SQLException ex) {
                    }
                }
                throw new SqlException(e);
            } 
        }
    }

    @Override
    public void after(Connection connection) {
        try {
            if (previousCatalog != null) {
                connection.setCatalog(previousCatalog);
            }
        } catch (SQLException ex) {
        }
    }

}
