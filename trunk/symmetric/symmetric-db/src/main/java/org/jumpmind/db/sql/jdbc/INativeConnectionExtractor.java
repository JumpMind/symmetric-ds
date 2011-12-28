package org.jumpmind.db.sql.jdbc;

import java.sql.Connection;

public interface INativeConnectionExtractor {
    
    public Connection extract(Connection c);

}
