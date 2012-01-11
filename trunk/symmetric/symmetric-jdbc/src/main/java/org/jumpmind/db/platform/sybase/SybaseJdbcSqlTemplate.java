package org.jumpmind.db.platform.sybase;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.sql.jdbc.JdbcSqlTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

public class SybaseJdbcSqlTemplate extends JdbcSqlTemplate {

    public SybaseJdbcSqlTemplate(DataSource dataSource, DatabasePlatformSettings settings,
            LobHandler lobHandler) {
        super(dataSource, settings, lobHandler);
    }
        
    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    } 
    
}
