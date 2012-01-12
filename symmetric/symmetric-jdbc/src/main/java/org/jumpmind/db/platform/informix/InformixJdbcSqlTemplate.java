package org.jumpmind.db.platform.informix;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.sql.jdbc.JdbcSqlTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

public class InformixJdbcSqlTemplate extends JdbcSqlTemplate {

    public InformixJdbcSqlTemplate(DataSource dataSource, DatabasePlatformSettings settings,
            LobHandler lobHandler) {
        super(dataSource, settings, lobHandler);        
        primaryKeyViolationCodes = new int[] {-268};
    }
    
    @Override
    public boolean allowsNullForIdentityColumn() {
        return false;
    }
    
}
