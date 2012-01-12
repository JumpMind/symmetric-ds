package org.jumpmind.db.platform.sybase;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.sql.jdbc.JdbcSqlTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

public class SybaseJdbcSqlTemplate extends JdbcSqlTemplate {

    public SybaseJdbcSqlTemplate(DataSource dataSource, DatabasePlatformSettings settings,
            LobHandler lobHandler) {
        super(dataSource, settings, lobHandler);
        primaryKeyViolationCodes = new int[] {423,511,515,530,547,2601,2615,2714};
    }
        
    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    } 
    
}
