package org.jumpmind.db.platform.informix;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.springframework.jdbc.support.lob.LobHandler;

public class InformixJdbcSqlTemplate extends JdbcSqlTemplate {

    public InformixJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            LobHandler lobHandler, DatabaseInfo databaseInfo) {
        super(dataSource, settings, lobHandler, databaseInfo);        
        primaryKeyViolationCodes = new int[] {-268};
    }
    
    @Override
    public boolean allowsNullForIdentityColumn() {
        return false;
    }
    
}
