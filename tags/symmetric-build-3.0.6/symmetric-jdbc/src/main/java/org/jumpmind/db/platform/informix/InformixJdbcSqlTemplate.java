package org.jumpmind.db.platform.informix;

import javax.sql.DataSource;

import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

public class InformixJdbcSqlTemplate extends JdbcSqlTemplate {

    public InformixJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            LobHandler lobHandler) {
        super(dataSource, settings, lobHandler);        
        primaryKeyViolationCodes = new int[] {-268};
    }
    
    @Override
    public boolean allowsNullForIdentityColumn() {
        return false;
    }
    
}