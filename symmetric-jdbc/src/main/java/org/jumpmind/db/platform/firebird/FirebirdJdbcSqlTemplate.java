package org.jumpmind.db.platform.firebird;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.springframework.jdbc.support.lob.LobHandler;

public class FirebirdJdbcSqlTemplate extends JdbcSqlTemplate {

    public FirebirdJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            LobHandler lobHandler, DatabaseInfo databaseInfo) {
        super(dataSource, settings, lobHandler, databaseInfo);
        primaryKeyViolationCodes = new int [] {335544665};
    }
    
    @Override
    public boolean supportsReturningKeys() {
        return true;
    }
    
    @Override
    protected boolean allowsNullForIdentityColumn() {
        return true;
    }

}
