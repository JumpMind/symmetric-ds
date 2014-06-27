package org.jumpmind.db.platform.firebird;

import javax.sql.DataSource;

import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

public class FirebirdJdbcSqlTemplate extends JdbcSqlTemplate {

    public FirebirdJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            LobHandler lobHandler) {
        super(dataSource, settings, lobHandler);
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
