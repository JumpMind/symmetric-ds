package org.jumpmind.db.platform.h2;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.sql.jdbc.JdbcSqlTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

public class H2JdbcSqlTemplate extends JdbcSqlTemplate {

    public H2JdbcSqlTemplate(DataSource dataSource, DatabasePlatformSettings settings,
            LobHandler lobHandler) {
        super(dataSource, settings, lobHandler);
    }
    
    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "call IDENTITY()";
    }
    
}
