package org.jumpmind.db.platform.hsqldb;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.sql.jdbc.JdbcSqlTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

public class HsqlDbJdbcSqlTemplate extends JdbcSqlTemplate {

    public HsqlDbJdbcSqlTemplate(DataSource dataSource, DatabasePlatformSettings settings,
            LobHandler lobHandler) {
        super(dataSource, settings, lobHandler);
    }
    
    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "call IDENTITY()";
    }
    

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }
    
}
