package org.jumpmind.db.platform.interbase;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.sql.jdbc.JdbcSqlTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

public class InterbaseJdbcSqlTemplate extends JdbcSqlTemplate {

    public InterbaseJdbcSqlTemplate(DataSource dataSource, DatabasePlatformSettings settings,
            LobHandler lobHandler) {
        super(dataSource, settings, lobHandler);
    }
    
    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select gen_id(gen_" + sequenceName + ", 0) from rdb$database";
    }
    
    @Override
    protected boolean allowsNullForIdentityColumn() {
        return true;
    }

    
}
