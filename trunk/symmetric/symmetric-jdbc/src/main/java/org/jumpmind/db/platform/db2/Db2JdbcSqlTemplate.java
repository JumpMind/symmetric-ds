package org.jumpmind.db.platform.db2;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

public class Db2JdbcSqlTemplate extends JdbcSqlTemplate {

    public Db2JdbcSqlTemplate(DataSource dataSource, DatabasePlatformSettings settings,
            LobHandler lobHandler) {
        super(dataSource, settings, lobHandler);
        primaryKeyViolationCodes = new int[] {-803};        
    }
    

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "values IDENTITY_VAL_LOCAL()";
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

}
