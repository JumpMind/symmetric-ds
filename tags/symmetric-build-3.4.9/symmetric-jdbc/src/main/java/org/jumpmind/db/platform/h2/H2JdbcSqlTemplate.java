package org.jumpmind.db.platform.h2;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;

public class H2JdbcSqlTemplate extends JdbcSqlTemplate {

    public H2JdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            SymmetricLobHandler lobHandler, DatabaseInfo databaseInfo) {
        super(dataSource, settings, lobHandler, databaseInfo);
        primaryKeyViolationSqlStates = new String[] {"23001", "23505"};
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
