package org.jumpmind.db.platform.derby;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.springframework.jdbc.support.lob.LobHandler;

public class DerbyJdbcSqlTemplate extends JdbcSqlTemplate {

    public DerbyJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            LobHandler lobHandler, DatabaseInfo databaseInfo) {
        super(dataSource, settings, lobHandler, databaseInfo);
        primaryKeyViolationSqlStates = new String[] {"23505"};        
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
