package org.jumpmind.db.platform.hsqldb;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;

public class HsqlDbJdbcSqlTemplate extends JdbcSqlTemplate {

    public HsqlDbJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            SymmetricLobHandler lobHandler, DatabaseInfo databaseInfo) {
        super(dataSource, settings, lobHandler, databaseInfo);
        primaryKeyViolationSqlStates = new String[] {"23505"};        
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
