package org.jumpmind.db.platform.hsqldb2;

import javax.sql.DataSource;

import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

public class HsqlDb2JdbcSqlTemplate extends JdbcSqlTemplate {

    public HsqlDb2JdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            LobHandler lobHandler) {
        super(dataSource, settings, lobHandler);        
        primaryKeyViolationSqlStates = new String[] {"23505"};
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
