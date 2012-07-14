package org.jumpmind.db.platform.interbase;

import javax.sql.DataSource;

import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

public class InterbaseJdbcSqlTemplate extends JdbcSqlTemplate {

    public InterbaseJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            LobHandler lobHandler) {
        super(dataSource, settings, lobHandler);
        primaryKeyViolationCodes = new int [] {335544665};
    }
    
    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select gen_id(GEN_" + sequenceName + ", 0) from rdb$database";
    }
    
    @Override
    protected boolean allowsNullForIdentityColumn() {
        return true;
    }

    
}
