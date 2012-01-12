package org.jumpmind.db.platform.oracle;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.sql.jdbc.JdbcSqlTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

public class OracleJdbcSqlTemplate extends JdbcSqlTemplate {

    public OracleJdbcSqlTemplate(DataSource dataSource, DatabasePlatformSettings settings,
            LobHandler lobHandler) {
        super(dataSource, settings, lobHandler);        
        primaryKeyViolationCodes = new int[] {1};
    }
    
    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select " + sequenceName + ".currval from dual";
    }

}
