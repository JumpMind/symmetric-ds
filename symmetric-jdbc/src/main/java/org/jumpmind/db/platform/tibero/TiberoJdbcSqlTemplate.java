package org.jumpmind.db.platform.tibero;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;

public class TiberoJdbcSqlTemplate extends JdbcSqlTemplate {

    public TiberoJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            SymmetricLobHandler lobHandler, DatabaseInfo databaseInfo) {
        super(dataSource, settings, lobHandler, databaseInfo);        
        primaryKeyViolationCodes = new int[] {-10007};
        uniqueKeyViolationNameRegex = new String[] { "UNIQUE constraint violation \\(.*\\.'(.*)'\\)" };
        foreignKeyViolationCodes = new int[] {-10008};
        foreignKeyChildExistsViolationCodes = new int[] {-10009};
    }
    
    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select " + sequenceName + ".currval from dual";
    }
    
}
