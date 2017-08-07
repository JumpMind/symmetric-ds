package org.jumpmind.db.platform.tibero;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;

public class TiberoJdbcSqlTemplate extends JdbcSqlTemplate {

    public TiberoJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            SymmetricLobHandler lobHandler, DatabaseInfo databaseInfo) {
        super(dataSource, settings, lobHandler, databaseInfo);        
        primaryKeyViolationCodes = new int[] {1};
        foreignKeyViolationCodes = new int[] {-10008};
    }
    
    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select " + sequenceName + ".currval from dual";
    }

    
    @Override
    public boolean isUniqueKeyViolation(Throwable ex) {
        SQLException sqlEx = findSQLException(ex);
        return sqlEx.getErrorCode() == -10007;
    }    
}
