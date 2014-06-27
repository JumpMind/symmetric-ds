package org.jumpmind.db.platform.firebird;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.ConcurrencySqlException;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;

public class FirebirdJdbcSqlTemplate extends JdbcSqlTemplate {

    public FirebirdJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            SymmetricLobHandler lobHandler, DatabaseInfo databaseInfo) {
        super(dataSource, settings, lobHandler, databaseInfo);
        primaryKeyViolationCodes = new int [] {335544665, 335544349};
    }
    
    @Override
    public boolean supportsReturningKeys() {
        return true;
    }
    
    @Override
    protected boolean allowsNullForIdentityColumn() {
        return true;
    }
    
    @Override
    public SqlException translate(String message, Throwable ex) {
        if (ex instanceof SQLException) {
            if (((SQLException)ex).getErrorCode() == 335544336) {
                throw new ConcurrencySqlException(message, ex);
            }
        }
        return super.translate(message, ex);
    }

}
