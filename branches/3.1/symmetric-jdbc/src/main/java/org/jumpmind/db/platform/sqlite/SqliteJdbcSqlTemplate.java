package org.jumpmind.db.platform.sqlite;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;

public class SqliteJdbcSqlTemplate extends JdbcSqlTemplate {

    public SqliteJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings, SymmetricLobHandler lobHandler,
            DatabaseInfo databaseInfo) {
        super(dataSource, settings, lobHandler, databaseInfo);
    }

    public boolean isUniqueKeyViolation(Exception ex) {
        SQLException sqlEx = findSQLException(ex);
        return (sqlEx != null && sqlEx.getMessage() != null && sqlEx.getMessage().contains("[SQLITE_CONSTRAINT]"));
    }
    
    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "SELECT last_insert_rowid();";
    }
}
