package org.jumpmind.db.platform.mssql;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class MsSqlJdbcSqlTemplate extends JdbcSqlTemplate {

    public MsSqlJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings, DatabaseInfo databaseInfo) {
        super(dataSource, settings, null, databaseInfo);
        primaryKeyViolationCodes = new int[] {2627};
    }
    
    @Override
    public ISqlTransaction startSqlTransaction() {
        return new MsSqlJdbcSqlTransaction(this);
    }
   
    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }
    
}
