package org.jumpmind.db.platform.sybase;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.mssql.MsSqlJdbcSqlTransaction;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;

public class SybaseJdbcSqlTemplate extends JdbcSqlTemplate {

    public SybaseJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            SymmetricLobHandler lobHandler, DatabaseInfo databaseInfo) {
        super(dataSource, settings, lobHandler, databaseInfo);
        primaryKeyViolationCodes = new int[] {423,511,515,530,547,2601,2615,2714};
    }

    @Override
    public ISqlTransaction startSqlTransaction() {
        return new SybaseJdbcSqlTransaction(this);
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

}
