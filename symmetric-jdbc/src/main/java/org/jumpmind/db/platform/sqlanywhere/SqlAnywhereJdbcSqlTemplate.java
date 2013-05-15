package org.jumpmind.db.platform.sqlanywhere;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;

public class SqlAnywhereJdbcSqlTemplate extends JdbcSqlTemplate implements ISqlTemplate {

    public SqlAnywhereJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            SymmetricLobHandler lobHandler, DatabaseInfo databaseInfo) {
        super(dataSource, settings, lobHandler, databaseInfo);
        primaryKeyViolationCodes = new int[] {423,511,515,530,547,2601,2615,2714};
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }


}
