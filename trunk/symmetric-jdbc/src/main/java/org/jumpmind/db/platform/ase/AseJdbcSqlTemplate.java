package org.jumpmind.db.platform.ase;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;

public class AseJdbcSqlTemplate extends JdbcSqlTemplate {

    public AseJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            SymmetricLobHandler lobHandler, DatabaseInfo databaseInfo) {
        super(dataSource, settings, lobHandler, databaseInfo);
        primaryKeyViolationCodes = new int[] {423,511,515,530,547,2601,2615,2714};
    }

    @Override
    public ISqlTransaction startSqlTransaction() {
        return new AseJdbcSqlTransaction(this);
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

}
