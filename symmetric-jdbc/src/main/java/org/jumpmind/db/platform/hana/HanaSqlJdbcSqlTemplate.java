package org.jumpmind.db.platform.hana;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;

public class HanaSqlJdbcSqlTemplate extends JdbcSqlTemplate {

    public HanaSqlJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings, SymmetricLobHandler lobHandler,
            DatabaseInfo databaseInfo) {
        super(dataSource, settings, lobHandler, databaseInfo);
    }

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select current_identity_value() FROM dummy;";
    }
    
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }
}
