package org.jumpmind.db.platform.mssql2000;

import javax.sql.DataSource;

import org.jumpmind.db.platform.mssql.MsSqlDatabasePlatform;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class MsSql2000DatabasePlatform extends MsSqlDatabasePlatform {

    public MsSql2000DatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }

    @Override
    public String getDefaultSchema() {
        return "dbo";
    }
    
}
