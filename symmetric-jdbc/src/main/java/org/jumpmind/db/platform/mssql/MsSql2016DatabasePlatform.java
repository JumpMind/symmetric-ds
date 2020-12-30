package org.jumpmind.db.platform.mssql;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class MsSql2016DatabasePlatform extends MsSql2008DatabasePlatform {

    public MsSql2016DatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
        if (settings.isAllowTriggerCreateOrReplace()) {
            getDatabaseInfo().setTriggersCreateOrReplaceSupported(true);
        }
    }
    
    @Override
    protected IDdlBuilder createDdlBuilder() {
        return new MsSql2016DdlBuilder();
    }
    
    @Override
    public String getName() {
        return DatabaseNamesConstants.MSSQL2016;
    }
}
