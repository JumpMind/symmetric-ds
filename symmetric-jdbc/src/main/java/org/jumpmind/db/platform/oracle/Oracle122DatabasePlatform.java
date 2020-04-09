package org.jumpmind.db.platform.oracle;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class Oracle122DatabasePlatform extends OracleDatabasePlatform {

    public Oracle122DatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }
    
    @Override
    protected OracleDdlBuilder createDdlBuilder() {
        return new Oracle122DdlBuilder();
    }
    
    @Override
    public String getName() {
        return DatabaseNamesConstants.ORACLE122;
    }

}
