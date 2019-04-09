package org.jumpmind.db.platform.hbase;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.platform.generic.GenericJdbcDatabasePlatform;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class HbasePlatform extends GenericJdbcDatabasePlatform {

    public HbasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }

    @Override
    public String getName() {
        return DatabaseNamesConstants.HBASE;
    }
    
    @Override
    protected IDdlBuilder createDdlBuilder() {
        return new HbaseDdlBuilder();
    }
    
    @Override
    public IDdlBuilder getDdlBuilder() {
        return new HbaseDdlBuilder();
    }

}
