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
    
    @Override
    public boolean supportsLimitOffset() {
        return true;
    }
    
    @Override
    public String massageForLimitOffset(String sql, int limit, int offset) {
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        return sql + " offset " + offset + " rows fetch next " + limit + " rows only;";
    }

}
