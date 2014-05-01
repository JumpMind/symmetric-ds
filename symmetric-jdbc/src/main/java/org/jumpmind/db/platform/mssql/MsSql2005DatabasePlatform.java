package org.jumpmind.db.platform.mssql;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.sql.SqlTemplateSettings;

/*
 * The platform implementation for the Microsoft SQL Server 2005 database.
 */
public class MsSql2005DatabasePlatform extends MsSql2000DatabasePlatform {

    /*
     * Creates a new platform instance.
     */
    public MsSql2005DatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }
    
    @Override
    public String getName() {
        return DatabaseNamesConstants.MSSQL2005;
    }
    
    @Override
    public String getDefaultSchema() {
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = (String) getSqlTemplate().queryForObject("select SCHEMA_NAME()",
                    String.class);
        }
        return defaultSchema;
    }
}
