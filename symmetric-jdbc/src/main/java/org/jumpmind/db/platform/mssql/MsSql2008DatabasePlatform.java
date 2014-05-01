package org.jumpmind.db.platform.mssql;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.SqlTemplateSettings;

/*
 * The platform implementation for the Microsoft SQL Server 2008 database.
 */
public class MsSql2008DatabasePlatform extends MsSql2005DatabasePlatform {

    /*
     * Creates a new platform instance.
     */
    public MsSql2008DatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }
    
    @Override
    public IDdlBuilder getDdlBuilder() {
        return new MsSql2008DdlBuilder(getName());
    }
    
    @Override
    public String getName() {
        return DatabaseNamesConstants.MSSQL2008;
    } 
}
