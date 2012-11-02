package org.jumpmind.db.platform.sqlite;

import javax.sql.DataSource;

import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class SqliteDatabasePlatform extends AbstractJdbcDatabasePlatform implements IDatabasePlatform {

    /* The standard H2 driver. */
    public static final String JDBC_DRIVER = "org.sqlite.JDBC";
    
    public SqliteDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }

    public String getName() {
        return DatabaseNamesConstants.SQLITE;
    }

    public String getDefaultSchema() {
        // TODO Auto-generated method stub
        return null;
    }

    public String getDefaultCatalog() {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    protected SqliteDdlBuilder createDdlBuilder() {
        return  new SqliteDdlBuilder();
    }

    @Override
    protected SqliteDdlReader createDdlReader() {
        return new SqliteDdlReader(this);
    }    
    
    
    protected ISqlTemplate createSqlTemplate() {
        return new SqliteJdbcSqlTemplate(dataSource, settings, null, getDatabaseInfo()); 
    }
}
