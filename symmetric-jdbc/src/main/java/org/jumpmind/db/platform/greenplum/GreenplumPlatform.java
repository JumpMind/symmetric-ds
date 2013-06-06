package org.jumpmind.db.platform.greenplum;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.postgresql.PostgreSqlDatabasePlatform;
import org.jumpmind.db.platform.postgresql.PostgresLobHandler;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;

public class GreenplumPlatform extends PostgreSqlDatabasePlatform {

    /* PostgreSql can be either PostgreSql or Greenplum.  Metadata queries to determine which one */
    public static final String SQL_GET_GREENPLUM_NAME = "select gpname from gp_id";
    public static final String SQL_GET_GREENPLUM_VERSION = "select productversion from gp_version_at_initdb";    
    
    public GreenplumPlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }
        
    @Override
    protected GreenplumDdlBuilder createDdlBuilder() {
        return  new GreenplumDdlBuilder();
    }

    @Override
    protected GreenplumDdlReader createDdlReader() {
        return new GreenplumDdlReader(this);
    }        

    @Override
    protected GreenplumJdbcSqlTemplate createSqlTemplate() {
        SymmetricLobHandler lobHandler = new PostgresLobHandler();
        return new GreenplumJdbcSqlTemplate(dataSource, settings, lobHandler, getDatabaseInfo());
    }
        
    @Override
    public String getName() {
        return DatabaseNamesConstants.GREENPLUM;
    }
    
}
