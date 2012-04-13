package org.jumpmind.db.platform.greenplum;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.platform.postgresql.PostgreSqlDatabasePlatform;

public class GreenplumPlatform extends PostgreSqlDatabasePlatform {

    /* PostgreSql can be either PostgreSql or Greenplum.  Metadata queries to determine which one */
    public static final String SQL_GET_GREENPLUM_NAME = "select gpname from gp_id";
    public static final String SQL_GET_GREENPLUM_VERSION = "select productversion from gp_version_at_initdb";    
    
    public GreenplumPlatform(DataSource dataSource, DatabasePlatformSettings settings) {
        super(dataSource, settings);
        this.ddlReader = new GreenplumDdlReader(this);
        this.ddlBuilder = new GreenplumDdlBuilder();
    }
    
    @Override
    public String getName() {
        return DatabaseNamesConstants.GREENPLUM;
    }
    
}
