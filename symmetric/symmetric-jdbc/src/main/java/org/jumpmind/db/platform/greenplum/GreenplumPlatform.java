package org.jumpmind.db.platform.greenplum;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.platform.postgresql.PostgreSqlPlatform;

public class GreenplumPlatform extends PostgreSqlPlatform {

    /* Database name of this platform. */
    public static final String DATABASE = "Greenplum";
    public static final String DATABASENAME = "Greenplum4";
    
    /* PostgreSql can be either PostgreSql or Greenplum.  Metadata queries to determine which one */
    public static final String SQL_GET_GREENPLUM_NAME = "select gpname from gp_id";
    public static final String SQL_GET_GREENPLUM_VERSION = "select productversion from gp_version_at_initdb";    
    
    public GreenplumPlatform(DataSource dataSource, DatabasePlatformSettings settings) {
        super(dataSource, settings);
        info.setTriggersSupported(false);
        this.ddlReader = new GreenplumDdlReader(this);
    }
    
}
