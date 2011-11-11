package org.jumpmind.symmetric.ddl.platform.greenplum;

import org.jumpmind.symmetric.ddl.PlatformInfo;
import org.jumpmind.symmetric.ddl.platform.postgresql.PostgreSqlPlatform;

public class GreenplumPlatform extends PostgreSqlPlatform {

    /* Database name of this platform. */
    public static final String DATABASE = "Greenplum";
    public static final String DATABASENAME = "Greenplum4";
    
    /* PostgreSql can be either PostgreSql or Greenplum.  Metadata queries to determine which one */
    public static final String SQL_GET_GREENPLUM_NAME = "select gpname from gp_id";
    public static final String SQL_GET_GREENPLUM_VERSION = "select productversion from gp_version_at_initdb";    
    
    public GreenplumPlatform() {
        super();
        PlatformInfo info = getPlatformInfo();
        info.setTriggersSupported(false);
        setModelReader(new GreenplumModelReader(this));
    }
    
}
