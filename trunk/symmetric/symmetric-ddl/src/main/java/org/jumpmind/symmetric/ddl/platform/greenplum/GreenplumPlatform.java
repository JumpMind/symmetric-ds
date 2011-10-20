package org.jumpmind.symmetric.ddl.platform.greenplum;

import org.jumpmind.symmetric.ddl.platform.postgresql.PostgreSqlPlatform;

public class GreenplumPlatform extends PostgreSqlPlatform {

    public GreenplumPlatform() {
        super();
        setModelReader(new GreenplumModelReader(this));
    }
    
}
