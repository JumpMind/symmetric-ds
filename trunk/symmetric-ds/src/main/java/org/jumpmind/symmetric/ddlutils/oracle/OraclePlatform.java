package org.jumpmind.symmetric.ddlutils.oracle;

import org.jumpmind.symmetric.ddl.platform.oracle.Oracle10Platform;

public class OraclePlatform extends Oracle10Platform {

    public OraclePlatform() {
        super();
        setModelReader(new OracleModelReader(this));
    }
}
