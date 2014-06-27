package org.jumpmind.symmetric.ddlutils.oracle;

import org.apache.ddlutils.platform.oracle.Oracle10Platform;

public class OraclePlatform extends Oracle10Platform {

    public OraclePlatform() {
        super();
        setModelReader(new OracleModelReader(this));
    }
}
