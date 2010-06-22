package org.jumpmind.symmetric.ddlutils.firebird;

public class FirebirdPlatform extends org.jumpmind.symmetric.ddl.platform.firebird.FirebirdPlatform {

    public FirebirdPlatform() {
        super();
        setModelReader(new FirebirdModelReader(this));
    }
}
