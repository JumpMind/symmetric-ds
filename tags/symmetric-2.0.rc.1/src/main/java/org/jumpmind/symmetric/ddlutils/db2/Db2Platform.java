package org.jumpmind.symmetric.ddlutils.db2;

import org.apache.ddlutils.platform.db2.Db2v8Platform;

public class Db2Platform extends Db2v8Platform {

    public Db2Platform() {
        super();
        setSqlBuilder(new Db2Builder(this));
    }

}
