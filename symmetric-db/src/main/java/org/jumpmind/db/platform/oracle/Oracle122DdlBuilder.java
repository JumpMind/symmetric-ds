package org.jumpmind.db.platform.oracle;

public class Oracle122DdlBuilder extends OracleDdlBuilder {

    public Oracle122DdlBuilder() {
        super();
        databaseInfo.setMaxIdentifierLength(128);
    }
}
