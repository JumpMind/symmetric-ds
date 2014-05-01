package org.jumpmind.db.platform.mssql;

import org.jumpmind.db.platform.DatabaseNamesConstants;

public class MsSql2005DdlBuilder extends MsSql2000DdlBuilder {
    
    public MsSql2005DdlBuilder() {
        super(DatabaseNamesConstants.MSSQL2005);
    }
    
    public MsSql2005DdlBuilder(String databaseName) {
        super(databaseName);
    }
}
