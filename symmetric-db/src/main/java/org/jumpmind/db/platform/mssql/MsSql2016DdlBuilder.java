package org.jumpmind.db.platform.mssql;

import org.jumpmind.db.platform.DatabaseNamesConstants;

public class MsSql2016DdlBuilder extends MsSql2008DdlBuilder {

    public MsSql2016DdlBuilder() {
        this.databaseName = DatabaseNamesConstants.MSSQL2016;
        databaseInfo.setTriggersCreateOrReplaceSupported(true);
    }
}
