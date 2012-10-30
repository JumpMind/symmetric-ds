package org.jumpmind.db.platform.mssql;

import java.sql.Types;

public class MsSql10DdlBuilder extends MsSqlDdlBuilder {
    
    public MsSql10DdlBuilder() {
        databaseInfo.addNativeTypeMapping(Types.DATE, "DATE", Types.DATE);
    }

}
