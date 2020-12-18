package org.jumpmind.db.platform.db2;

import java.sql.Types;

import org.jumpmind.db.platform.DatabaseNamesConstants;

public class Db2zOsDdlBuilder extends Db2DdlBuilder {
    public Db2zOsDdlBuilder() {
        this.databaseName = DatabaseNamesConstants.DB2ZOS;
        databaseInfo.addNativeTypeMapping(Types.LONGVARCHAR, "CLOB", Types.CLOB);
    }
}
