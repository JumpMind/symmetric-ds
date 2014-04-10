package org.jumpmind.db.platform.mssql;

import org.jumpmind.db.AbstractDdlTypesTest;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class MSSqlDdlTypesTest extends AbstractDdlTypesTest {

    @Override
    protected String getName() {
        return DatabaseNamesConstants.MSSQL;
    }

    @Override
    protected String[] getDdlTypes() {
        return new String[] { 
                "bigint", "bit", "numeric", "smallint", "decimal", "smallmoney", "int", "tinyint", "money",
                "float", "real",
                "smalldatetime", "datetime",
                "char", "varchar", "text", 
                "nchar", "nvarchar", "ntext", 
                "binary", "varbinary", "image",
                "timestamp", "uniqueidentifier", "sql_variant", "xml"
        };
    }

}
