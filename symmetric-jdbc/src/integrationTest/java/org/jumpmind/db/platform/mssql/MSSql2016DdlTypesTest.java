package org.jumpmind.db.platform.mssql;

import org.jumpmind.db.AbstractDdlTypesTest;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class MSSql2016DdlTypesTest extends AbstractDdlTypesTest {

    @Override
    protected String getName() {
        return DatabaseNamesConstants.MSSQL2016;
    }

    @Override
    protected String[] getDdlTypes() {
        return new String[] { 
                "bigint", "bit", "numeric", "smallint", "decimal", "smallmoney", "int", "tinyint", "money",
                "float", "real",
                "smalldatetime", "datetime", "date", "time", "datetime2",
                "char", "varchar", "text", 
                "nchar", "nvarchar", "ntext", 
                "binary", "varbinary", "image",
                "timestamp", "uniqueidentifier", "xml"//, "sql_variant"
        };
    }

}
