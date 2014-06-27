package org.jumpmind.db.platform.oracle;

import org.jumpmind.db.AbstractDdlTypesTest;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class OracleDdlTypesTest extends AbstractDdlTypesTest {

    @Override
    protected String getName() {
        return DatabaseNamesConstants.ORACLE;
    }

    @Override
    protected String[] getDdlTypes() {
        return new String[] { "nchar(5)", "nvarchar2(1000)", "varchar2(100)", "number(*,2)",
                "binary_float", "binary_double", "date", "timestamp with time zone",
                "timestamp with local time zone", "nclob", "rowid", "xmltype", "integer" };
    }

}
