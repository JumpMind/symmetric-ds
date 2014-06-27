package org.jumpmind.db.platform.postgresql;

import org.jumpmind.db.AbstractDdlTypesTest;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class PostgreSqlDdlTypesTest extends AbstractDdlTypesTest {

    @Override
    protected String getName() {
        return DatabaseNamesConstants.POSTGRESQL;
    }

    @Override
    protected String[] getDdlTypes() {
        return new String[] { "serial", "bigserial", "decimal", "numeric" };
    }

}
