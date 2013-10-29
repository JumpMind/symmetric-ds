package org.jumpmind.db.platform.h2;

import org.jumpmind.db.AbstractDdlTypesTest;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class H2DdlTypesTest extends AbstractDdlTypesTest {

    @Override
    protected String getName() {
        return DatabaseNamesConstants.H2;
    }

    @Override
    protected String[] getDdlTypes() {
        return new String[] { "varchar(55)" };
    }

}
