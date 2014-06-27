package org.jumpmind.db.platform.firebird;

import org.jumpmind.db.AbstractDdlTypesTest;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class FirebirdDdlTypesTest extends AbstractDdlTypesTest {

    @Override
    protected String getName() {
        return DatabaseNamesConstants.FIREBIRD;
    }

    @Override
    protected String[] getDdlTypes() {
        return new String[] { "numeric(15,2)" };
    }

}
