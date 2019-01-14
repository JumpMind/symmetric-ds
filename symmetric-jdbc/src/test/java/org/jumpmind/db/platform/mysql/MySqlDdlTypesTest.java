package org.jumpmind.db.platform.mysql;

import org.jumpmind.db.AbstractDdlTypesTest;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class MySqlDdlTypesTest extends AbstractDdlTypesTest {

	@Override
	protected String getName() {
		return DatabaseNamesConstants.MYSQL;
	}

	@Override
	protected String[] getDdlTypes() {
		return new String[] { "enum" };
	}

}
