package org.jumpmind.db.platform.nuodb;

import org.jumpmind.db.AbstractDdlTypesTest;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class NuoDbDdlTypesTest extends AbstractDdlTypesTest {

	@Override
	protected String getName() {
		return DatabaseNamesConstants.NUODB;
	}

	@Override
	protected String[] getDdlTypes() {
		return new String[] { "enum" };
	}

}
