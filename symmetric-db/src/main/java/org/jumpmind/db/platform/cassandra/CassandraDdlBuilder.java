package org.jumpmind.db.platform.cassandra;

import org.jumpmind.db.platform.AbstractDdlBuilder;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class CassandraDdlBuilder extends AbstractDdlBuilder {

	public CassandraDdlBuilder() {
		super(DatabaseNamesConstants.CASSANDRA);
	}

	
}
