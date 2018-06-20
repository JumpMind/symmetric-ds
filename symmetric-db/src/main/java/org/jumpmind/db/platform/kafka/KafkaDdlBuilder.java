package org.jumpmind.db.platform.kafka;

import org.jumpmind.db.platform.AbstractDdlBuilder;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class KafkaDdlBuilder extends AbstractDdlBuilder {

	public KafkaDdlBuilder() {
		super(DatabaseNamesConstants.KAFKA);
	}

}
