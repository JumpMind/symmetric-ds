package org.jumpmind.db.platform.kafka;

import org.jumpmind.db.sql.AbstractJavaDriverSqlTemplate;

public class KafkaSqlTemplate extends AbstractJavaDriverSqlTemplate{

	@Override
	public String getDatabaseProductName() {
		return "kafka";
	}

}
