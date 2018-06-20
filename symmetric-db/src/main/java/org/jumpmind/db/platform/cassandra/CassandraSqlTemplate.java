package org.jumpmind.db.platform.cassandra;

import org.jumpmind.db.sql.AbstractJavaDriverSqlTemplate;

public class CassandraSqlTemplate extends AbstractJavaDriverSqlTemplate {

	@Override
	public String getDatabaseProductName() {
		return "cassandra";
	}

}
