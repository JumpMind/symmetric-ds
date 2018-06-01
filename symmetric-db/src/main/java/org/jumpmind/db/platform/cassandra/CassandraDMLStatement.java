package org.jumpmind.db.platform.cassandra;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.DmlStatement;

public class CassandraDMLStatement extends DmlStatement {

	public CassandraDMLStatement(DmlType type, String catalogName, String schemaName, String tableName,
			Column[] keysColumns, Column[] columns, boolean[] nullKeyValues, DatabaseInfo databaseInfo,
			boolean useQuotedIdentifiers, String textColumnExpression) {
		super(type, catalogName, schemaName, tableName, keysColumns, columns, nullKeyValues, databaseInfo, useQuotedIdentifiers,
				textColumnExpression);
	}

}
