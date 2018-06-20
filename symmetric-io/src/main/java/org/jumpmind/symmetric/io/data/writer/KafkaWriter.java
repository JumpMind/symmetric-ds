package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.io.data.CsvData;

public class KafkaWriter extends DynamicDefaultDatabaseWriter {

	public KafkaWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform, String prefix,
			IDatabaseWriterConflictResolver conflictResolver, DatabaseWriterSettings settings) {
		super(symmetricPlatform, targetPlatform, prefix, conflictResolver, settings);
	}

	/*
	@Override
	protected Table lookupTableAtTarget(Table sourceTable) {
		return sourceTable;
	}
	
	
	@Override
	protected void logFailureDetails(Throwable e, CsvData data, boolean logLastDmlDetails) {
	}

	@Override
	protected void allowInsertIntoAutoIncrementColumns(boolean value, Table table) {
	}
	*/
}
