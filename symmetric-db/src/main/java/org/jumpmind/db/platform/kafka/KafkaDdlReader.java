package org.jumpmind.db.platform.kafka;

import java.util.List;

import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlReader;

public class KafkaDdlReader implements IDdlReader {
	protected KafkaPlatform platform;
    
	public KafkaDdlReader(IDatabasePlatform platform) {
        this.platform = (KafkaPlatform) platform;
	}

	@Override
	public Database readTables(String catalog, String schema, String[] tableTypes) {
		return null;
	}

	@Override
	public Table readTable(String catalog, String schema, String tableName) {
		return null;
	}

	@Override
	public List<String> getTableTypes() {
		return null;
	}

	@Override
	public List<String> getCatalogNames() {
		return null;
	}

	@Override
	public List<String> getSchemaNames(String catalog) {
		return null;
	}

	@Override
	public List<String> getTableNames(String catalog, String schema, String[] tableTypes) {
		return null;
	}

	@Override
	public List<String> getColumnNames(String catalog, String schema, String tableName) {
		return null;
	}

	@Override
	public List<Trigger> getTriggers(String catalog, String schema, String tableName) {
		return null;
	}

	@Override
	public Trigger getTriggerFor(Table table, String name) {
		return null;
	}

}
