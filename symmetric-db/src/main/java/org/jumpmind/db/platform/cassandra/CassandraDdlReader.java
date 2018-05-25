package org.jumpmind.db.platform.cassandra;

import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlReader;

public class CassandraDdlReader implements IDdlReader {
	protected CassandraPlatform platform;
    
	public CassandraDdlReader(IDatabasePlatform platform) {
        this.platform = (CassandraPlatform) platform;
	}
	
	@Override
	public Database readTables(String catalog, String schema, String[] tableTypes) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Table readTable(String catalog, String schema, String tableName) {
		Map<String, Table> tables = platform.getMetaData()
				.get(catalog == null ? schema : catalog);
		return tables.get(tableName.toLowerCase());
	}

	@Override
	public List<String> getTableTypes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getCatalogNames() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getSchemaNames(String catalog) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getTableNames(String catalog, String schema, String[] tableTypes) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getColumnNames(String catalog, String schema, String tableName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Trigger> getTriggers(String catalog, String schema, String tableName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Trigger getTriggerFor(Table table, String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
