package org.jumpmind.db.platform.kafka;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlReader;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.db.util.TableRow;

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

    @Override
    public Collection<ForeignKey> getExportedKeys(Table table) {
        return null;
    }
    
    @Override
    public List<TableRow> getExportedForeignTableRows(ISqlTransaction transaction, List<TableRow> tableRows, Set<TableRow> visited) {
        return null;
    }
    
    @Override
    public List<TableRow> getImportedForeignTableRows(List<TableRow> tableRows, Set<TableRow> visited, BinaryEncoding encoding) {
        return null;
    }

}
