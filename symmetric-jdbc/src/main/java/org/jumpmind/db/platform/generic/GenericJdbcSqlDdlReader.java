package org.jumpmind.db.platform.generic;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;

public class GenericJdbcSqlDdlReader extends AbstractJdbcDdlReader {

    public GenericJdbcSqlDdlReader(IDatabasePlatform platform) {
        super(platform);
    }
    
    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, IIndex index) {
        return "PRIMARY".equals(index.getName());
    }

	@Override
	protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData, Map<String, Object> values)
			throws SQLException {
		try {
			return super.readTable(connection, metaData, values);
		}
		catch (Exception e) {
			return null;
		}
	}
	
	@Override
	public Table readTable(String catalog, String schema, String table) {
		try {
			return super.readTable(catalog, schema, table);
		}
		catch (Exception e) {
			return null;
		}
	}
	
	@Override
	public Database readTables(String catalog, String schema, String[] tableTypes) {
		try {
			return super.readTables(catalog, schema, tableTypes);
		}
		catch (Exception e) {
			return null;
		}
	}
}
