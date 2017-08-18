package org.jumpmind.symmetric.io.data.writer;

import javax.sql.DataSource;

import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.platform.IDdlReader;
import org.jumpmind.db.platform.jdbc.JdbcDdlBuilder;
import org.jumpmind.db.platform.jdbc.JdbcDdlReader;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class GenericJdbcPlatform extends AbstractJdbcDatabasePlatform {

	private String name;
	
	public GenericJdbcPlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }
	
	public void setName(String name) {
		this.name = name; 
	}
	
	@Override
	public String getName() {
		return this.name == null ? "jdbc" : this.name;
	}

	@Override
	public String getDefaultSchema() {
		return null;
	}

	@Override
	public String getDefaultCatalog() {
		return null;
	}

	@Override
	protected IDdlBuilder createDdlBuilder() {
		return new JdbcDdlBuilder(getName());
	}

	@Override
	protected IDdlReader createDdlReader() {
		return new JdbcDdlReader(this);
	}

	@Override
	public DatabaseInfo getDatabaseInfo() {
		return new DatabaseInfo();
	}
	
}
