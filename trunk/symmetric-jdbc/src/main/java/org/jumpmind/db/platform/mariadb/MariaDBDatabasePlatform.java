package org.jumpmind.db.platform.mariadb;

import javax.sql.DataSource;

import org.jumpmind.db.platform.mysql.MySqlDatabasePlatform;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class MariaDBDatabasePlatform extends MySqlDatabasePlatform {

    public static final String SQL_GET_MARIADB_NAME = "select variable_value from information_schema.global_variables where variable_name='VERSION'";

	public MariaDBDatabasePlatform(DataSource dataSource,
			SqlTemplateSettings settings) {
		super(dataSource, settings);
	}

}
