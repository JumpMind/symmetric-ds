package org.jumpmind.db.platform.sqlanywhere;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class SqlAnywhere12DatabasePlatform extends SqlAnywhereDatabasePlatform {
    public SqlAnywhere12DatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }
}
