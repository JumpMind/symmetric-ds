package org.jumpmind.db.platform.db2;

import javax.sql.DataSource;

import org.jumpmind.db.sql.SqlTemplateSettings;

public class Db2zOsDatabasePlatform extends Db2DatabasePlatform {

    public Db2zOsDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }

}
