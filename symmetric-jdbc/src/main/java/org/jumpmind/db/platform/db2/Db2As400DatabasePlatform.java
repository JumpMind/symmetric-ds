package org.jumpmind.db.platform.db2;

import javax.sql.DataSource;

import org.jumpmind.db.sql.SqlTemplateSettings;

public class Db2As400DatabasePlatform extends Db2DatabasePlatform {

    public Db2As400DatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }

    @Override
    protected Db2DdlReader createDdlReader() {
        return new Db2As400DdlReader(this);
    }

    @Override
    protected Db2DdlBuilder createDdlBuilder() {
        return new Db2As400DdlBuilder();
    }

    public String getDefaultCatalog() {
        return "";
    }

}
