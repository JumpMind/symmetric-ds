package org.jumpmind.db.platform;

import javax.sql.DataSource;

import org.jumpmind.db.AbstractDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.jdbc.JdbcSqlTemplate;
import org.jumpmind.util.Log;

abstract public class AbstractJdbcDatabasePlatform extends AbstractDatabasePlatform {

    protected DataSource dataSource;

    protected ISqlTemplate sqlTemplate;

    public AbstractJdbcDatabasePlatform(DataSource dataSource, Log log) {
        super(log);
        this.dataSource = dataSource;
        this.sqlTemplate = new JdbcSqlTemplate(dataSource);
    }

    @Override
    public ISqlTemplate getSqlTemplate() {
        return sqlTemplate;
    }

}
