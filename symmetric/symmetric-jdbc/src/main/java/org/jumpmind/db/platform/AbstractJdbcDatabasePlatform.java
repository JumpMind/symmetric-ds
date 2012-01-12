package org.jumpmind.db.platform;

import javax.sql.DataSource;

import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.jdbc.JdbcSqlTemplate;
import org.jumpmind.log.Log;

abstract public class AbstractJdbcDatabasePlatform extends AbstractDatabasePlatform {

    protected DataSource dataSource;

    protected ISqlTemplate sqlTemplate;
    
    protected DatabasePlatformSettings settings;
    
    public AbstractJdbcDatabasePlatform(DataSource dataSource, DatabasePlatformSettings settings, Log log) {
        super(log);
        this.dataSource = dataSource;
        this.settings = settings;
        createSqlTemplate();
    }
    
    protected void createSqlTemplate() {
        this.sqlTemplate = new JdbcSqlTemplate(dataSource, settings, null);        
    }

    @Override
    public ISqlTemplate getSqlTemplate() {
        return sqlTemplate;
    }

}
