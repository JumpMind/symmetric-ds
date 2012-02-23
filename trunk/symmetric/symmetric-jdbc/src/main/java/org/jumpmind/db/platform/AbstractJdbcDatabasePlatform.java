package org.jumpmind.db.platform;

import javax.sql.DataSource;

import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTemplate;

abstract public class AbstractJdbcDatabasePlatform extends AbstractDatabasePlatform {

    protected DataSource dataSource;

    protected ISqlTemplate sqlTemplate;
    
    protected DatabasePlatformSettings settings;
    
    public AbstractJdbcDatabasePlatform(DataSource dataSource, DatabasePlatformSettings settings) {
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
    
    @SuppressWarnings("unchecked")
    public <T> T getDataSource() {
        return (T)dataSource;
    }

}
