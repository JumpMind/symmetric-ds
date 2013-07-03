package org.jumpmind.db.platform;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;

abstract public class AbstractJdbcDatabasePlatform extends AbstractDatabasePlatform {

    protected DataSource dataSource;

    protected ISqlTemplate sqlTemplate;

    protected SqlTemplateSettings settings;

    public AbstractJdbcDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        this.dataSource = dataSource;
        this.settings = settings;
        this.ddlBuilder = createDdlBuilder();
        this.sqlTemplate = createSqlTemplate();
        this.ddlReader = createDdlReader();
    }

    protected abstract IDdlBuilder createDdlBuilder();
    
    protected abstract IDdlReader createDdlReader();
    
    protected ISqlTemplate createSqlTemplate() {
        return new JdbcSqlTemplate(dataSource, settings, null, getDatabaseInfo()); 
    }

    @Override
    public ISqlTemplate getSqlTemplate() {
        return sqlTemplate;
    }

    @SuppressWarnings("unchecked")
    public <T> T getDataSource() {
        return (T) dataSource;
    }

    public void resetDataSource() {
        if (dataSource instanceof BasicDataSource) {
            BasicDataSource dbcp = (BasicDataSource) dataSource;
            try {
                dbcp.close();
            } catch (SQLException e) {
                throw sqlTemplate.translate(e);
            }
        }
    }

}
