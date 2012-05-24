package org.jumpmind.db.platform;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTemplate;

abstract public class AbstractJdbcDatabasePlatform extends AbstractDatabasePlatform {

    protected DataSource dataSource;

    protected ISqlTemplate sqlTemplate;

    protected SqlTemplateSettings settings;

    public AbstractJdbcDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
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
