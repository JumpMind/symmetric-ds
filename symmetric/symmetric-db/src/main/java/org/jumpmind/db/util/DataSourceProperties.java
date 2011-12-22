package org.jumpmind.db.util;

import java.io.IOException;
import java.io.InputStream;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.jumpmind.exception.IoException;
import org.jumpmind.properties.EnvironmentSpecificProperties;

public class DataSourceProperties extends EnvironmentSpecificProperties {

    public final static String DB_DRIVER = "db.driver";
    public final static String DB_URL = "db.url";
    public final static String DB_USERNAME = "db.user";
    public final static String DB_PASSWORD = "db.password";

    private static final long serialVersionUID = 1L;

    private BasicDataSource dataSource;

    public DataSourceProperties(String systemPropertyName, InputStream is, String... propertiesForEnv) {
        super(systemPropertyName, propertiesForEnv);
        try {
            load(is);
        } catch (IOException e) {
            throw new IoException(e);
        }
    }

    public DataSource getDataSource() {
        if (dataSource == null) {
            dataSource = new BasicDataSource();
            dataSource.setDriverClassName(getProperty(DB_DRIVER));
            dataSource.setUrl(getProperty(DB_URL));
            dataSource.setUsername(getProperty(DB_USERNAME));
            dataSource.setPassword(getProperty(DB_PASSWORD));
        }
        return dataSource;
    }

}
