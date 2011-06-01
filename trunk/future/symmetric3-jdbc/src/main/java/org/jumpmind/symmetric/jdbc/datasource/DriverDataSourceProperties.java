package org.jumpmind.symmetric.jdbc.datasource;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Driver;
import java.util.Properties;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.common.IoException;
import org.jumpmind.symmetric.core.db.SqlException;
import org.jumpmind.symmetric.jdbc.db.JdbcSqlTemplate;

public class DriverDataSourceProperties extends Properties {

    private static final long serialVersionUID = 1L;

    public DriverDataSourceProperties(String file) {
        load(new File(file));
    }

    public DriverDataSourceProperties(File file) {
        load(file);
    }

    public DriverDataSourceProperties() {
    }

    public void store(String file) {
        store(new File(file));
    }

    public DataSource getDataSource() {
        return getDataSource(System.getProperty("db.default", getProperty("db.default", "h2")));
    }

    public DataSource getDataSource(String name) {
        try {
            String driverClassName = getProperty("db." + name + ".driver");
            String url = getProperty("db." + name + ".url");
            String username = getProperty("db." + name + ".username");
            String password = getProperty("db." + name + ".password");
            DriverDataSource ds = new DriverDataSource();
            Driver jdbcDriver = (Driver) Class.forName(driverClassName).newInstance();
            ds.setDriver(jdbcDriver);
            ds.setUrl(url);
            ds.setUsername(username);
            ds.setPassword(password);
            ds.setSuppressClose(true);
            new JdbcSqlTemplate(ds).testConnection();
            return ds;
        } catch (Exception ex) {
            throw createDataSourceCreationException(name, ex);
        }
    }

    protected RuntimeException createDataSourceCreationException(String name, Exception ex) {
        String driverClassName = getProperty("db." + name + ".driver");
        String url = getProperty("db." + name + ".url");
        String username = getProperty("db." + name + ".username");
        String password = getProperty("db." + name + ".password");
        StringBuilder msg = new StringBuilder(
                "Could not create a data source with the following properties:");
        msg.append("\ndb." + name + ".driver=" + driverClassName);
        msg.append("\ndb." + name + ".url=" + url);
        msg.append("\ndb." + name + ".username=" + username);
        msg.append("\ndb." + name + ".password=" + password);
        return new SqlException(msg.toString(), ex);
    }

    public void load(File file) {
        FileReader fis = null;
        try {
            fis = new FileReader(file);
            load(fis);
        } catch (IOException ex) {
            throw new IoException(ex);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ex) {
                }
            }
        }
    }

    public void store(File file) {
        file.getParentFile().mkdirs();
        FileWriter fos = null;
        try {
            fos = new FileWriter(file);
            store(fos, "");
        } catch (IOException ex) {
            throw new IoException(ex);
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ex) {
                }
            }
        }
    }
}
