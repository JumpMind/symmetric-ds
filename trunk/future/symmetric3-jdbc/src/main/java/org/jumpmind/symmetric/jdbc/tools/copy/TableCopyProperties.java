package org.jumpmind.symmetric.jdbc.tools.copy;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Driver;
import java.util.Properties;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.db.DbException;
import org.jumpmind.symmetric.core.io.IoException;
import org.jumpmind.symmetric.jdbc.datasource.SimpleDriverDataSource;
import org.jumpmind.symmetric.jdbc.sql.Template;

public class TableCopyProperties extends Properties {

    private static final long serialVersionUID = 1L;

    public TableCopyProperties(String file) {
        load(new File(file));
    }
    
    public TableCopyProperties(File file) {
        load(file);
    }

    public TableCopyProperties() {
    }

    public void store(String file) {
        store(new File(file));
    }

    public String[] getTables() {
        return getProperty("copy.tables", "").split(",");
    }

    public String getConditionForTable(String tableName) {
        return getProperty("copy.tables." + tableName + ".condition", "");
    }

    public DataSource getTargetDataSource() {
        return getDataSource("target");
    }

    public DataSource getSourceDataSource() {
        return getDataSource("source");
    }

    protected DataSource getDataSource(String name) {
        try {
            String driverClassName = getProperty("copy.database." + name + ".driver");
            String url = getProperty("copy.database." + name + ".url");
            String username = getProperty("copy.database." + name + ".username");
            String password = getProperty("copy.database." + name + ".password");
            SimpleDriverDataSource ds = new SimpleDriverDataSource();
            Driver jdbcDriver = (Driver) Class.forName(driverClassName).newInstance();
            ds.setDriver(jdbcDriver);
            ds.setUrl(url);
            ds.setUsername(username);
            ds.setPassword(password);
            new Template(ds).testConnection();
            return ds;
        } catch (Exception ex) {
            throw createDataSourceCreationException(name, ex);
        }
    }

    protected RuntimeException createDataSourceCreationException(String name, Exception ex) {
        String driverClassName = getProperty("copy.database." + name + ".driver");
        String url = getProperty("copy.database." + name + ".url");
        String username = getProperty("copy.database." + name + ".username");
        String password = getProperty("copy.database." + name + ".password");
        StringBuilder msg = new StringBuilder(
                "Could not create a data source with the following properties:");
        msg.append("\ncopy.database." + name + ".driver=" + driverClassName);
        msg.append("\ncopy.database." + name + ".url=" + url);
        msg.append("\ncopy.database." + name + ".username=" + username);
        msg.append("\ncopy.database." + name + ".password=" + password);
        return new DbException(msg.toString(), ex);
    }

    public void loadExample() {
        try {
            load(getExampleInputStream());
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    public static InputStream getExampleInputStream() {
        return TableCopyProperties.class.getResourceAsStream("template-tablecopy.properties");
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
            store(fos, "SymmetricDS Table Copy Utility");
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
