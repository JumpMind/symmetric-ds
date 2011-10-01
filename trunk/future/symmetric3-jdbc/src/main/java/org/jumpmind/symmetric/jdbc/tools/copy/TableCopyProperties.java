package org.jumpmind.symmetric.jdbc.tools.copy;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.common.IoException;
import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.jdbc.datasource.DriverDataSourceProperties;

public class TableCopyProperties extends DriverDataSourceProperties {

    private static final long serialVersionUID = 1L;

    public TableCopyProperties(String file) {
        load(new File(file));
    }

    public TableCopyProperties(File file) {
        load(file);
    }

    public TableCopyProperties() {
    }

    public String[] getTables() {
        return getProperty("copy.tables", "").split(",");
    }
    
    public File getTargetFile() {
        String fileName = getProperty("db.target.file");
        if (StringUtils.isNotBlank(fileName)) {
            return new File(fileName);
        } else {
            return null;
        }
    }
    
    public boolean isOverwriteTargetFile() {
        return getProperty("db.target.file.overwrite", "true").equalsIgnoreCase("true");
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

}
