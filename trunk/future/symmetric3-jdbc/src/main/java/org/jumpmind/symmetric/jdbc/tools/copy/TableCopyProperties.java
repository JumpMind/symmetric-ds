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
    
    public String getSourceSchema() {
        return getProperty("db.source.schema");
    }
    
    public String getSourceCatalog() {
        return getProperty("db.source.catalog");
    }


    public String[] getTables() {
        String tables = getProperty("copy.tables", "");
        if (StringUtils.isNotBlank(tables.trim())) {
            return tables.split(",");
        } else {
            return new String[0];
        }

    }

    public File getTargetFileDir() {
        String fileName = getProperty("db.target.file.dir");
        if (StringUtils.isNotBlank(fileName)) {
            return new File(fileName);
        } else {
            return null;
        }
    }

    public File[] getSourceFiles() {
        String dbSourceFiles = getProperty("db.source.files");
        if (StringUtils.isNotBlank(dbSourceFiles)) {
            String[] fileNames = dbSourceFiles.split(",");
            File[] files = new File[fileNames.length];
            for (int i = 0; i < files.length; i++) {
                files[i] = new File(fileNames[i].trim());
            }
            return files;
        } else {
            return null;
        }
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
