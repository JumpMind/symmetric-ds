package org.jumpmind.symmetric.load.csv;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.load.DataLoaderContext;
import org.jumpmind.symmetric.load.TableTemplate;

public class CsvContext extends DataLoaderContext {

    private transient Map<String, TableTemplate> tableTemplateMap;

    private TableTemplate tableTemplate;

    public CsvContext() {
        this.tableTemplateMap = new HashMap<String, TableTemplate>();
    }
    
    public void setTableName(String tableName) {
        super.setTableName(tableName);
        this.tableTemplate = tableTemplateMap.get(tableName);
    }

    public void setKeyNames(String[] keyNames) {
        super.setKeyNames(keyNames);
        tableTemplate.setKeyNames(keyNames);
    }

    public void setColumnNames(String[] columnNames) {
        super.setColumnNames(columnNames);
        tableTemplate.setColumnNames(columnNames);
    }

    public Collection<TableTemplate> getAllTableTemplates() {
        return tableTemplateMap.values();
    }

    public TableTemplate getTableTemplate() {
        return tableTemplate;
    }

    public void setTableTemplate(TableTemplate tableTemplate) {
        this.tableTemplate = tableTemplate;
        tableTemplateMap.put(getTableName(), tableTemplate);
    }

}
