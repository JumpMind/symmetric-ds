package org.jumpmind.symmetric.web.rest.model;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class TableName {

    protected String catalogName;
    
    protected String schemaName;
    
    protected String tableName;

    public TableName() {
    }
    
    public TableName(String catalogName, String schemaName, String tableName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    
}
