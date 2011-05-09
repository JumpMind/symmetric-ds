package org.jumpmind.symmetric.core.process.sql;

import org.jumpmind.symmetric.core.model.Table;

public class TableToExtract {

    private Table table;
    private String condition;

    public TableToExtract() {
    }

    public TableToExtract(Table table, String condition) {
        this.table = table;
        this.condition = condition;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getCondition() {
        return condition;
    }
}
