package org.jumpmind.symmetric.jdbc.tools.copy;

import org.jumpmind.symmetric.core.model.Table;

public class TableToCopy {

    private Table table;
    private String condition;

    public TableToCopy() {
    }

    public TableToCopy(Table table, String condition) {
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
