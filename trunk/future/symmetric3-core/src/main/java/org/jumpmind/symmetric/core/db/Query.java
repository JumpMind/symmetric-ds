package org.jumpmind.symmetric.core.db;

import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Table;

public class Query {

    protected Table table;
    
    protected boolean quoteTable = false;
    protected boolean quoteColumns = false;
    
    static public Query create(Table table) {
        return new Query(table);
    }

    public Query(String table) {
        this(new Table(table));
     }
    
    public Query(Table table) {
       this.table = table;
    }
    
    public String getSql() {
        return null;
    }
    
    public Object[] getArgs() {
        return null;
    }
    
    public int[] getArgTypes() {
        return null;
    }
    
    public Query join(Table table, String... columns) {
        return this;
    }
    
    public Query where() {
        return this;
    }
    
    public Query where(String column, String condition, Object value) {
        return this;
    }
    
    public Query where(Column column, String condition, Object value) {
        return this;
    }
    
    public Query and(String column, String condition, Object value) {
        return this;
    }
    
    public Query and(Column column, String condition, Object value) {
        return this;
    }

    
    public Query or(String column, String condition, Object value) {
        return this;
    }
    
    public Query or(Column column, String condition, Object value) {
        return this;
    }

    
    public Query startGroup() {
        return this;
    }
    
    public Query endGroup() {
        return this;
    }
    
    @Override
    public String toString() {    
        return super.toString();
    }

    public void setQuoteColumns(boolean quoteColumns) {
        this.quoteColumns = quoteColumns;
    }
    
    public boolean isQuoteColumns() {
        return quoteColumns;
    }
    
    public void setQuoteTable(boolean quoteTable) {
        this.quoteTable = quoteTable;
    }
    
    public boolean isQuoteTable() {
        return quoteTable;
    }
    
}
