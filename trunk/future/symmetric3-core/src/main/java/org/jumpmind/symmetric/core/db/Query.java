package org.jumpmind.symmetric.core.db;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Table;

public class Query {

    protected Table[] tables;

    protected boolean quoteTable = false;
    protected boolean quoteColumns = false;

    List<Object> args;

    StringBuilder sql;

    static public Query create(Table table, int expectedNumberOfArgs) {
        return new Query(expectedNumberOfArgs, table);
    }

    static public Query create(Table table) {
        return new Query(0, table);
    }

    public Query(int expectedNumberOfArgs, String... tables) {
        this(expectedNumberOfArgs, buildTables(tables));
    }

    public Query(int expectedNumberOfArgs, Table... tables) {
        this.tables = tables;
        this.args = new ArrayList<Object>(expectedNumberOfArgs);
        this.sql = select(tables);
    }

    protected static StringBuilder select(Table... tables) {
        if (tables != null && tables.length > 0) {
            StringBuilder sql = new StringBuilder("select ");
            if (hasColumns(tables)) {
                addColumnList(sql, tables);
                addTables(sql, tables);
            } else {
                if (tables.length == 1) {
                    sql.append("* from ");
                    sql.append(tables[0].getFullyQualifiedTableName());
                } else {
                    throw new IllegalStateException(
                            "Cannot join tables if columns are not specified");
                }
            }
            return sql;
        } else {
            throw new IllegalStateException("Need a list of tables to build a select statement");
        }
    }

    protected static void addColumnList(StringBuilder sql, Table... tables) {
        for (int i = 0; i < tables.length; i++) {
            Table table = tables[i];
            String tableAlias = getTableAlias(i, tables);
            Column[] columns = table.getColumns();
            int columnIndex = 0;
            for (Column column : columns) {
                sql.append(tableAlias);
                sql.append(".");
                sql.append(column.getName());
                sql.append(columns.length > 1 && ++columnIndex < columns.length ? ", " : " ");
            }
        }
    }

    protected static String getTableAlias(int index, Table... tables) {
        String tableAlias = "";
        if (tables.length > 0) {
            tableAlias = "t" + 1;
        }
        return tableAlias;
    }

    protected static void addTables(StringBuilder sql, Table... tables) {
        sql.append("from ");
        for (int i = 0; i < tables.length; i++) {
            Table lastTable = i > 0 ? tables[i - 1] : null;
            Table table = tables[i];
            String tableAlias = getTableAlias(i, tables);
            if (lastTable != null) {
                sql.append("inner join ");
                sql.append(table.getFullyQualifiedTableName());
                sql.append(" ");
                sql.append(tableAlias);
                sql.append(" ");
                String lastTableAlias = getTableAlias(i - 1, tables);
                List<Column> columns = autoBuildColumnJoinList(lastTable, table);
                int columnIndex = 0;
                for (Column column : columns) {
                    sql.append(lastTableAlias);
                    sql.append(column.getName());
                    sql.append("=");
                    sql.append(tableAlias);
                    sql.append(".");
                    sql.append(column.getName());
                    sql.append(columns.size() > 1 && ++columnIndex < columns.size() ? " AND " : " ");
                }
            } else {
                sql.append(table.getFullyQualifiedTableName());
                sql.append(" ");
                sql.append(tableAlias);
                sql.append(" ");
            }
        }
    }

    protected static List<Column> autoBuildColumnJoinList(Table t1, Table t2) {
        List<Column> columns = new ArrayList<Column>();
        Column[] t1Columns = t1.getColumns();
        for (Column column1 : t1Columns) {
            Column[] t2Columns = t2.getColumns();
            for (Column column2 : t2Columns) {
                if (column1.getName().equals(column2.getName())) {
                    columns.add(column1);
                }
            }
        }
        return columns;
    }

    protected static boolean hasColumns(Table... tables) {
        boolean hasColumns = true;
        for (Table table : tables) {
            hasColumns &= table.getColumnCount() > 0;
        }
        return hasColumns;
    }

    protected static Table[] buildTables(String... tables) {
        if (tables != null) {
            Table[] array = new Table[tables.length];
            for (int i = 0; i < tables.length; i++) {
                array[i] = new Table(tables[i]);
            }
            return array;
        } else {
            return null;
        }
    }

    public String getSql() {
        return sql.toString();
    }

    public Object[] getArgs() {
        return args.toArray(new Object[args.size()]);
    }

    public int[] getArgTypes() {
        return null;
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
