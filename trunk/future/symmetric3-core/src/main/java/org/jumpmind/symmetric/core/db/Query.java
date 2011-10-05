package org.jumpmind.symmetric.core.db;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Table;

public class Query {

    protected Table[] tables;

    protected List<Object> args;

    protected List<Integer> argTypes;

    protected StringBuilder sql;
    
    protected String quoteString;

    static public Query create(String quoteString, int expectedNumberOfArgs, Table... tables) {
        return new Query(quoteString, expectedNumberOfArgs, tables);
    }

    static public Query create(String quoteString, Table... tables) {
        return new Query(quoteString, 0, tables);
    }

    public Query(String quoteString, int expectedNumberOfArgs, String... tables) {
        this(quoteString, expectedNumberOfArgs, buildTables(tables));
    }

    public Query(String quoteString, int expectedNumberOfArgs, Table... tables) {
        this.quoteString = quoteString;
        this.tables = tables;
        this.args = new ArrayList<Object>(expectedNumberOfArgs);
        this.argTypes = new ArrayList<Integer>(expectedNumberOfArgs);
        this.sql = select(quoteString, tables);
    }

    public String getSql() {
        return sql.toString().trim();
    }

    public Object[] getArgs() {
        return args.toArray(new Object[args.size()]);
    }

    public int[] getArgTypes() {
        int[] array = null;
        if (argTypes.size() > 0 && argTypes.size() == args.size()) {
            array = new int[argTypes.size()];
            for (int i = 0; i < argTypes.size(); i++) {
                Integer type = argTypes.get(i);
                if (type != null) {
                    array[i] = type;
                }
            }

        }
        return array;
    }

    public Query where() {
        sql.append("where ");
        return this;
    }

    public Query where(String column, String condition, Object value) {
        return where(new Column(column), condition, value);
    }

    public Query where(Column column, String condition, Object value) {
        return where().append(column, condition, value);
    }

    protected Query append(Column column, String condition, Object value) {
        String tableAlias = getTableAlias(findTableIndexWith(column, tables), tables);
        sql.append(tableAlias);
        sql.append(".");
        sql.append(column.getName());
        sql.append(condition);
        if (value == null) {
            sql.append(" is null ");
        } else {
            sql.append("? ");
            args.add(value);
            if (column.getTypeCode() != Integer.MAX_VALUE) {
                argTypes.add(column.getTypeCode());
            }
        }
        return this;
    }

    public Query append(String value) {
        sql.append(value);
        sql.append(" ");
        return this;
    }

    public Query and(String column, String condition, Object value) {
        return and(new Column(column), condition, value);
    }

    public Query and(Column column, String condition, Object value) {
        return append("and").append(column, condition, value);
    }

    public Query or(String column, String condition, Object value) {
        return or(new Column(column), condition, value);
    }

    public Query or(Column column, String condition, Object value) {
        return append("or").append(column, condition, value);
    }

    public Query startGroup() {
        sql.append("(");
        return this;
    }

    public Query endGroup() {
        sql.append(")");
        return this;
    }

    @Override
    public String toString() {
        return getSql();
    }

    protected static StringBuilder select(String quoteString, Table[] tables) {
        if (tables != null && tables.length > 0) {
            StringBuilder sql = new StringBuilder("select ");
            if (hasColumns(tables)) {
                addColumnList(sql, tables);
                addTables(sql, quoteString, tables);
            } else {
                if (tables.length == 1) {
                    sql.append("* from ");
                    sql.append(tables[0].getFullyQualifiedTableName(quoteString));
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
                sql.append((columns.length > 1 && ++columnIndex < columns.length) || i+1 < tables.length ? ", " : " ");
            }
        }
    }

    protected static String getTableAlias(int index, Table... tables) {
        String tableAlias = "t";
        if (tables.length > 1 && index >= 0) {
            tableAlias += (index + 1);
        }
        return tableAlias;
    }

    protected static void addTables(StringBuilder sql, String quoteString, Table... tables) {
        sql.append("from ");
        for (int i = 0; i < tables.length; i++) {
            Table lastTable = i > 0 ? tables[i - 1] : null;
            Table table = tables[i];
            String tableAlias = getTableAlias(i, tables);
            if (lastTable != null) {
                sql.append("inner join ");
                sql.append(table.getFullyQualifiedTableName(quoteString));
                sql.append(" ");
                sql.append(tableAlias);
                sql.append(" on ");                
                String lastTableAlias = getTableAlias(i - 1, tables);
                List<Column> columns = autoBuildJoins(lastTable, table);                
                int columnIndex = 0;
                for (Column column : columns) {
                    sql.append(lastTableAlias);
                    sql.append(".");
                    sql.append(column.getName());
                    sql.append("=");
                    sql.append(tableAlias);
                    sql.append(".");
                    sql.append(column.getName());
                    sql.append(columns.size() > 1 && ++columnIndex < columns.size() ? " AND " : " ");
                }
            } else {
                sql.append(table.getFullyQualifiedTableName(quoteString));
                sql.append(" ");
                sql.append(tableAlias);
                sql.append(" ");
            }
        }
    }

    protected static List<Column> autoBuildJoins(Table t1, Table t2) {
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

    protected static int findTableIndexWith(Column column, Table[] tables) {
        int matchOnNameIndex = -1;
        for (int i = 0; i < tables.length; i++) {
            Table table = tables[i];
            Column[] columns = table.getColumns();
            for (Column column2 : columns) {
                if (column2.equals(column)) {
                    return i;
                } else if (matchOnNameIndex < 0 && column2.getName().equals(column.getName())) {
                    matchOnNameIndex = i;
                }
            }
        }
        return matchOnNameIndex;

    }

}
