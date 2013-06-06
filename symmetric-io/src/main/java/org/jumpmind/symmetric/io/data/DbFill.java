package org.jumpmind.symmetric.io.data;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.util.AppUtils;
import org.jumpmind.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate data for populating databases.
 */
public class DbFill {

    final Logger log = LoggerFactory.getLogger(getClass());

    private String catalog;

    private String schema;

    private IDatabasePlatform platform;

    private boolean ignoreMissingTables;

    private boolean cascading = false;
    
    private String ignore[] = null;

    private int inputLength = 1;
    
    private Random rand = null;
    
    private int interval = 0;
    
    private boolean debug = false;
    
    private boolean verbose = false;
    
    private boolean continueOnError = false;
    
    // Weights given to insert, update, and delete commands when
    // randomly selecting a command for any given table.
    private int[] dmlWeight = {1,0,0};
    
    private Table[] allDbTablesCache = null;
    
    // -1 for no limit
    private static final int RANDOM_SELECT_SIZE = 100; 
    
    // Must remain 0-2 to choose randomly.
    public final static int INSERT = 0;
    public final static int UPDATE = 1;
    public final static int DELETE = 2;
    
    private boolean firstPass = true;

    public DbFill() {
    }

    public DbFill(IDatabasePlatform platform) {
        this.platform = platform;
    }

    public void fillTables(String... tableNames) {
        fillTables(tableNames, null);
    }

    public void fillTables(String[] tableNames, Map<String,int[]> tableProperties) {
        Table[] tables;
        if (tableNames.length == 0) {
            // If no tableNames are provided look up all tables.
            tables = getAllDbTables();
            if (ignore != null) {
                // Ignore any tables matching an ignorePrefix. (e.g., "sym_")
                List<Table> tableList = new ArrayList<Table>(tables.length);
                table_loop:
                for (Table table : tables) {
                    for (String ignoreName : ignore) {
                        if (table.getName().startsWith(ignoreName)) {
                            if (verbose) {
                                System.out.println("Ignore table " + table.getName());
                            }
                            continue table_loop;
                        }
                    }
                    tableList.add(table);
                }
                tables = tableList.toArray(new Table[tableList.size()]);
            }
        } else {
            ArrayList<Table> tableList = new ArrayList<Table>();
            for (String tableName : tableNames) {
                Table table = platform.readTableFromDatabase(getCatalogToUse(), getSchemaToUse(),
                        tableName);
                if (table != null) {
                    tableList.add(table);
                } else if (!ignoreMissingTables) {
                    throw new RuntimeException("Cannot find table " + tableName + " in catalog "
                            + getCatalogToUse() + " and schema " + getSchemaToUse());
                }
            }
            tables = tableList.toArray(new Table[tableList.size()]);
        }

        fillTables(tables, tableProperties);
    }
    
    /**
     * Identify the tables not included in the given list that the initial tables have FK relationships to.
     * 
     * @param tables
     * 
     * @return The table array argument and the tables that the initial table array argument depend on.
     */
    public Table[] addFkInsertDependentTables(Table... tables) {
        Table[] fkDepTblArray = null;
        if (tables != null) {
            List<Table> fkDepList = new ArrayList<Table>();
            Set<String> tableNames = new HashSet<String>();
            for (Table tbl : tables) {
                tableNames.add(tbl.getName());
            }
            for (Table table : tables) {
                for (ForeignKey fk : table.getForeignKeys()) {
                    if (tableNames.add(fk.getForeignTableName())) {
                        Table tableObj = getDbTable(fk.getForeignTableName());
                        fkDepList.add(tableObj);
                    }
                }
            }
            fkDepTblArray = fkDepList.toArray(new Table[fkDepList.size()]);
            fkDepTblArray = (Table[])ArrayUtils.addAll(fkDepTblArray, tables);
            if (fkDepList.size()>0) {
                fkDepTblArray = addFkInsertDependentTables(fkDepTblArray);
            }
        }
        return fkDepTblArray;
    }
    
    /**
     * Identify the tables not included in the given list that the initial tables have FK relationships to.
     * 
     * @param deleteTables
     * 
     * @return The table array argument and the tables that the initial table array argument depend on.
     */
    public Table[] addFkDeleteDependentTables(Table... deleteTables) {

        Table[] fkDepTblArray = null;
        if (deleteTables != null) {
            List<Table> fkDepList = new ArrayList<Table>();
            Set<String> deleteTableNames = new HashSet<String>();
            for (Table tbl : deleteTables) {
                deleteTableNames.add(tbl.getName());
            }
            Table[] allTables = getAllDbTables();
            for (Table table : allTables) {
                for (ForeignKey fk : table.getForeignKeys()) {
                    if (deleteTableNames.contains(fk.getForeignTableName())) {
                        if (deleteTableNames.add(table.getName())) {
                            fkDepList.add(table);
                        }
                    }
                }
            }
            fkDepTblArray = fkDepList.toArray(new Table[fkDepList.size()]);
            fkDepTblArray = (Table[])ArrayUtils.addAll(fkDepTblArray, deleteTables);
            if (fkDepList.size()>0) {
                fkDepTblArray = addFkDeleteDependentTables(fkDepTblArray);
            }
        }
        return fkDepTblArray;
    }

    /**
     * Once we have an array of table objects we can begin sorting and IUD operations.
     * 
     * @param tables Array of table objects.
     */
    private void fillTables(Table[] tables, Map<String,int[]> tableProperties) {
        for (int i = 0; i < inputLength; i++) {
            makePass(tables, tableProperties);
        }
    }
    
    /**
     * Perform an INSERT, UPDATE, or DELETE on every table in tables.
     * 
     * @param tables Array of tables to perform statement on. Tables must be in 
     *          insert order.
     * @param tableProperties Map indicating IUD weights for each table name provided
     *          in the properties file.
     */
    private void makePass(Table[] tables, Map<String,int[]> tableProperties) {
        for (Table table : tables) {
            // Sleep for the configured time between tables
            if (!firstPass) {
                AppUtils.sleep(interval);
            } else {
                firstPass = false;
            }
            int dmlType = INSERT;
            if (tableProperties != null && tableProperties.containsKey(table.getName())) {
                dmlType = randomIUD(tableProperties.get(table.getName()));
            } else if (dmlWeight != null) {
                dmlType = randomIUD(dmlWeight);
            }
            switch (dmlType) {
                case INSERT:
                    if (verbose) {
                        System.out.println("Inserting into table " + table.getName());
                    }
                    insertRandomRecord(table);
                    break;
                case UPDATE:
                    if (verbose) {
                        System.out.println("Updating record in table " + table.getName());
                    }
                    updateRandomRecord(table);
                    break;
                case DELETE:
                    if (verbose) {
                        System.out.println("Deleting record in table " + table.getName());
                    }
                    deleteRandomRecord(table);
                    break;
            }
        }
    }
    
    /**
     * Given a table's IUD weights a random DML statement type is chosen. 
     * 
     * @param iudWeight
     * @return 
     */
    private int randomIUD(int[] iudWeight) {
        if (iudWeight.length != 3) {
            throw new RuntimeException("Incorrect number of IUD weights provided.");
        }
        int total = iudWeight[0] + iudWeight[1] + iudWeight[2];
        if (total == 0) {
            return INSERT;
        }
        int rVal = getRand().nextInt(total);
        if (rVal < iudWeight[0]) {
            return INSERT;
        } else if (rVal < iudWeight[0] + iudWeight[1]) {
            return UPDATE;
        } 
        return DELETE;
    }

    /**
     * Select a random row from the table in the connected database. Return null if there are no rows.
     * 
     * TODO: Cache rows.
     * 
     * @param sqlTemplate
     * @param table The table to select a row from.
     * @return A random row from the table. Null if there are no rows.
     */
    private Row selectRandomRow(Table table) {
        Row row = null;
        // Select all rows and return the primary key columns. 
        String sql = platform.createDmlStatement(DmlType.SELECT_ALL, table.getCatalog(), table.getSchema(), table.getName(),
                table.getPrimaryKeyColumns(), table.getColumns(), null).getSql();
        final List<Row> rows = new ArrayList<Row>();
        platform.getSqlTemplate().query(sql, RANDOM_SELECT_SIZE, new ISqlRowMapper<Object>() {
            public Object mapRow(Row row) {
                rows.add(row);
                return Boolean.TRUE;
            }
        }, null, null);
        if (rows.size() != 0) {
            int rowNum = getRand().nextInt(rows.size());
            row = rows.get(rowNum);
        }
        return row;
    }

    /**
     * All dependent tables for the given table are determined and sorted by fk dependencies.
     * Each table has a record inserted, dependent tables first.
     * 
     * @param sqlTemplate
     * @param insertedColumns
     * @param table
     */
    private void insertRandomRecord(Table table) {
        
        Table[] tables = null;
        if (cascading) {
            tables = addFkInsertDependentTables(table);
            tables = Database.sortByForeignKeys(tables);
        } else {
            tables = new Table[] {table};
        }
        
        Map<String, Object> insertedColumns = new HashMap<String, Object>(tables.length);
        
        for (Table tbl : tables) {
            DmlStatement statement = platform.createDmlStatement(DmlType.INSERT, tbl);
            generateRandomValues(insertedColumns, tbl);
            Column[] statementColumns = statement.getMetaData();
            Object[] statementValues = new Object[statementColumns.length];
            for (int j = 0; j < statementColumns.length; j++) {
                statementValues[j] = insertedColumns.get(tbl.getName() + "."
                        + statementColumns[j].getName());
            }
            try {
                platform.getSqlTemplate().update(statement.getSql(), statementValues);
                if (verbose) {
                    System.out.println("Successful insert into " + tbl.getName());
                }
            } catch (SqlException ex) {
                log.error("Failed to process {} with values of {}", statement.getSql(),
                        ArrayUtils.toString(statementValues));
                if (continueOnError) {
                    if (debug) {
                        ex.printStackTrace();
                    }
                } else {
                    throw ex;
                }
            }
        }
    }
    
    /**
     * Select a random row from the table and update all columns except for primary and foreign keys.
     * 
     * @param sqlTemplate
     * @param table
     */
    private void updateRandomRecord(Table table) {
        Row row = selectRandomRow(table);
        if (row == null) {
            log.warn("Unable to update a random record in empty table '" + table.getName() + "'.");
            return;
        }
        DmlStatement updStatement = platform.createDmlStatement(DmlType.UPDATE, 
                table.getCatalog(), table.getSchema(), table.getName(),
                table.getPrimaryKeyColumns(), table.getNonPrimaryKeyColumns(), null);
        Column[] columns = updStatement.getMetaData();
        Object[] values = new Object[columns.length];
        
        // Get list of local fk reference columns
        List<String> localFkRefColumns = getLocalFkRefColumns(table);
        for (int i=0; i < columns.length; i++) {
            if (columns[i].isPrimaryKey() || localFkRefColumns.contains(columns[i].getName())) {
                values[i] = row.getString(columns[i].getName());
            } else {
                values[i] =  generateRandomValueForColumn(columns[i]);
            }
        }
        try {
            platform.getSqlTemplate().update(updStatement.getSql(), values);
            if (verbose) {
                System.out.println("Successful update in " + table.getName());
            }
        } catch (SqlException ex) {
            log.error("Failed to process {} with values of {}", updStatement.getSql(),
                    ArrayUtils.toString(values));
            if (continueOnError) {
                if (debug) {
                    ex.printStackTrace();
                }
            } else {
                throw ex;
            }
        }
    }
    
    private void deleteRandomRecord(Table table) {
        deleteRandomRecord(table, null);
    }
    
    /**
     * Delete a random row in the given table or delete all rows matching selectColumns
     * in the given table.
     * 
     * @param table Table to delete from.
     * @param selectColumns If provided, the rows that match this criteria are deleted.
     */
    private void deleteRandomRecord(Table table, Map<Column, Object> selectColumns) {
        List<Row> rows = null;
        if (selectColumns != null) {
            // Select dependent records to delete
            Column[] selectColumnArray = selectColumns.keySet().toArray(new Column[0]);
            String sqlSelect = platform.createDmlStatement(DmlType.SELECT, table.getCatalog(), table.getSchema(), table.getName(),
                    selectColumnArray, table.getColumns(), null).getSql();
            Object[] values = new Object[selectColumnArray.length];
            for (int i=0; i<selectColumnArray.length; i++) {
                values[i] = selectColumns.get(selectColumnArray[i]);
            }
            rows = platform.getSqlTemplate().query(sqlSelect, values);
        } else {
            // Select new random row to delete
            rows = new ArrayList<Row>(1);
            Row row = selectRandomRow(table);
            if (row == null) {
                log.warn("Unable to delete a random record from empty table '" + table.getName() + "'.");
                return;
            }
            rows.add(row);
        }
        
        for (Row row : rows) {
            if (cascading) {
                // Delete dependent tables
                for (Table tbl : getAllDbTables()) {
                    if (tbl.getName().equals(table.getName()))
                    {
                        continue;
                    }
                    for (ForeignKey fk : tbl.getForeignKeys()) {
                        if (fk.getForeignTableName().equals(table.getName())) {
                            Map<Column, Object> selectValues = new HashMap<Column, Object>();
                            for (Reference ref : fk.getReferences()) {
                                // Create a column/value map for each column referenced by the foreign table.
                                selectValues.put(ref.getLocalColumn(), row.getString(ref.getForeignColumnName()));
                            }
                            // Delete all records in the foreign table that map this row.
                            deleteRandomRecord(tbl, selectValues);
                        }
                    }
                }
            }
            DmlStatement statement = platform.createDmlStatement(DmlType.DELETE, table);
            Column[] keys = statement.getMetaData();
            Object[] keyValues = new Object[keys.length];
            for (int i=0; i<keys.length; i++) {
                keyValues[i] = row.get(keys[i].getName());
            }
            
            try {
                platform.getSqlTemplate().update(statement.getSql(), keyValues);
                if (verbose) {
                    System.out.println("Successful delete from " + table.getName());
                }
            } catch (SqlException ex) {
                log.error("Failed to process {} with values of {}", statement.getSql(),
                        ArrayUtils.toString(keyValues));
                if (continueOnError) {
                    if (debug) {
                        ex.printStackTrace();
                    }
                } else {
                    throw ex;
                }
            }
        }
    }
        
    /** 
     * Generates a random row for the given table. If a fk dependency exists, it is assumed
     * the foreign table has already been populated with random data. A runtime exception will
     * result if a foreign table has not already been populated.
     * 
     * Foreign table data should be included in the columnValues map.
     * 
     * @param columnValues
     * @param table
     */
    private void generateRandomValues(Map<String, Object> columnValues, Table table) {
        Column[] columns = table.getColumns();
        
        for_column: 
        for (Column column : columns) {
            Object objectValue = null;
            for (ForeignKey fk : table.getForeignKeys()) {
                for (Reference ref : fk.getReferences()) {
                    if (ref.getLocalColumnName().equalsIgnoreCase(column.getName())) {
                        objectValue = columnValues.get(fk.getForeignTableName() + "."
                                + ref.getForeignColumnName());
                        if (objectValue != null) {
                            columnValues.put(table.getName() + "." + column.getName(),
                                    objectValue);
                            continue for_column;
                        } else {
                            throw new RuntimeException("The foreign key column, " + column.getName()
                                    + ", found in " + table.getName() + " references " + "table "
                                    + fk.getForeignTableName() + " which was not included. Dependent tables will automatically be added if cascading is turned on.");
                        }
                    }
                }
            }
            objectValue = generateRandomValueForColumn(column);
            if (objectValue == null) {
                throw new RuntimeException("No random value generated for the object " + table.getName() + "." +
                        column.getName() + " of code " + column.getMappedTypeCode() + " jdbc name " + column.getJdbcTypeName());
            }
            
            columnValues.put(table.getName() + "." + column.getName(), objectValue);
        }
    }
    
    private Object generateRandomValueForColumn(Column column) {
        Object objectValue = null;
        int type = column.getMappedTypeCode();
        if (column.isTimestampWithTimezone()) {
            objectValue = String.format("%s %s",
                    FormatUtils.TIMESTAMP_FORMATTER.format(randomDate()),
                    AppUtils.getTimezoneOffset());
        } else if (type == Types.DATE || type == Types.TIMESTAMP || type == Types.TIME) {
            objectValue = randomDate();
        } else if (type == Types.CHAR) {
            objectValue = randomChar().toString();
        } else if (type == Types.INTEGER || type == Types.BIGINT) {
            objectValue = randomInt();
        } else if (type == Types.SMALLINT) {
            objectValue = randomSmallInt();
        } else if (type == Types.FLOAT) {
            objectValue = randomFloat();
        } else if (type == Types.DOUBLE) {
            objectValue = randomDouble();
        } else if (type == Types.TINYINT) {
            objectValue = randomTinyInt();
        } else if (type == Types.NUMERIC || type == Types.DECIMAL 
                || type == Types.REAL) {
            objectValue = randomBigDecimal(column.getSizeAsInt(), column.getScale());
        } else if (type == Types.BOOLEAN || type == Types.BIT) {
            objectValue = randomBoolean();
        } else if (type == Types.BLOB || type == Types.LONGVARBINARY || type == Types.BINARY
                || type == Types.VARBINARY || type == Types.CLOB ||
                // SQLServer next type
                type == -10) {
            objectValue = randomBytes();
        } else if (type == Types.ARRAY) {
            objectValue = null;
        } else if (type == Types.VARCHAR || type == Types.LONGVARCHAR) {
            int size = 0;
            // Assume if the size is 0 there is no max size configured.
            if (column.getSizeAsInt() != 0) {
                size = column.getSizeAsInt()>50?50:column.getSizeAsInt();
            } else {
                // No max length so default to 50
                size = 50;
            }
            objectValue = randomString(size);
        } else if (type == Types.OTHER) {
            if ("UUID".equalsIgnoreCase(column.getJdbcTypeName())) {
                objectValue = randomUUID();
            } else if ("active_inactive".equalsIgnoreCase(column.getJdbcTypeName())) {
                objectValue = randomBoolean() ? "Active" : "Inactive";
            }
        }
        return objectValue;
    }

    private Object randomSmallInt() {
        // TINYINT (-32768 32767)
        return new Integer(getRand().nextInt(65535) - 32768);
    }

    private Object randomFloat() {
        return getRand().nextFloat();
    }
    
    private Object randomDouble() {
        final long places = 1000000000l;
        double d = Math.random()*places;
        long l = Math.round(d);
        return ((double)l)/(double)places+2 + (double)randomInt();
    }

    private Object randomTinyInt() {
        // TINYINT (-128 to 127)
        return new Integer(getRand().nextInt(256) - 128);
    }

    private String randomString(int maxLength) {
        StringBuilder str = new StringBuilder(maxLength);
        for (int i = 0; i < maxLength; i++) {
            str.append(randomChar());
        }
        return str.toString();
    }

    private byte[] randomBytes() {
        int length = 10;
        byte array[] = new byte[length];
        for (int i = 0; i < length; i++) {
            array[i] = (byte) getRand().nextInt(256);
        }
        return array;
    }

    private boolean randomBoolean() {
        return getRand().nextBoolean();
    }

    private BigDecimal randomBigDecimal(int size, int digits) {
        Random rnd = getRand();
        StringBuilder str = new StringBuilder();
        if (size>0 && rnd.nextBoolean()) {
            str.append("-");
        }
        for (int i=0; i<size; i++) {
            if (i == size-digits)
                str.append(".");
            str.append(rnd.nextInt(10));
        }
        return new BigDecimal(str.toString());
    }

    private Character randomChar() {
        int rnd = getRand().nextInt(52);
        char base = (rnd < 26) ? 'A' : 'a';
        return (char) (base + rnd % 26);
    }
    
    private Date randomDate() {
        // Random date between 1970 and 2020
        long l = Math.abs(getRand().nextLong());
        long ms = (50L * 365 * 24 * 60 * 60 * 1000);
        return new Date(l % ms);
    }

    private Integer randomInt() {
        return new Integer(getRand().nextInt(1000000));
    }
    
    private String randomUUID() {
        return UUID.randomUUID().toString();
    }

    protected String getSchemaToUse() {
        if (StringUtils.isBlank(schema)) {
            return platform.getDefaultSchema();
        } else {
            return schema;
        }
    }

    protected String getCatalogToUse() {
        if (StringUtils.isBlank(catalog)) {
            return platform.getDefaultCatalog();
        } else {
            return catalog;
        }
    }
    
    protected List<String> getLocalFkRefColumns(Table table) {
        List<String> columns = new ArrayList<String>();
        for (ForeignKey fk : table.getForeignKeys()) {
            for (Reference ref : fk.getReferences()) {
                columns.add(ref.getLocalColumnName());
            }
        }
        return columns;
    }
    
    protected Table[] getAllDbTables() {
        if (allDbTablesCache == null) {
            allDbTablesCache = platform.readDatabase(getCatalogToUse(), getSchemaToUse(), null).getTables();
        }
        return allDbTablesCache;
    }
    
    protected Table getDbTable(String tableName) {
        if (allDbTablesCache == null) {
            allDbTablesCache = platform.readDatabase(getCatalogToUse(), getSchemaToUse(), null).getTables();
        }
        for (Table table : allDbTablesCache) {
            if (table.getName().equalsIgnoreCase(tableName)) {
                return table;
            }
        }
        return null;
    }

    public void setPlatform(IDatabasePlatform platform) {
        this.platform = platform;
    }

    public int getRecordCount() {
        return inputLength;
    }

    public void setRecordCount(int recordCount) {
        this.inputLength = recordCount;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public boolean isCascading() {
        return cascading;
    }

    public void setCascading(boolean cascading) {
        this.cascading = cascading;
    }

    public String[] getIgnore() {
        return ignore;
    }

    public void setIgnore(String[] ignore) {
        this.ignore = ignore;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }
    
    public Random getRand() {
        if (rand == null) {
            rand = new java.util.Random();
        }
        return rand;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public void setDmlWeight(int[] dmlWeight) {
        this.dmlWeight = dmlWeight;
    }

    public void setContinueOnError(boolean continueOnError) {
        this.continueOnError = continueOnError;
    }

}
