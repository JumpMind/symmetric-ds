package org.jumpmind.symmetric;

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
import javax.sql.DataSource;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.JdbcDatabasePlatformFactory;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.util.AppUtils;
import org.jumpmind.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate data for populating databases.
 */
class DbFill {

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
    
    // TODO: Add to command
    private boolean continueOnError = true;
    
    private int statementType = INSERT;
    
    // Must remain 0-2 to choose randomly.
    public final static int INSERT = 0;
    public final static int UPDATE = 1;
    public final static int DELETE = 2;

    public DbFill() {
    }

    public DbFill(IDatabasePlatform platform) {
        this.platform = platform;
    }

    public DbFill(DataSource dataSource) {
        platform = JdbcDatabasePlatformFactory.createNewPlatformInstance(dataSource, null, true);
    }

    public void setDataSource(DataSource dataSource) {
        platform = JdbcDatabasePlatformFactory.createNewPlatformInstance(dataSource, null, true);
    }

    public void fillTables(String... tableNames) {
        fillTables(tableNames, null);
    }

    public void fillTables(String[] tableNames, Map<String,int[]> tableProperties) {
        Table[] tables;
        if (tableNames.length == 0) {
            // If no tableNames are provided look up all tables.
            Database db = platform.readDatabase(catalog, schema, null);
            tables = db.getTables();
            if (ignore != null) {
                // Ignore any tables matching an ignorePrefix. (e.g., "sym_")
                List<Table> tableList = new ArrayList<Table>(tables.length);
                table_loop:
                for (Table table : tables) {
                    for (String ignoreName : ignore) {
                        if (table.getName().startsWith(ignoreName)) {
                            System.out.println("Ignore table " + table.getName());
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
                        Table tableObj = platform.readTableFromDatabase(getCatalogToUse(), getSchemaToUse(),
                                fk.getForeignTableName());
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
            Table[] allTables = platform.readDatabase(getCatalogToUse(), getSchemaToUse(), null).getTables();
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
            if (i > 0) {
                // Sleep for the configured time before making another pass
                AppUtils.sleep(interval);
            }
            makePass(statementType, tables, tableProperties);
        }
    }
    
    /**
     * Perform an INSERT, UPDATE, or DELETE on every table in tables.
     * 
     * @param statement INSERT, UPDATE, or DELETE.
     * @param tables Array of tables to perform statement on. Tables must be in 
     *          insert order.
     */
    private void makePass(int statement, Table[] tables, Map<String,int[]> tableProperties) {
        ISqlTemplate sqlTemplate = platform.getSqlTemplate();

        for (Table table : tables) {
            if (tableProperties.containsKey(table.getName())) {
                statementType = randomIUD(tableProperties.get(table.getName()));
            }
            switch (statementType) {
                case INSERT:
                    System.out.println("Inserting into table " + table.getName());
                    insertRandomRecordCascading(sqlTemplate, table);
                    break;
                case UPDATE:
                    System.out.println("Updating record in table " + table.getName());
                    updateRandomRecord(sqlTemplate, table);
                    break;
                case DELETE:
                    System.out.println("Deleting record in table " + table.getName());
                    deleteRandomRecordCascading(sqlTemplate, table);
                    break;
            }
        }
    }
    
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
    
    private void insertRandomRecordCascading(ISqlTemplate sqlTemplate, Table insertTable) {
        Table[] tables = addFkInsertDependentTables(new Table[]{insertTable});
        tables = Database.sortByForeignKeys(tables);
        Map<String, Object> insertedColumns = new HashMap<String, Object>(tables.length);
        for (Table table : tables) {
            insertRandomRecord(sqlTemplate, insertedColumns, table);
        }
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
    private Row selectRandomRow(ISqlTemplate sqlTemplate, Table table) {
        Row row = null;
        // Select all rows and return the primary key columns. 
        String sql = platform.createDmlStatement(DmlType.SELECT_ALL, table.getCatalog(), table.getSchema(), table.getName(),
                table.getPrimaryKeyColumns(), table.getColumns(), null).getSql();
        final List<Row> rows = new ArrayList<Row>();
        platform.getSqlTemplate().query(sql, new ISqlRowMapper<Object>() {
            public Object mapRow(Row row) {
                rows.add(row);
                return Boolean.TRUE;
            }
        });
        
        if (rows.size() != 0) {
            int rowNum = getRand().nextInt(rows.size());
            row = rows.get(rowNum);
        }
        return row;
    }

    /**
     * Create a random row and insert it into the table.
     * 
     * @param sqlTemplate
     * @param insertedColumns
     * @param table
     */
    private void insertRandomRecord(ISqlTemplate sqlTemplate, Map<String, Object> insertedColumns, Table table) {
        DmlStatement statement = platform.createDmlStatement(DmlType.INSERT, table);
        generateRandomValues(insertedColumns, table);
        Column[] statementColumns = statement.getMetaData();
        Object[] statementValues = new Object[statementColumns.length];
        for (int j = 0; j < statementColumns.length; j++) {
            statementValues[j] = insertedColumns.get(table.getName() + "."
                    + statementColumns[j].getName());
        }
        try {
            sqlTemplate.update(statement.getSql(), statementValues);
            if (verbose) {
                System.out.println("Successful insert into " + table.getName());
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
    
    /**
     * Select a random row from the table and update all columns except for primary and foreign keys.
     * 
     * @param sqlTemplate
     * @param table
     */
    private void updateRandomRecord(ISqlTemplate sqlTemplate, Table table) {
        Row row = selectRandomRow(sqlTemplate, table);
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
            sqlTemplate.update(updStatement.getSql(), values);
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
    
    /**
     * Select a random row to delete in the table.
     * 
     * @param sqlTemplate
     * @param deletedColumns
     * @param table
     */
    private void deleteRandomRecordCascading(ISqlTemplate sqlTemplate, Table table) {
        System.out.println( "Deleting " + table.getName());
        Table[] tables = addFkDeleteDependentTables(new Table[]{table});
        
        // Find random row and delete dependent tables recursively.
        Row row = selectRandomRow(sqlTemplate, table);
        
        // Find all fk links to this table.
        for (Table tbl : tables) {
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
                    deleteDependentRecords(sqlTemplate, selectValues, tbl, tables);
                }
            }
        }
        
        DmlStatement delStatement = platform.createDmlStatement(DmlType.DELETE, table);
        Column[] keys = delStatement.getMetaData();
        Object[] keyValues = new Object[keys.length];
        for (int i=0; i<keys.length; i++) {
            keyValues[i] = row.get(keys[i].getName());
        }
        platform.getSqlTemplate().update(delStatement.getSql(), keyValues);
    }
    
    private void deleteDependentRecords(ISqlTemplate sqlTemplate, Map<Column, Object> selectColumns, Table table, Table[] tables) {

        System.out.println( "Deleting dependent table " + table.getName());
        
        // select each record
        Column[] selectColumnArray = selectColumns.keySet().toArray(new Column[0]);
        String sqlSelect = platform.createDmlStatement(DmlType.SELECT, table.getCatalog(), table.getSchema(), table.getName(),
                selectColumnArray, table.getColumns(), null).getSql();
        
        Object[] values = new Object[selectColumnArray.length];
        for (int i=0; i<selectColumnArray.length; i++) {
            values[i] = selectColumns.get(selectColumnArray[i]);
        }
        
        List<Row> rows = sqlTemplate.query(sqlSelect, values);
        
        for (Row row : rows) {
            // delete dependent records
            for (Table tbl : tables) {
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
                        deleteDependentRecords(sqlTemplate, selectValues, tbl, tables);
                    }
                }
            }
            
            DmlStatement delStatement = platform.createDmlStatement(DmlType.DELETE, table);
            Column[] keys = delStatement.getMetaData();
            Object[] keyValues = new Object[keys.length];
            for (int i=0; i<keys.length; i++) {
                keyValues[i] = row.get(keys[i].getName());
            }
            try { 
                platform.getSqlTemplate().update(delStatement.getSql(), keyValues);
            } catch(Throwable t) {
                System.out.println( "Unable to delete record from table " + table.getName());
                throw new RuntimeException(t);
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
                                    + " found in " + table.getName() + " references " + "table "
                                    + fk.getForeignTableName() + " which was not included.");
                        }
                    }
                }
            }
            objectValue = generateRandomValueForColumn(column);
            if (objectValue == null) {
                throw new RuntimeException("No random value generated for the object " + table.getName() + "." + 
                        column.getName() + " of type " + column.getMappedTypeCode());
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
            objectValue = randomString(column.getSizeAsInt()>100?100:column.getSizeAsInt());
        } else if (type == Types.OTHER) {
            if ("UUID".equalsIgnoreCase(column.getJdbcTypeName())) {
                objectValue = randomUUID();
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
            // TODO: Allow user to input seed to recreate a fill.
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

    public void setStatementType(int statementType) {
        this.statementType = statementType;
    }

}
