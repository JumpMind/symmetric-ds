/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.io.data;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.util.BinaryEncoding;
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
    
    private String prefixed[] = null;
    
    private int inputLength = 1;

    private Random rand = null;

    private int interval = 0;

    private boolean debug = false;

    private boolean verbose = false;

    private boolean continueOnError = false;
    
    private boolean print = false;
    
    private String textColumnExpression;
    
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
                                log.info("Ignore table " + table.getName());
                            }
                            continue table_loop;
                        }
                    }
                    for (String prefixedName : prefixed) {
                        if (!table.getName().startsWith(prefixedName)) {
                            if (verbose) {
                                log.info("Non prefixed table (" + prefixedName + ")" + table.getName());
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
                        log.info("Inserting into table " + table.getName());
                    }
                    insertRandomRecord(table);
                    break;
                case UPDATE:
                    if (verbose) {
                        log.info("Updating record in table " + table.getName());
                    }
                    updateRandomRecord(table);
                    break;
                case DELETE:
                    if (verbose) {
                        log.info("Deleting record in table " + table.getName());
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
                table.getPrimaryKeyColumns(), table.getColumns(), null, textColumnExpression).getSql();
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

    private void updateRandomRecord(Table table) {
    	DmlStatement updStatement = createUpdateDmlStatement(table); 
    	Row row = createRandomUpdateValues(updStatement, table);
        try {
            platform.getSqlTemplate().update(updStatement.getSql(), row.toArray(table.getColumnNames()));
            if (verbose) {
                log.info("Successful update in " + table.getName());
            }
        } catch (SqlException ex) {
            log.info("Failed to process {} with values of {}", updStatement.getSql(),
                    ArrayUtils.toString(row.toArray(table.getColumnNames())));
            if (continueOnError) {
                if (debug) {
                    log.info(ex.getMessage(), ex);
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
    private void insertRandomRecord(Table table) {
    	DmlStatement insertStatement = createInsertDmlStatement(table); 
    	Row row = createRandomInsertValues(insertStatement, table);
        try {
            platform.getSqlTemplate().update(insertStatement.getSql(), insertStatement.getValueArray(row.toArray(table.getColumnNames()), 
                    row.toArray(table.getPrimaryKeyColumnNames())));
            if (verbose) {
                log.info("Successful update in " + table.getName());
            }
        } catch (SqlException ex) {
            log.info("Failed to process {} with values of {}", insertStatement.getSql(),
                    ArrayUtils.toString(row.toArray(table.getColumnNames())));
            if (continueOnError) {
                if (debug) {
                    log.info(ex.getMessage(), ex);
                }
            } else {
                throw ex;
            }
        }
    }
    
    public String createDynamicRandomInsertSql(Table table) {
    	DmlStatement insertStatement = createInsertDmlStatement(table);
    	Row row = createRandomInsertValues(insertStatement, table);
    	return insertStatement.buildDynamicSql(BinaryEncoding.HEX, row, false, true);
    }
    
    public String createDynamicRandomUpdateSql(Table table) {
    	DmlStatement updStatement = createUpdateDmlStatement(table);
    	Row row = createRandomUpdateValues(updStatement, table);
    	return updStatement.buildDynamicSql(BinaryEncoding.HEX, row, false, true);
    }
    
    public String createDynamicRandomDeleteSql(Table table) {
    	DmlStatement deleteStatement = createDeleteDmlStatement(table);
    	Row row = selectRandomRow(table);
    	return deleteStatement.buildDynamicDeleteSql(BinaryEncoding.HEX, row, false, true);
    }

    /**
     * Delete a random row in the given table or delete all rows matching selectColumns
     * in the given table.
     *
     * @param table Table to delete from.
     * @param selectColumns If provided, the rows that match this criteria are deleted.
     */
    private void deleteRandomRecord(Table table) {
    	DmlStatement deleteStatement = createDeleteDmlStatement(table); 
    	Row row = selectRandomRow(table);
        try {
            platform.getSqlTemplate().update(deleteStatement.getSql(), row.toArray(table.getColumnNames()));
            if (verbose) {
                log.info("Successful update in " + table.getName());
            }
        } catch (SqlException ex) {
            log.info("Failed to process {} with values of {}", deleteStatement.getSql(),
                    ArrayUtils.toString(row.toArray(table.getColumnNames())));
            if (continueOnError) {
                if (debug) {
                    log.info(ex.getMessage(), ex);
                }
            } else {
                throw ex;
            }
        }
    }

    private Object generateRandomValueForColumn(Column column) {
        Object objectValue = null;
        int type = column.getMappedTypeCode();
        if (column.isEnum()) {
            objectValue = column.getEnumValues()[new Random().nextInt(column.getEnumValues().length)];
        } else if (column.isTimestampWithTimezone()) {
            objectValue = String.format("%s %s",
                    FormatUtils.TIMESTAMP_FORMATTER.format(randomDate()),
                    AppUtils.getTimezoneOffset());
        } else if (type == Types.DATE) {
             objectValue = DateUtils.truncate(randomDate(), Calendar.DATE);
        } else if (type == Types.TIMESTAMP || type == Types.TIME) {
            objectValue = randomTimestamp();
        } else if (type == Types.INTEGER || type == Types.BIGINT) {
            objectValue = randomInt();
        } else if (type == Types.SMALLINT) {
            objectValue = randomSmallInt(column.getJdbcTypeName().toLowerCase().contains("unsigned"));
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
                || type == Types.VARBINARY ||
                // SQLServer text type
                type == -10) {
            objectValue = randomBytes();
        } else if (type == Types.ARRAY) {
            objectValue = null;
        } else if (type == Types.VARCHAR || type == Types.LONGVARCHAR || type == Types.CHAR || type == Types.CLOB) {
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
            }
        }
        return objectValue;
    }

    private Object randomSmallInt(boolean unsigned) {
        if (unsigned) {
            return new Integer(getRand().nextInt(32768));
        } else {
            // TINYINT (-32768 32767)
            return new Integer(getRand().nextInt(65535) - 32768);
        }
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
        // TINYINT (-128 to 127) or (0 to 255) depending on database platform
        return new Integer(getRand().nextInt(127));
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
        if (size == 0 && digits == 0) {
            // set the values to something reasonable
            size = 10;
            digits = 6;
        }
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
    
    private Timestamp randomTimestamp() {
        return Timestamp.valueOf(FormatUtils.TIMESTAMP_FORMATTER.format(randomDate()));
    }

    private Integer randomInt() {
        return new Integer(getRand().nextInt(1000000));
    }

    private String randomUUID() {
        return UUID.randomUUID().toString();
    }

    public String getSchemaToUse() {
        if (StringUtils.isBlank(schema)) {
            return platform.getDefaultSchema();
        } else {
            return schema;
        }
    }

    public String getCatalogToUse() {
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
	
	public DmlStatement createInsertDmlStatement(Table table) {
		return platform.createDmlStatement(DmlType.INSERT,
				table.getCatalog(), table.getSchema(), table.getName(),
				table.getPrimaryKeyColumns(), table.getColumns(),
				null, textColumnExpression);
	}
	
	public DmlStatement createUpdateDmlStatement(Table table) {
		return platform.createDmlStatement(DmlType.UPDATE,
				table.getCatalog(), table.getSchema(), table.getName(),
				table.getPrimaryKeyColumns(), table.getNonPrimaryKeyColumns(),
				null, textColumnExpression);
	}
	
	public DmlStatement createDeleteDmlStatement(Table table) {
		return platform.createDmlStatement(DmlType.DELETE,
				table.getCatalog(), table.getSchema(), table.getName(),
				table.getPrimaryKeyColumns(), table.getNonPrimaryKeyColumns(),
				null, textColumnExpression);
	}
	
	private Row createRandomInsertValues(DmlStatement updStatement, Table table) {
		Column[] columns = updStatement.getMetaData();
        Row row = new Row(columns.length);
		for (int i = 0; i < columns.length; i++) {
			row.put(columns[i].getName(), generateRandomValueForColumn(columns[i]));
		}
		return row;
	}
	
	private Row createRandomUpdateValues(DmlStatement updStatement, Table table) {
		Row row = selectRandomRow(table);
		if (row == null) {
			log.warn("Unable to update a random record in empty table '"
					+ table.getName() + "'.");
			return null;
		}
		Column[] columns = updStatement.getMetaData();
	
		// Get list of local fk reference columns
		List<String> localFkRefColumns = getLocalFkRefColumns(table);
		for (int i = 0; i < columns.length; i++) {
			if (!(columns[i].isPrimaryKey()
					|| localFkRefColumns.contains(columns[i].getName()))) {
				row.put(columns[i].getName(), generateRandomValueForColumn(columns[i]));
			}
		}
		return row;
	}
		        
    protected String getTypeValue(String type, String value) {
        if (type.equalsIgnoreCase("CHAR")) {
            value = "'" + value + "'";
        } else if (type.equalsIgnoreCase("VARCHAR")) {
            value = "'" + value + "'";
        } else if (type.equalsIgnoreCase("LONGVARCHAR")) {
            value = "'" + value + "'";
        } else if (type.equalsIgnoreCase("DATE")) {
            value = "'" + value + "'";
        } else if (type.equalsIgnoreCase("TIME")) {
            value = "'" + value + "'";
        } else if (type.equalsIgnoreCase("TIMESTAMP")) {
            value = "'" + value + "'";
        } else if (type.equalsIgnoreCase("CLOB")) {
            value = "'" + value + "'";
        } else if (type.equalsIgnoreCase("BLOB")) {
            value = "'" + value + "'";
        } else if (type.equalsIgnoreCase("ARRAY")) {
            value = "[" + value + "]";
        }
        return value;
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
    
    public String[] getPrefixed() {
        return prefixed;
    }

    public void setPrefixed(String[] prefixed) {
        this.prefixed = prefixed;
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
    
    public void setPrint(boolean print) {
    	this.print = print;
    }
    
    public boolean getPrint() {
    	return print;
    }
    
    public int getInsertWeight() {
    	return dmlWeight[0];
    }
    
    public int getUpdateWeight() {
    	return dmlWeight[1];
    }
    
    public int getDeleteWeight() {
    	return dmlWeight[2];
    }

    public void setTextColumnExpression(String textColumnExpression) {
        this.textColumnExpression = textColumnExpression;
    }
    
    public String getTextColumnExpression() {
        return textColumnExpression;
    }
}
