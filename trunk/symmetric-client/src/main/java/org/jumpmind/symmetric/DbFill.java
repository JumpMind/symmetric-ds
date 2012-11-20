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
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlException;
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

        fillTables(tables);
    }
    
    /**
     * Identify the tables not included in the given list that the initial tables have FK relation ships to.
     * 
     * @param tables
     * 
     * @return The table array argument and the tables that the initial table array argument depend on.
     */
    public Table[] addFKDependentTables(Table...tables) {
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
                fkDepTblArray = addFKDependentTables(fkDepTblArray);
            }
        }
        return fkDepTblArray;
    }

    private void fillTables(Table... tables) {
        ISqlTemplate sqlTemplate = platform.getSqlTemplate();
        
        if (cascading) {
            tables = addFKDependentTables(tables);
        }
        
        tables = Database.sortByForeignKeys(tables);

        // Maintain a map of previously inserted values for foreign key constraints
        Map<String, Object> insertedColumns = new HashMap<String, Object>(tables.length);

        // Insert multiple records
        for (int i = 0; i < inputLength; i++) {

            for (Table table : tables) {
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
                    System.out.println("Succesfully inserted values in " + table.getName());
                } catch (SqlException ex) {
                    log.error("Failed to process {} with values of {}", statement.getSql(),
                            ArrayUtils.toString(statementValues));
                    throw ex;
                }
            }

            insertedColumns.clear();
        }
    }

    private void generateRandomValues(Map<String, Object> insertedColumns, Table table) {
        Column[] columns = table.getColumns();
        
        for_column: 
        for (Column column : columns) {
            Object objectValue = null;
            for (ForeignKey fk : table.getForeignKeys()) {
                for (Reference ref : fk.getReferences()) {
                    if (ref.getLocalColumnName().equalsIgnoreCase(column.getName())) {
                        objectValue = insertedColumns.get(fk.getForeignTableName() + "."
                                + ref.getForeignColumnName());
                        if (objectValue != null) {
                            insertedColumns.put(table.getName() + "." + column.getName(),
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

            int type = column.getMappedTypeCode();
            if (type == Types.DATE || type == Types.TIMESTAMP || type == Types.TIME || type == -101) {
                objectValue = randomDate();
            } else if (type == Types.CHAR) {
                objectValue = randomChar().toString();
            } else if (type == Types.INTEGER || type == Types.BIGINT) {
                objectValue = randomInt();
//            } else if (type == Types.BIT) {
//                objectValue = randomBit();
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
                objectValue = randomString(column.getSizeAsInt()>1000?1000:column.getSizeAsInt());
            } else if (type == Types.OTHER) {
                if ("UUID".equalsIgnoreCase(column.getJdbcTypeName())) {
                    objectValue = randomUUID();
                }
            }
            if (objectValue == null) {
                throw new RuntimeException("No random value generated for the object " + table.getName() + "." + column.getName() + " of type " + type);
            }
            
            insertedColumns.put(table.getName() + "." + column.getName(), objectValue);
        }
    }

    private Object randomSmallInt() {
        // TINYINT (-32768 32767)
        return new Integer(new java.util.Random().nextInt(65535) - 32768);
    }

    private Object randomFloat() {
        return new java.util.Random().nextFloat();
    }
    
    private Object randomDouble() {
        final long places = 1000000000l;
        double d = Math.random()*places;
        long l = Math.round(d);
        return ((double)l)/(double)places+2 + (double)randomInt();
    }

    private Object randomTinyInt() {
        // TINYINT (-128 to 127)
        return new Integer(new java.util.Random().nextInt(256) - 128);
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
        java.util.Random rnd = new java.util.Random();
        byte array[] = new byte[length];
        for (int i = 0; i < length; i++) {
            array[i] = (byte) rnd.nextInt(256);
        }
        return array;
    }

    private boolean randomBoolean() {
        return new java.util.Random().nextBoolean();
    }

    private BigDecimal randomBigDecimal(int size, int digits) {
        Random rnd = new java.util.Random();
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
        int rnd = new java.util.Random().nextInt(52);
        char base = (rnd < 26) ? 'A' : 'a';
        return (char) (base + rnd % 26);
    }
    
    private Date randomDate() {
        // Random date between 1970 and 2020
        long l = Math.abs(new java.util.Random().nextLong());
        long ms = (50L * 365 * 24 * 60 * 60 * 1000);
        return new Date(l % ms);
    }

    private Integer randomInt() {
        return new Integer(new java.util.Random().nextInt(1000000));
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

}
