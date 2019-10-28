package org.jumpmind.db.platform;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.ForeignKey.ForeignKeyAction;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.IndexColumn;
import org.jumpmind.db.model.NonUniqueIndex;
import org.jumpmind.db.model.PlatformColumn;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.model.UniqueIndex;
import org.jumpmind.db.sql.IConnectionCallback;
import org.jumpmind.db.sql.IConnectionHandler;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * An utility class to create a Database model from a live database.
 */
public abstract class AbstractJdbcDdlReader implements IDdlReader {

    /* The Log to which logging calls will be made. */
    protected Logger log = LoggerFactory.getLogger(getClass());

    /* The descriptors for the relevant columns in the table meta data. */
    private final List<MetaDataColumnDescriptor> _columnsForTable;

    /* The descriptors for the relevant columns in the table column meta data. */
    private final List<MetaDataColumnDescriptor> _columnsForColumn;

    /* The descriptors for the relevant columns in the primary key meta data. */
    private final List<MetaDataColumnDescriptor> _columnsForPK;

    /* The descriptors for the relevant columns in the foreign key meta data. */
    private final List<MetaDataColumnDescriptor> _columnsForFK;

    /* The descriptors for the relevant columns in the index meta data. */
    private final List<MetaDataColumnDescriptor> _columnsForIndex;

    /* The platform that this model reader belongs to. */
    protected IDatabasePlatform platform;
    
    /*
     * Contains default column sizes (minimum sizes that a JDBC-compliant db
     * must support).
     */
    private HashMap<Integer, String> _defaultSizes = new HashMap<Integer, String>();

    /* The default database catalog to read. */
    private String _defaultCatalogPattern = "%";

    /* The default database schema(s) to read. */
    private String _defaultSchemaPattern = "%";

    /* The default pattern for reading all tables. */
    private String _defaultTablePattern = "%";

    /* The default pattern for reading all columns. */
    private String _defaultColumnPattern;

    /* The table types to recognize per default. */
    private String[] _defaultTableTypes = { "TABLE" };

    public AbstractJdbcDdlReader(IDatabasePlatform platform) {
        this.platform = platform;

        _defaultSizes.put(new Integer(Types.CHAR), "254");
        _defaultSizes.put(new Integer(Types.VARCHAR), "254");
        _defaultSizes.put(new Integer(Types.LONGVARCHAR), "254");
        _defaultSizes.put(new Integer(Types.BINARY), "254");
        _defaultSizes.put(new Integer(Types.VARBINARY), "254");
        _defaultSizes.put(new Integer(Types.LONGVARBINARY), "254");
        _defaultSizes.put(new Integer(Types.INTEGER), "32");
        _defaultSizes.put(new Integer(Types.BIGINT), "64");
        _defaultSizes.put(new Integer(Types.REAL), "7,0");
        _defaultSizes.put(new Integer(Types.FLOAT), "15,0");
        _defaultSizes.put(new Integer(Types.DOUBLE), "15,0");
        _defaultSizes.put(new Integer(Types.DECIMAL), "15,15");
        _defaultSizes.put(new Integer(Types.NUMERIC), "15,15");

        _columnsForTable = initColumnsForTable();
        _columnsForColumn = initColumnsForColumn();
        _columnsForPK = initColumnsForPK();
        _columnsForFK = initColumnsForFK();
        _columnsForIndex = initColumnsForIndex();
    }
    
    @Override
    public List<Trigger> getTriggers(String catalog, String schema,
    		String tableName) {
    	return Collections.emptyList();
    }

    /*
     * Returns the platform that this model reader belongs to.
     * 
     * @return The platform
     */
    public IDatabasePlatform getPlatform() {
        return platform;
    }

    /*
     * Returns the platform specific settings.
     * 
     * @return The platform settings
     */
    public DatabaseInfo getPlatformInfo() {
        return platform.getDatabaseInfo();
    }

    /*
     * Returns descriptors for the columns that shall be read from the result
     * set when reading the meta data for a table. Note that the columns are
     * read in the order defined by this list.<br/> Redefine this method if you
     * want more columns or a different order.
     * 
     * @return The descriptors for the result set columns
     */
    protected List<MetaDataColumnDescriptor> initColumnsForTable() {
        List<MetaDataColumnDescriptor> result = new ArrayList<MetaDataColumnDescriptor>();

        result.add(new MetaDataColumnDescriptor(getName("TABLE_NAME"), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor(getName("TABLE_TYPE"), Types.VARCHAR, "UNKNOWN"));
        result.add(new MetaDataColumnDescriptor(getResultSetCatalogName(), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor(getResultSetSchemaName(), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor(getName("REMARKS"), Types.VARCHAR));

        return result;
    }

    /*
     * Returns descriptors for the columns that shall be read from the result
     * set when reading the meta data for table columns. Note that the columns
     * are read in the order defined by this list.<br/> Redefine this method if
     * you want more columns or a different order.
     * 
     * @return The map column name -> descriptor for the result set columns
     */
    protected List<MetaDataColumnDescriptor> initColumnsForColumn() {
        List<MetaDataColumnDescriptor> result = new ArrayList<MetaDataColumnDescriptor>();

        // As suggested by Alexandre Borgoltz, we're reading the COLUMN_DEF
        // first because Oracle
        // has problems otherwise (it seemingly requires a LONG column to be the
        // first to be read)
        // See also DDLUTILS-29
        result.add(new MetaDataColumnDescriptor(getName("COLUMN_DEF"), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor(getName("COLUMN_DEFAULT"), Types.VARCHAR));

        // we're also reading the table name so that a model reader impl can
        // filter manually
        result.add(new MetaDataColumnDescriptor(getName("TABLE_NAME"), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor(getName("COLUMN_NAME"), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor(getName("TYPE_NAME"), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor(getName("DATA_TYPE"), Types.INTEGER, new Integer(
                java.sql.Types.OTHER)));
        result.add(new MetaDataColumnDescriptor(getName("NUM_PREC_RADIX"), Types.INTEGER, new Integer(10)));
        result.add(new MetaDataColumnDescriptor(getName("DECIMAL_DIGITS"), Types.INTEGER, new Integer(0)));
        result.add(new MetaDataColumnDescriptor(getName("COLUMN_SIZE"), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor(getName("IS_NULLABLE"), Types.VARCHAR, "YES"));
        result.add(new MetaDataColumnDescriptor(getName("IS_AUTOINCREMENT"), Types.VARCHAR, "YES"));
        result.add(new MetaDataColumnDescriptor(getName("REMARKS"), Types.VARCHAR));

        return result;
    }

    /*
     * Returns descriptors for the columns that shall be read from the result
     * set when reading the meta data for primary keys. Note that the columns
     * are read in the order defined by this list.<br/> Redefine this method if
     * you want more columns or a different order.
     * 
     * @return The map column name -> descriptor for the result set columns
     */
    protected List<MetaDataColumnDescriptor> initColumnsForPK() {
        List<MetaDataColumnDescriptor> result = new ArrayList<MetaDataColumnDescriptor>();

        result.add(new MetaDataColumnDescriptor(getName("COLUMN_NAME"), Types.VARCHAR));
        // we're also reading the table name so that a model reader impl can
        // filter manually
        result.add(new MetaDataColumnDescriptor(getName("TABLE_NAME"), Types.VARCHAR));
        // the name of the primary key is currently only interesting to the pk
        // index name resolution
        result.add(new MetaDataColumnDescriptor(getName("PK_NAME"), Types.VARCHAR));

        return result;
    }

    /*
     * Returns descriptors for the columns that shall be read from the result
     * set when reading the meta data for foreign keys originating from a table.
     * Note that the columns are read in the order defined by this list.<br/>
     * Redefine this method if you want more columns or a different order.
     * 
     * @return The map column name -> descriptor for the result set columns
     */
    protected List<MetaDataColumnDescriptor> initColumnsForFK() {
        List<MetaDataColumnDescriptor> result = new ArrayList<MetaDataColumnDescriptor>();

        result.add(new MetaDataColumnDescriptor(getName("PKTABLE_NAME"), Types.VARCHAR));
        // we're also reading the table name so that a model reader impl can
        // filter manually
        result.add(new MetaDataColumnDescriptor(getName("FKTABLE_NAME"), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor(getName("KEY_SEQ"), Types.TINYINT, new Short((short) 0)));
        result.add(new MetaDataColumnDescriptor(getName("FK_NAME"), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor(getName("FKTABLE_NAME"), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor(getName("PKCOLUMN_NAME"), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor(getName("FKCOLUMN_NAME"), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor(getName("UPDATE_RULE"), Types.TINYINT));
        result.add(new MetaDataColumnDescriptor(getName("DELETE_RULE"), Types.TINYINT));
        return result;
    }

    /*
     * Returns descriptors for the columns that shall be read from the result
     * set when reading the meta data for indices. Note that the columns are
     * read in the order defined by this list.<br/> Redefine this method if you
     * want more columns or a different order.
     * 
     * @return The map column name -> descriptor for the result set columns
     */
    protected List<MetaDataColumnDescriptor> initColumnsForIndex() {
        List<MetaDataColumnDescriptor> result = new ArrayList<MetaDataColumnDescriptor>();

        result.add(new MetaDataColumnDescriptor(getName("INDEX_NAME"), Types.VARCHAR));
        // we're also reading the table name so that a model reader impl can
        // filter manually
        result.add(new MetaDataColumnDescriptor(getName("TABLE_NAME"), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor(getName("NON_UNIQUE"), Types.BIT, Boolean.TRUE));
        result.add(new MetaDataColumnDescriptor(getName("ORDINAL_POSITION"), Types.TINYINT, new Short(
                (short) 0)));
        result.add(new MetaDataColumnDescriptor(getName("COLUMN_NAME"), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor(getName("TYPE"), Types.TINYINT));
        return result;
    }

    
    protected String getName(String defaultName){
        return defaultName;
    }
    
    /*
     * Returns the catalog(s) in the database to read per default.
     * 
     * @return The default catalog(s)
     */
    public String getDefaultCatalogPattern() {
        return _defaultCatalogPattern;
    }

    /*
     * Sets the catalog(s) in the database to read per default.
     * 
     * @param catalogPattern The catalog(s)
     */
    public void setDefaultCatalogPattern(String catalogPattern) {
        _defaultCatalogPattern = catalogPattern;
    }

    /*
     * Returns the schema(s) in the database to read per default.
     * 
     * @return The default schema(s)
     */
    public String getDefaultSchemaPattern() {
        return _defaultSchemaPattern;
    }

    /*
     * Sets the schema(s) in the database to read per default.
     * 
     * @param schemaPattern The schema(s)
     */
    public void setDefaultSchemaPattern(String schemaPattern) {
        _defaultSchemaPattern = schemaPattern;
    }

    /*
     * Returns the default pattern to read the relevant tables from the
     * database.
     * 
     * @return The table pattern
     */
    public String getDefaultTablePattern() {
        return _defaultTablePattern;
    }

    /*
     * Sets the default pattern to read the relevant tables from the database.
     * 
     * @param tablePattern The table pattern
     */
    public void setDefaultTablePattern(String tablePattern) {
        _defaultTablePattern = tablePattern;
    }

    /*
     * Returns the default pattern to read the relevant columns from the
     * database.
     * 
     * @return The column pattern
     */
    public String getDefaultColumnPattern() {
        return _defaultColumnPattern;
    }

    /*
     * Sets the default pattern to read the relevant columns from the database.
     * 
     * @param columnPattern The column pattern
     */
    public void setDefaultColumnPattern(String columnPattern) {
        _defaultColumnPattern = columnPattern;
    }

    /*
     * Returns the table types to recognize per default.
     * 
     * @return The default table types
     */
    public String[] getDefaultTableTypes() {
        return _defaultTableTypes;
    }

    /*
     * Sets the table types to recognize per default. Typical types are "TABLE",
     * "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS",
     * "SYNONYM".
     * 
     * @param types The table types
     */
    public void setDefaultTableTypes(String[] types) {
        _defaultTableTypes = types;
    }

    /*
     * Returns the descriptors for the columns to be read from the table meta
     * data result set.
     * 
     * @return The column descriptors
     */
    protected List<MetaDataColumnDescriptor> getColumnsForTable() {
        return _columnsForTable;
    }

    /*
     * Returns the descriptors for the columns to be read from the column meta
     * data result set.
     * 
     * @return The column descriptors
     */
    protected List<MetaDataColumnDescriptor> getColumnsForColumn() {
        return _columnsForColumn;
    }

    /*
     * Returns the descriptors for the columns to be read from the primary key
     * meta data result set.
     * 
     * @return The column descriptors
     */
    protected List<MetaDataColumnDescriptor> getColumnsForPK() {
        return _columnsForPK;
    }

    /*
     * Returns the descriptors for the columns to be read from the foreign key
     * meta data result set.
     * 
     * @return The column descriptors
     */
    protected List<MetaDataColumnDescriptor> getColumnsForFK() {
        return _columnsForFK;
    }

    /*
     * Returns the descriptors for the columns to be read from the index meta
     * data result set.
     * 
     * @return The column descriptors
     */
    protected List<MetaDataColumnDescriptor> getColumnsForIndex() {
        return _columnsForIndex;
    }

    /*
     * Reads the database model from the given connection.
     * 
     * @param connection The connection
     * 
     * @param name The name of the resulting database; <code>null</code> when
     * the default name (the catalog) is desired which might be
     * <code>null</code> itself though
     * 
     * @return The database model
     */
    public Database getDatabase(Connection connection) throws SQLException {
        return readTables(null, null, null);
    }

    protected String getResultSetSchemaName() {
        return "TABLE_SCHEM";
    }

    protected String getResultSetCatalogName() {
        return "TABLE_CAT";
    }

    /*
     * Reads the database model from the given connection.
     * 
     * @param catalog The catalog to access in the database; use
     * <code>null</code> for the default value
     * 
     * @param schema The schema to access in the database; use <code>null</code>
     * for the default value
     * 
     * @param tableTypes The table types to process; use <code>null</code> or an
     * empty list for the default ones
     * 
     * @return The database model
     */
    public Database readTables(final String catalog, final String schema, final String[] tableTypes) {
        JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform.getSqlTemplate();
        return postprocessModelFromDatabase(sqlTemplate
                .execute(new IConnectionCallback<Database>() {
                    public Database execute(Connection connection) throws SQLException {
                        Database db = new Database();
                        db.setName(Table.getFullyQualifiedTablePrefix(catalog, schema));
                        db.setCatalog(catalog);
                        db.setSchema(schema);
                        db.addTables(readTables(connection, catalog, schema, tableTypes));
                        db.initialize();
                        return db;
                    }
                }));
    }

    /*
     * Allows the platform to postprocess the model just read from the database.
     * 
     * @param model The model
     */
    protected Database postprocessModelFromDatabase(Database model) {
        // Default values for CHAR/VARCHAR/LONGVARCHAR columns have quotation
        // marks around them which we'll remove now
        for (int tableIdx = 0; tableIdx < model.getTableCount(); tableIdx++) {
            postprocessTableFromDatabase(model.getTable(tableIdx));
        }
        return model;
    }

    /*
     * Reads the tables from the database metadata.
     * 
     * @param catalog The catalog to acess in the database; use
     * <code>null</code> for the default value
     * 
     * @param schemaPattern The schema(s) to acess in the database; use
     * <code>null</code> for the default value
     * 
     * @param tableTypes The table types to process; use <code>null</code> or an
     * empty list for the default ones
     * 
     * @return The tables
     */
    protected Collection<Table> readTables(Connection connection, String catalog,
            String schemaPattern, String[] tableTypes) throws SQLException {
        ResultSet tableData = null;
        try {
            DatabaseMetaDataWrapper metaData = new DatabaseMetaDataWrapper();

            metaData.setMetaData(connection.getMetaData());
            metaData.setCatalog(catalog == null ? getDefaultCatalogPattern() : catalog);
            metaData.setSchemaPattern(schemaPattern == null ? getDefaultSchemaPattern()
                    : schemaPattern);
            metaData.setTableTypes((tableTypes == null) || (tableTypes.length == 0) ? getDefaultTableTypes()
                    : tableTypes);

            tableData = metaData.getTables(getDefaultTablePattern());

            List<Table> tables = new ArrayList<Table>();

            while (tableData.next()) {
                Map<String, Object> values = readMetaData(tableData, getColumnsForTable());
                Table table = readTable(connection, metaData, values);

                if (table != null) {
                    tables.add(table);
                }
            }

            final Collator collator = Collator.getInstance();

            Collections.sort(tables, new Comparator<Table>() {
                public int compare(Table obj1, Table obj2) {
                    return collator.compare(obj1.getName().toUpperCase(), obj2.getName()
                            .toUpperCase());
                }
            });

            return tables;
        } finally {
            if (tableData != null) {
                tableData.close();
            }
        }
    }

    @Override
    public Table readTable(final String catalog, final String schema, final String table) {
        try {
            log.debug("reading table: " + table);
            JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform.getSqlTemplate();
            return postprocessTableFromDatabase(sqlTemplate.execute(new IConnectionCallback<Table>() {
                public Table execute(Connection connection) throws SQLException {
                    DatabaseMetaDataWrapper metaData = new DatabaseMetaDataWrapper();
                    metaData.setMetaData(connection.getMetaData());
                    metaData.setCatalog(catalog);
                    metaData.setSchemaPattern(schema);
                    metaData.setTableTypes(null);
    
                    ResultSet tableData = null;
                    try {
                        log.debug("getting table metadata for {}", table);
                        tableData = metaData.getTables(getTableNamePattern(table));
                        log.debug("done getting table metadata for {}", table);
                        if (tableData != null && tableData.next()) {
                            Map<String, Object> values = readMetaData(tableData, initColumnsForTable());
                            return readTable(connection, metaData, values);
                        } else {
                            log.debug("table {} not found", table);
                            return null;
                        }
                    } finally {
                        close(tableData);
                    }
                }
            }));
        } catch (SqlException e) {
            if (e.getMessage()!=null && StringUtils.containsIgnoreCase(e.getMessage(), "does not exist")) {
                return null;
            } else {
                log.error("Failed to get metadata for {}", Table.getFullyQualifiedTableName(catalog, schema, table));
                throw e;
            }
        }
    }

    protected Table postprocessTableFromDatabase(Table table) {
        if (table != null) {
            for (int columnIdx = 0; columnIdx < table.getColumnCount(); columnIdx++) {
                Column column = table.getColumn(columnIdx);

                if (TypeMap.isTextType(column.getMappedTypeCode())
                        || TypeMap.isDateTimeType(column.getMappedTypeCode())) {
                    String defaultValue = column.getDefaultValue();

                    if ((defaultValue != null) && (defaultValue.length() >= 2)
                            && defaultValue.startsWith("'") && defaultValue.endsWith("'")) {
                        defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
                        column.setDefaultValue(defaultValue);
                    }
                }
            }
        }
        return table;
    }

    protected void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ex) {

            }
        }
    }

    protected void close(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException ex) {

            }
        }
    }

    protected String getTableNamePattern(String tableName) {
        return tableName;
    }

    protected String getTableNamePatternForConstraints(String tableName) {
        return tableName;
    }

    /*
     * Reads the next table from the meta data.
     * 
     * @param metaData The database meta data
     * 
     * @param values The table metadata values as defined by {@link
     * #getColumnsForTable()}
     * 
     * @return The table or <code>null</code> if the result set row did not
     * contain a valid table
     */
    protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData,
            Map<String, Object> values) throws SQLException {
        String tableName = (String) values.get(getName("TABLE_NAME"));
        if (tableName == null) {
            tableName = (String) values.get("NAME");
        }
        try {
            Table table = null;

            if ((tableName != null) && (tableName.length() > 0)) {
                String type = (String) values.get(getName("TABLE_TYPE"));
                String[] unsupportedTableTypes = getUnsupportedTableTypes();
                for (String unsupportedTableType : unsupportedTableTypes) {
                    if (StringUtils.isNotBlank(type) && type.equals(unsupportedTableType)) {
                        return null;
                    }
                }

                table = new Table();
                table.setName(tableName);
                table.setType(type);

                String catalog = (String) values.get(getName(getResultSetCatalogName()));
                table.setCatalog(catalog);

                String schema = (String) values.get(getName(getResultSetSchemaName()));
                table.setSchema(schema);

                table.setDescription((String) values.get(getName("REMARKS")));

                table.addColumns(readColumns(metaData, tableName));

                if (table.getColumnCount() > 0) {
                    table.addForeignKeys(readForeignKeys(connection, metaData, tableName));
                    table.addIndices(readIndices(connection, metaData, tableName));

                    Collection<String> primaryKeys = readPrimaryKeyNames(metaData, tableName);

                    for (Iterator<String> it = primaryKeys.iterator(); it.hasNext();) {
                        table.findColumn(it.next(), true).setPrimaryKey(true);
                    }

                    if (getPlatformInfo().isSystemIndicesReturned()) {
                        removeSystemIndices(connection, metaData, table);
                    }
                } else {
                    table = null;
                }
            }
            return table;
        } catch (RuntimeException ex) {
            log.error(String.format("Failed to read table: %s.  Error: %s", tableName, ex.getMessage()));
            throw ex;
        } catch (SQLException ex) {
            log.error(String.format("Failed to read table: %s.  Error: %s", tableName, ex.getMessage()));
            throw ex;
        }
    }

    protected String[] getUnsupportedTableTypes() {
        return new String[0];
    }

    /*
     * Removes system indices (generated by the database for primary and foreign
     * keys) from the table.
     * 
     * @param metaData The database meta data
     * 
     * @param table The table
     */
    protected void removeSystemIndices(Connection connection, DatabaseMetaDataWrapper metaData,
            Table table) throws SQLException {
        removeInternalPrimaryKeyIndex(connection, metaData, table);

        for (int fkIdx = 0; fkIdx < table.getForeignKeyCount(); fkIdx++) {
            removeInternalForeignKeyIndex(connection, metaData, table, table.getForeignKey(fkIdx));
        }
    }

    /*
     * Tries to remove the internal index for the table's primary key.
     * 
     * @param metaData The database meta data
     * 
     * @param table The table
     */
    protected void removeInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table) throws SQLException {
        Column[] pks = table.getPrimaryKeyColumns();
        List<String> columnNames = new ArrayList<String>();

        for (int columnIdx = 0; columnIdx < pks.length; columnIdx++) {
            columnNames.add(pks[columnIdx].getName());
        }

        for (int indexIdx = 0; indexIdx < table.getIndexCount();) {
            IIndex index = table.getIndex(indexIdx);

            if (index.isUnique() && matches(index, columnNames)
                    && isInternalPrimaryKeyIndex(connection, metaData, table, index)) {
                table.removeIndex(indexIdx);
            } else {
                indexIdx++;
            }
        }
    }

    /*
     * Tries to remove the internal index for the given foreign key.
     * 
     * @param metaData The database meta data
     * 
     * @param table The table where the table is defined
     * 
     * @param fk The foreign key
     */
    protected void removeInternalForeignKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk) throws SQLException {
        List<String> columnNames = new ArrayList<String>();

        for (int columnIdx = 0; columnIdx < fk.getReferenceCount(); columnIdx++) {
            columnNames.add(fk.getReference(columnIdx).getLocalColumnName());
        }

        for (int indexIdx = 0; indexIdx < table.getIndexCount();) {
            IIndex index = table.getIndex(indexIdx);
            if (matches(index, columnNames) && isInternalForeignKeyIndex(connection, metaData, table, fk, index)) {
                fk.setAutoIndexPresent(true);
                table.removeIndex(indexIdx);
            } else {
                indexIdx++;
            }
        }
    }

    /*
     * Checks whether the given index matches the column list.
     * 
     * @param index The index
     * 
     * @param columnsToSearchFor The names of the columns that the index should
     * be for
     * 
     * @return <code>true</code> if the index matches the columns
     */
    protected boolean matches(IIndex index, List<String> columnsToSearchFor) {
        for (String column : columnsToSearchFor) {
            boolean found = false;
            for (int i = 0; i < index.getColumnCount(); i++) {
                if (column != null && column.equals(index.getColumn(i).getName())) {
                    found = true;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }
    
    /*
     * Tries to determine whether the index is the internal database-generated
     * index for the given table's primary key. Note that only unique indices
     * with the correct columns are fed to this method. Redefine this method for
     * specific platforms if there are better ways to determine internal
     * indices.
     * 
     * @param metaData The database meta data
     * 
     * @param table The table owning the index
     * 
     * @param index The index to check
     * 
     * @return <code>true</code> if the index seems to be an internal primary
     * key one
     */
    protected boolean isInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, IIndex index) throws SQLException {
        return false;
    }

    /*
     * Tries to determine whether the index is the internal database-generated
     * index for the given foreign key. Note that only non-unique indices with
     * the correct columns are fed to this method. Redefine this method for
     * specific platforms if there are better ways to determine internal
     * indices.
     * 
     * @param metaData The database meta data
     * 
     * @param table The table owning the index and foreign key
     * 
     * @param fk The foreign key
     * 
     * @param index The index to check
     * 
     * @return <code>true</code> if the index seems to be an internal primary
     * key one
     */
    protected boolean isInternalForeignKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk, IIndex index)
            throws SQLException {
        return false;
    }

    /*
     * Reads the column definitions for the indicated table.
     * 
     * @param metaData The database meta data
     * 
     * @param tableName The name of the table
     * 
     * @return The columns
     */
    protected Collection<Column> readColumns(DatabaseMetaDataWrapper metaData, String tableName)
            throws SQLException {
        ResultSet columnData = null;        
        try {
            Set<String> columnNames = new HashSet<String>();
            columnData = metaData.getColumns(getTableNamePattern(tableName),
                    getDefaultColumnPattern());

            List<Column> columns = new ArrayList<Column>();

            while (columnData.next()) {
                Map<String, Object> values = readMetaData(columnData, getColumnsForColumn());
                Column column = readColumn(metaData, values);
                if (!columnNames.contains(column.getName())) {
                    columnNames.add(column.getName());
                    columns.add(column);
                }
                
                genericizeDefaultValuesAndUpdatePlatformColumn(column);
            }
            return columns;
        } finally {
            close(columnData);
        }
    }
    
    protected void genericizeDefaultValuesAndUpdatePlatformColumn(Column column) {
        PlatformColumn platformColumn = column.findPlatformColumn(platform.getName());
        platformColumn.setDefaultValue(column.getDefaultValue());
        
        /*
         * Translate from platform specific functions to ansi sql functions
         */
        if ("getdate()".equalsIgnoreCase(column.getDefaultValue())) {
            column.setDefaultValue("CURRENT_TIMESTAMP");
        }
    }

    protected Integer mapUnknownJdbcTypeForColumn(Map<String, Object> values) {
        return null;
    }

    /*
     * Extracts a column definition from the result set.
     * 
     * @param metaData The database meta data
     * 
     * @param values The column meta data values as defined by {@link
     * #getColumnsForColumn()}
     * 
     * @return The column
     */
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        Column column = new Column();
        PlatformColumn platformColumn = new PlatformColumn();
        platformColumn.setName(platform.getName());
        
        column.setName((String) values.get(getName("COLUMN_NAME")));
        String defaultValue = (String) values.get(getName("COLUMN_DEF"));
        if (defaultValue == null) {
            defaultValue = (String) values.get(getName("COLUMN_DEFAULT"));
        }
        if (defaultValue != null) {
            defaultValue = defaultValue.trim();
            column.setDefaultValue(defaultValue);
        }

        String typeName = (String) values.get(getName("TYPE_NAME"));
        column.setJdbcTypeName(typeName);

        Integer mappedType = mapUnknownJdbcTypeForColumn(values);
        if (mappedType != null) {
            column.setMappedTypeCode(mappedType);
        } else {
            column.setMappedTypeCode((Integer) values.get(getName("DATA_TYPE")));
        }

        column.setJdbcTypeCode((Integer) values.get(getName("DATA_TYPE")));
               
        column.setPrecisionRadix(((Integer) values.get(getName("NUM_PREC_RADIX"))).intValue());

        String columnSize = (String) values.get(getName("COLUMN_SIZE"));
        int decimalDigits = ((Integer) values.get(getName("DECIMAL_DIGITS"))).intValue();

        try {
            platformColumn.setType(typeName);
            if (isNotBlank(columnSize)) {
                platformColumn.setSize(Integer.parseInt(columnSize));
            }
            platformColumn.setDecimalDigits(decimalDigits);
            column.addPlatformColumn(platformColumn);
        } catch (Exception ex) {
            log.warn("", ex);
        }        
        
        if (columnSize == null) {
            columnSize = (String) _defaultSizes.get(new Integer(column.getMappedTypeCode()));
        }
        // we're setting the size after the precision and radix in case
        // the database prefers to return them in the size value
        column.setSize(columnSize);
        if (decimalDigits != 0) {
            // if there is a scale value, set it after the size (which probably
            // did not contain
            // a scale specification)
            column.setScale(decimalDigits);
        }
        column.setRequired("NO".equalsIgnoreCase(((String) values.get(getName("IS_NULLABLE"))).trim()));
        column.setDescription((String) values.get(getName("REMARKS")));

        return column;
    }

    /*
     * Retrieves the names of the columns that make up the primary key for a
     * given table.
     * 
     * @param metaData The database meta data
     * 
     * @param tableName The name of the table from which to retrieve PK
     * information
     * 
     * @return The primary key column names
     */
    protected Collection<String> readPrimaryKeyNames(DatabaseMetaDataWrapper metaData,
            String tableName) throws SQLException {
        List<String> pks = new ArrayList<String>();
        ResultSet pkData = null;

        try {
            pkData = metaData.getPrimaryKeys(getTableNamePatternForConstraints(tableName));
            while (pkData.next()) {
                Map<String, Object> values = readMetaData(pkData, getColumnsForPK());

                pks.add(readPrimaryKeyName(metaData, values));
            }
        } finally {
            close(pkData);
        }
        return pks;
    }

    /*
     * Extracts a primary key name from the result set.
     * 
     * @param metaData The database meta data
     * 
     * @param values The primary key meta data values as defined by {@link
     * #getColumnsForPK()}
     * 
     * @return The primary key name
     */
    protected String readPrimaryKeyName(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        return (String) values.get(getName("COLUMN_NAME"));
    }

    /*
     * Retrieves the foreign keys of the indicated table.
     * 
     * @param metaData The database meta data
     * 
     * @param tableName The name of the table from which to retrieve FK
     * information
     * 
     * @return The foreign keys
     */
    protected Collection<ForeignKey> readForeignKeys(Connection connection,
            DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {
        Map<String, ForeignKey> fks = new LinkedHashMap<String, ForeignKey>();
        if (getPlatformInfo().isForeignKeysSupported()) {
            ResultSet fkData = null;
    
            try {
                fkData = metaData.getForeignKeys(getTableNamePatternForConstraints(tableName));
    
                while (fkData.next()) {
                    Map<String, Object> values = readMetaData(fkData, getColumnsForFK());
                    String fkTableName = (String)values.get(getName("FKTABLE_NAME"));
                    if (isBlank(fkTableName) || fkTableName.equalsIgnoreCase(tableName)) {
                        readForeignKey(metaData, values, fks);
                    }
                }
            } finally {
                close(fkData);
            }
        }
        return fks.values();
    }

    /*
     * Reads the next foreign key spec from the result set.
     * 
     * @param metaData The database meta data
     * 
     * @param values The foreign key meta data as defined by {@link
     * #getColumnsForFK()}
     * 
     * @param knownFks The already read foreign keys for the current table
     */
    protected void readForeignKey(DatabaseMetaDataWrapper metaData, Map<String, Object> values,
            Map<String, ForeignKey> knownFks) throws SQLException {
        String fkName = (String) values.get(getName("FK_NAME"));
        ForeignKey fk = (ForeignKey) knownFks.get(fkName);

        if (fk == null) {
            fk = new ForeignKey(fkName);
            fk.setForeignTableName((String) values.get(getName("PKTABLE_NAME")));
            try {
            		fk.setForeignTableCatalog((String) values.get(getName("PKTABLE_CAT")));
            } catch (Exception e) { }
            try {
        			fk.setForeignTableSchema((String) values.get(getName("PKTABLE_SCHEM")));
	        	} catch (Exception e) { }
            readForeignKeyUpdateRule(values, fk);
            readForeignKeyDeleteRule(values, fk);
	        
	        knownFks.put(fkName, fk);
        }

        Reference ref = new Reference();

        ref.setForeignColumnName((String) values.get(getName("PKCOLUMN_NAME")));
        ref.setLocalColumnName((String) values.get(getName("FKCOLUMN_NAME")));
        if (values.containsKey(getName("KEY_SEQ"))) {
            ref.setSequenceValue(((Short) values.get(getName("KEY_SEQ"))).intValue());
        }
        fk.addReference(ref);
    }
    
    protected void readForeignKeyUpdateRule(Map<String, Object> values, ForeignKey fk) {
        if(values.get(getName("UPDATE_RULE")) != null && values.get(getName("UPDATE_RULE")) instanceof Short) {
            fk.setOnUpdateAction(ForeignKey.getForeignKeyAction((Short) values.get(getName("UPDATE_RULE"))));
        } else {
            fk.setOnUpdateAction(ForeignKeyAction.NOACTION);
        }
    }
    
    protected void readForeignKeyDeleteRule(Map<String, Object> values, ForeignKey fk) {
        if(values.get(getName("DELETE_RULE")) != null && values.get(getName("DELETE_RULE")) instanceof Short) {
            fk.setOnDeleteAction(ForeignKey.getForeignKeyAction((Short) values.get(getName("DELETE_RULE"))));
        } else {
            fk.setOnDeleteAction(ForeignKeyAction.NOACTION);
        }
    }

    /*
     * Determines the indices for the indicated table.
     * 
     * @param metaData The database meta data
     * 
     * @param tableName The name of the table
     * 
     * @return The list of indices
     */
    protected Collection<IIndex> readIndices(Connection connection,
            DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {
        Map<String, IIndex> indices = new LinkedHashMap<String, IIndex>();
        if (getPlatformInfo().isIndicesSupported()) {
            ResultSet indexData = null;
    
            try {
                indexData = metaData.getIndices(getTableNamePatternForConstraints(tableName), false, false);
    
                while (indexData.next()) {
                    Map<String, Object> values = readMetaData(indexData, getColumnsForIndex());
    
                    readIndex(metaData, values, indices);
                }
            } finally {
                close(indexData);
            }
        }
        return indices.values();
    }

    /*
     * Reads the next index spec from the result set.
     * 
     * @param metaData The database meta data
     * 
     * @param values The index meta data as defined by {@link
     * #getColumnsForIndex()}
     * 
     * @param knownIndices The already read indices for the current table
     */
    protected void readIndex(DatabaseMetaDataWrapper metaData, Map<String, Object> values,
            Map<String, IIndex> knownIndices) throws SQLException {
        Short indexType = (Short) values.get(getName("TYPE"));

        // we're ignoring statistic indices
        if ((indexType != null) && (indexType.shortValue() == DatabaseMetaData.tableIndexStatistic)) {
            return;
        }

        String indexName = (String) values.get(getName("INDEX_NAME"));

        if (indexName != null) {
            IIndex index = (IIndex) knownIndices.get(indexName);

            if (index == null) {
                if (((Boolean) values.get(getName("NON_UNIQUE"))).booleanValue()) {
                    index = new NonUniqueIndex();
                } else {
                    index = new UniqueIndex();
                }

                index.setName(indexName);
                knownIndices.put(indexName, index);
            }

            IndexColumn indexColumn = new IndexColumn();

            String columnName = (String) values.get(getName("COLUMN_NAME"));
            if (columnName.startsWith("\"") && columnName.endsWith("\"")) {
                columnName = columnName.substring(1, columnName.length() - 1);
            }
            indexColumn.setName(columnName);
            if (values.containsKey(getName("ORDINAL_POSITION"))) {
                indexColumn.setOrdinalPosition(((Short) values.get(getName("ORDINAL_POSITION"))).intValue());
            }
            index.addColumn(indexColumn);
        }
    }

    /*
     * Reads the indicated columns from the result set.
     * 
     * @param resultSet The result set
     * 
     * @param columnDescriptors The descriptors of the columns to read
     * 
     * @return The read values keyed by the column name
     */
    protected Map<String, Object> readMetaData(ResultSet resultSet,
            List<MetaDataColumnDescriptor> columnDescriptors) throws SQLException {
        HashMap<String, Object> values = new HashMap<String, Object>();
        ResultSetMetaData meta = resultSet.getMetaData();
        int columnCount = meta.getColumnCount();
        Set<String> processed = new HashSet<String>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            boolean foundMetaDataDescriptor = false;
            String columnName = meta.getColumnName(i);
            for (MetaDataColumnDescriptor metaDataColumnDescriptor : columnDescriptors) {
                if (metaDataColumnDescriptor.getName().equals(columnName)) {
                    foundMetaDataDescriptor = true;
                    values.put(metaDataColumnDescriptor.getName(),
                            metaDataColumnDescriptor.readColumn(resultSet));
                    processed.add(columnName);
                    break;
                }
            }

            /*
             * Put all metadata values into the map for easy debugging
             * of drivers that return nonstandard names
             */
            if (!foundMetaDataDescriptor) {
                values.put(columnName, resultSet.getObject(i));
            }

        }
        
        for (MetaDataColumnDescriptor metaDataColumnDescriptor : columnDescriptors) {
            if (!processed.contains(metaDataColumnDescriptor.getName())) {
                values.put(metaDataColumnDescriptor.getName(),
                        metaDataColumnDescriptor.readColumn(resultSet));
            }
        }
        return values;
    }

    protected void determineAutoIncrementFromResultSetMetaData(Connection conn, Table table,
            final Column columnsToCheck[]) throws SQLException {
        determineAutoIncrementFromResultSetMetaData(conn, table, columnsToCheck, ".");
    }

    /*
     * Helper method that determines the auto increment status for the given
     * columns via the {@link ResultSetMetaData#isAutoIncrement(int)} method.
     * 
     * Fix problems following problems: 1) identifiers that use keywords 2)
     * different catalog and schema 3) different catalog separator character *
     * 
     * @param table The table
     * 
     * @param columnsToCheck The columns to check (e.g. the primary key columns)
     */
    protected void determineAutoIncrementFromResultSetMetaData(Connection conn, Table table,
            final Column columnsToCheck[], String catalogSeparator) throws SQLException {
        StringBuilder query = new StringBuilder();
        try {
            if (columnsToCheck == null || columnsToCheck.length == 0) {
                return;
            }
            query.append("SELECT ");
            for (int idx = 0; idx < columnsToCheck.length; idx++) {
                if (idx > 0) {
                    query.append(",");
                }
                query.append("t.");
                appendColumn(query, columnsToCheck[idx].getName());
            }
            query.append(" FROM ");

            if (table.getCatalog() != null && !table.getCatalog().trim().equals("")) {
                appendIdentifier(query, table.getCatalog());
                query.append(catalogSeparator);
            }
            if (table.getSchema() != null && !table.getSchema().trim().equals("")) {
                appendIdentifier(query, table.getSchema()).append(".");
            }
            appendIdentifier(query, table.getName()).append(" t WHERE 1 = 0");

            Statement stmt = null;
            try {
                stmt = conn.createStatement();
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Running the following query to get metadata about whether a column is an auto increment column: \n{}",
                            query);
                }
                ResultSet rs = null;
                try {
                    rs = stmt.executeQuery(query.toString());
                    ResultSetMetaData rsMetaData = rs.getMetaData();

                    for (int idx = 0; idx < columnsToCheck.length; idx++) {
                        if (log.isDebugEnabled()) {
                            log.debug(columnsToCheck[idx] + " is auto increment? "
                                    + rsMetaData.isAutoIncrement(idx + 1));
                        }
                        if (rsMetaData.isAutoIncrement(idx + 1)) {
                            columnsToCheck[idx].setAutoIncrement(true);
                        }
                    }
                } finally {
                    close(rs);
                }
            } finally {
                close(stmt);
            }
        } catch (SQLException ex) {
            StringBuilder msg = new StringBuilder(
                    "Failed to determine auto increment columns using this query: '" + query
                            + "'.  This is probably not harmful, but should be fixed.  ");
            msg.append("\n");
            msg.append(table.toString());
            if (columnsToCheck != null) {
                for (Column col : columnsToCheck) {
                    msg.append("\n");
                    msg.append(col.toString());
                }
            }
            log.warn(msg.toString(), ex);
        }
    }

    private StringBuilder appendIdentifier(StringBuilder query, String identifier) {
        if (getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn()) {
            query.append(getPlatformInfo().getDelimiterToken());
        }
        query.append(identifier);
        if (getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn()) {
            query.append(getPlatformInfo().getDelimiterToken());
        }
        return query;
    }
    
    /*
     * Allow subclasses to override column delimiters
     */
    protected StringBuilder appendColumn(StringBuilder query, String identifier) {
        return appendIdentifier(query, identifier);
    }

    /*
     * Replaces a specific character sequence in the given text with the
     * character sequence whose escaped version it is.
     * 
     * @param text The text
     * 
     * @param unescaped The unescaped string, e.g. "'"
     * 
     * @param escaped The escaped version, e.g. "''"
     * 
     * @return The resulting text
     */
    protected String unescape(String text, String unescaped, String escaped) {

        // we need special handling if the single quote is escaped via a double
        // single quote
        if (text != null && !"''".equals(text)) {
            if (escaped.equals("''")) {
                if ((text.length() > 2) && text.startsWith("'") && text.endsWith("'")) {
                    text = "'"
                            + StringUtils.replace(text.substring(1, text.length() - 1),
                                    escaped, unescaped) + "'";
                } else {
                    text = StringUtils.replace(text, escaped, unescaped);
                }
            } else {
                text = StringUtils.replace(text, escaped, unescaped);
            }
        }
        return text;
    }
    
    public List<String> getTableTypes() {
        JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform.getSqlTemplate();
        return sqlTemplate.execute(new IConnectionCallback<List<String>>() {
            public List<String> execute(Connection connection) throws SQLException {
                ArrayList<String> types = new ArrayList<String>();
                DatabaseMetaData meta = connection.getMetaData();
                ResultSet rs = null;
                try {
                    rs = meta.getTableTypes();
                    while (rs.next()) {
                        types.add(rs.getString(1));
                    }
                    return types;
                } finally {
                    JdbcSqlTemplate.close(rs);
                }
            }
        });
    }

    public List<String> getCatalogNames() {
        JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform.getSqlTemplate();
        return sqlTemplate.execute(new IConnectionCallback<List<String>>() {
            public List<String> execute(Connection connection) throws SQLException {
                ArrayList<String> catalogs = new ArrayList<String>();
                DatabaseMetaData meta = connection.getMetaData();
                ResultSet rs = null;
                try {
                    rs = meta.getCatalogs();
                    while (rs.next()) {
                        String catalog = rs.getString(1);
                        if (catalog != null) {
                            catalogs.add(catalog);
                        }
                    }
                    return catalogs;
                } finally {
                    JdbcSqlTemplate.close(rs);
                }
            }
        });
    }

    public List<String> getSchemaNames(final String catalog) {
        JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform.getSqlTemplate();
        return sqlTemplate.execute(new IConnectionCallback<List<String>>() {
            public List<String> execute(Connection connection) throws SQLException {
                ArrayList<String> schemas = new ArrayList<String>();
                IConnectionHandler connectionHandler = getConnectionHandler(catalog);
                if (connectionHandler != null) {
                    connectionHandler.before(connection);
                }
                DatabaseMetaData meta = connection.getMetaData();
                ResultSet rs = null;
                try {
                    
                    rs = meta.getSchemas();
                    while (rs.next()) {
                        int columnCount = rs.getMetaData().getColumnCount();
                        String schema = rs.getString(1);
                        String schemaCatalog = null;
                        if (columnCount > 1) {
                            schemaCatalog = rs.getString(2);
                        }
                        if ((StringUtils.isBlank(schemaCatalog) || StringUtils.isBlank(catalog)) && !schemas.contains(schema)) {
                            schemas.add(schema);
                        } else if (StringUtils.isNotBlank(schemaCatalog)
                                && schemaCatalog.equals(catalog)) {
                            schemas.add(schema);
                        }
                    }
                    return schemas;
                } finally {
                    if (connectionHandler != null) {
                        connectionHandler.after(connection);
                    }
                    close(rs);
                }
            }
        });
    }

    protected IConnectionHandler getConnectionHandler(String catalog) {
        return null;
    }
    
    public List<String> getTableNames(final String catalog, final String schema,
            final String[] tableTypes) {
    	JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform.getSqlTemplate();
        List<String> list = sqlTemplate.execute(new IConnectionCallback<List<String>>() {
            public List<String> execute(Connection connection) throws SQLException {
                ArrayList<String> list = new ArrayList<String>();
                DatabaseMetaData meta = connection.getMetaData();
                ResultSet rs = null;
                try {
                    rs = meta.getTables(catalog, schema, getDefaultTablePattern(), tableTypes);
                    while (rs.next()) {
                        String tableName = rs.getString("TABLE_NAME");
                        if (tableName == null) {
                            tableName = rs.getString("NAME");
                        }
                        list.add(tableName);
                    }
                    return list;
                } finally {
                    close(rs);
                }
            }
        });
        return list;
    }
    
    public List<String> getColumnNames(final String catalog, final String schema, final String tableName) {
        JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform.getSqlTemplate();
        return sqlTemplate.execute(new IConnectionCallback<List<String>>() {
            public List<String> execute(Connection connection) throws SQLException {
                ArrayList<String> list = new ArrayList<String>();
                DatabaseMetaData meta = connection.getMetaData();
                ResultSet rs = null;
                try {                    
                    rs = meta.getColumns(catalog, schema, tableName, null);
                    while (rs.next()) {
                        String tableName = rs.getString(getName("COLUMN_NAME"));
                        list.add(tableName);
                    }
                    return list;
                } finally {
                    close(rs);
                }
            }
        });
    }
    
    public List<String> getListOfTriggers() {
    	return new ArrayList<String>();
    }
    
    public Trigger getTriggerFor(Table table, String triggerName) {
    	Trigger trigger = null;
    	List<Trigger> triggers = getTriggers(table.getCatalog(), table.getSchema(), table.getName());
    	for (Trigger t : triggers) {
    		if (t.getName().equals(triggerName)) {
    			trigger = t;
    			break;
    		}
    	}
    	return trigger;
    }

}
