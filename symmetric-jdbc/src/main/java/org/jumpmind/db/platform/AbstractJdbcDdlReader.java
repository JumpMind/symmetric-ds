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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.IndexColumn;
import org.jumpmind.db.model.NonUniqueIndex;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.model.UniqueIndex;
import org.jumpmind.db.sql.IConnectionCallback;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.JdbcUtils;

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
    private IDatabasePlatform platform;

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

        result.add(new MetaDataColumnDescriptor("TABLE_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("TABLE_TYPE", Types.VARCHAR, "UNKNOWN"));
        result.add(new MetaDataColumnDescriptor(getResultSetCatalogName(), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor(getResultSetSchemaName(), Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("REMARKS", Types.VARCHAR));

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
        result.add(new MetaDataColumnDescriptor("COLUMN_DEF", Types.VARCHAR));
        // we're also reading the table name so that a model reader impl can
        // filter manually
        result.add(new MetaDataColumnDescriptor("TABLE_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("COLUMN_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("TYPE_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("DATA_TYPE", Types.INTEGER, new Integer(
                java.sql.Types.OTHER)));
        result.add(new MetaDataColumnDescriptor("NUM_PREC_RADIX", Types.INTEGER, new Integer(10)));
        result.add(new MetaDataColumnDescriptor("DECIMAL_DIGITS", Types.INTEGER, new Integer(0)));
        result.add(new MetaDataColumnDescriptor("COLUMN_SIZE", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("IS_NULLABLE", Types.VARCHAR, "YES"));
        result.add(new MetaDataColumnDescriptor("IS_AUTOINCREMENT", Types.VARCHAR, "YES"));
        result.add(new MetaDataColumnDescriptor("REMARKS", Types.VARCHAR));

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

        result.add(new MetaDataColumnDescriptor("COLUMN_NAME", Types.VARCHAR));
        // we're also reading the table name so that a model reader impl can
        // filter manually
        result.add(new MetaDataColumnDescriptor("TABLE_NAME", Types.VARCHAR));
        // the name of the primary key is currently only interesting to the pk
        // index name resolution
        result.add(new MetaDataColumnDescriptor("PK_NAME", Types.VARCHAR));

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

        result.add(new MetaDataColumnDescriptor("PKTABLE_NAME", Types.VARCHAR));
        // we're also reading the table name so that a model reader impl can
        // filter manually
        result.add(new MetaDataColumnDescriptor("FKTABLE_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("KEY_SEQ", Types.TINYINT, new Short((short) 0)));
        result.add(new MetaDataColumnDescriptor("FK_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("PKCOLUMN_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("FKCOLUMN_NAME", Types.VARCHAR));
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

        result.add(new MetaDataColumnDescriptor("INDEX_NAME", Types.VARCHAR));
        // we're also reading the table name so that a model reader impl can
        // filter manually
        result.add(new MetaDataColumnDescriptor("TABLE_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("NON_UNIQUE", Types.BIT, Boolean.TRUE));
        result.add(new MetaDataColumnDescriptor("ORDINAL_POSITION", Types.TINYINT, new Short(
                (short) 0)));
        result.add(new MetaDataColumnDescriptor("COLUMN_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("TYPE", Types.TINYINT));
        return result;
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
                        db.setName(Table.getQualifiedTablePrefix(catalog, schema));
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

    public Table readTable(final String catalog, final String schema, final String table) {
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
                    tableData = metaData.getTables(getTableNamePattern(table));
                    if (tableData != null && tableData.next()) {
                        ResultSetMetaData meta = tableData.getMetaData();
                        int count = meta.getColumnCount();
                        for (int i = 1 ; i <= count; i++) {
                            System.err.println(meta.getColumnName(i) + "=" + tableData.getObject(i));
                        }
                        Map<String, Object> values = readMetaData(tableData, initColumnsForTable());
                        return readTable(connection, metaData, values);
                    } else {
                        return null;
                    }
                } finally {
                    close(tableData);
                }
            }
        }));

    }

    public Table readTable(final String catalog, final String schema, final String tableName,
            final String sql) {
        JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform.getSqlTemplate();
        return postprocessTableFromDatabase(sqlTemplate.execute(new IConnectionCallback<Table>() {
            public Table execute(Connection connection) throws SQLException {
                Statement st = connection.createStatement();
                ResultSet rs = st.executeQuery(sql);
                try {
                    st = connection.createStatement();
                    rs = st.executeQuery(sql);
                    ResultSetMetaData rsm = rs.getMetaData();
                    Table table = new Table();
                    table.setCatalog(catalog);
                    table.setSchema(schema);
                    table.setName(tableName);
                    for (int i = 1; i <= rsm.getColumnCount(); i++) {
                        Column column = new Column(rsm.getColumnName(i));
                        column.setMappedTypeCode(rsm.getColumnType(i));
                        column.setJdbcTypeCode(rsm.getColumnType(i));
                        column.setJdbcTypeName(column.getMappedType());
                        column.setRequired(rsm.isNullable(i) == 0);
                        column.setScale(rsm.getScale(i));
                        column.setPrecisionRadix(rsm.getPrecision(i));
                        column.setAutoIncrement(rsm.isAutoIncrement(i));
                        table.addColumn(column);
                    }
                    return table;
                } finally {
                    JdbcUtils.closeResultSet(rs);
                    JdbcUtils.closeStatement(st);
                }
            }
        }));
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
        String tableName = (String) values.get("TABLE_NAME");
        Table table = null;

        if ((tableName != null) && (tableName.length() > 0)) {
            String type = (String) values.get("TABLE_TYPE");
            String[] unsupportedTableTypes = getUnsupportedTableTypes();
            for (String unsupportedTableType : unsupportedTableTypes) {
                if (StringUtils.isNotBlank(type) && type.equals(unsupportedTableType)) {
                    return null;
                }
            }
            
            table = new Table();
            table.setName(tableName);
            table.setType(type);
            
            String catalog = (String) values.get(getResultSetCatalogName());
            table.setCatalog(catalog);
            metaData.setCatalog(catalog);
            
            String schema = (String) values.get(getResultSetSchemaName());
            table.setSchema(schema);
            metaData.setSchemaPattern(schema);
            
            table.setDescription((String) values.get("REMARKS"));

            table.addColumns(readColumns(metaData, tableName));
            table.addForeignKeys(readForeignKeys(connection, metaData, tableName));
            table.addIndices(readIndices(connection, metaData, tableName));

            Collection<String> primaryKeys = readPrimaryKeyNames(metaData, tableName);

            for (Iterator<String> it = primaryKeys.iterator(); it.hasNext();) {
                table.findColumn(it.next(), true).setPrimaryKey(true);
            }

            if (getPlatformInfo().isSystemIndicesReturned()) {
                removeSystemIndices(connection, metaData, table);
            }
        }
        return table;
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
        boolean mustBeUnique = !getPlatformInfo().isSystemForeignKeyIndicesAlwaysNonUnique();

        for (int columnIdx = 0; columnIdx < fk.getReferenceCount(); columnIdx++) {
            String name = fk.getReference(columnIdx).getLocalColumnName();
            Column localColumn = table.findColumn(name, getPlatform().getDdlBuilder()
                    .isDelimitedIdentifierModeOn());

            if (mustBeUnique && !localColumn.isPrimaryKey()) {
                mustBeUnique = false;
            }
            columnNames.add(name);
        }

        for (int indexIdx = 0; indexIdx < table.getIndexCount();) {
            IIndex index = table.getIndex(indexIdx);
            if ((mustBeUnique == index.isUnique()) && matches(index, columnNames)
                    && isInternalForeignKeyIndex(connection, metaData, table, fk, index)) {
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
        if (index.getColumnCount() != columnsToSearchFor.size()) {
            return false;
        }
        for (int columnIdx = 0; columnIdx < index.getColumnCount(); columnIdx++) {
            if (!columnsToSearchFor.get(columnIdx).equals(index.getColumn(columnIdx).getName())) {
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
            columnData = metaData.getColumns(getTableNamePattern(tableName),
                    getDefaultColumnPattern());

            List<Column> columns = new ArrayList<Column>();

            while (columnData.next()) {
                Map<String, Object> values = readMetaData(columnData, getColumnsForColumn());

                columns.add(readColumn(metaData, values));
            }
            return columns;
        } finally {
            close(columnData);
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
        column.setName((String) values.get("COLUMN_NAME"));
        String defaultValue = (String) values.get("COLUMN_DEF");
        if (defaultValue != null) {
            column.setDefaultValue(defaultValue.trim());
        }

        Integer mappedType = mapUnknownJdbcTypeForColumn(values);
        if (mappedType != null) {
            column.setMappedTypeCode(mappedType);
        } else {
            column.setMappedTypeCode((Integer) values.get("DATA_TYPE"));
        }

        column.setJdbcTypeCode((Integer) values.get("DATA_TYPE"));
        column.setJdbcTypeName((String) values.get("TYPE_NAME"));
        column.setPrecisionRadix(((Integer) values.get("NUM_PREC_RADIX")).intValue());

        String size = (String) values.get("COLUMN_SIZE");
        int scale = ((Integer) values.get("DECIMAL_DIGITS")).intValue();

        if (size == null) {
            size = (String) _defaultSizes.get(new Integer(column.getMappedTypeCode()));
        }
        // we're setting the size after the precision and radix in case
        // the database prefers to return them in the size value
        column.setSize(size);
        if (scale != 0) {
            // if there is a scale value, set it after the size (which probably
            // did not contain
            // a scale specification)
            column.setScale(scale);
        }
        column.setRequired("NO".equalsIgnoreCase(((String) values.get("IS_NULLABLE")).trim()));
        column.setDescription((String) values.get("REMARKS"));

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
            pkData = metaData.getPrimaryKeys(getTableNamePattern(tableName));
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
        return (String) values.get("COLUMN_NAME");
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
        ResultSet fkData = null;

        try {
            fkData = metaData.getForeignKeys(tableName);

            while (fkData.next()) {
                Map<String, Object> values = readMetaData(fkData, getColumnsForFK());

                readForeignKey(metaData, values, fks);
            }
        } finally {
            close(fkData);
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
        String fkName = (String) values.get("FK_NAME");
        ForeignKey fk = (ForeignKey) knownFks.get(fkName);

        if (fk == null) {
            fk = new ForeignKey(fkName);
            fk.setForeignTableName((String) values.get("PKTABLE_NAME"));
            knownFks.put(fkName, fk);
        }

        Reference ref = new Reference();

        ref.setForeignColumnName((String) values.get("PKCOLUMN_NAME"));
        ref.setLocalColumnName((String) values.get("FKCOLUMN_NAME"));
        if (values.containsKey("KEY_SEQ")) {
            ref.setSequenceValue(((Short) values.get("KEY_SEQ")).intValue());
        }
        fk.addReference(ref);
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
        ResultSet indexData = null;

        try {
            indexData = metaData.getIndices(getTableNamePattern(tableName), false, false);

            while (indexData.next()) {
                Map<String, Object> values = readMetaData(indexData, getColumnsForIndex());

                readIndex(metaData, values, indices);
            }
        } finally {
            close(indexData);
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
        Short indexType = (Short) values.get("TYPE");

        // we're ignoring statistic indices
        if ((indexType != null) && (indexType.shortValue() == DatabaseMetaData.tableIndexStatistic)) {
            return;
        }

        String indexName = (String) values.get("INDEX_NAME");

        if (indexName != null) {
            IIndex index = (IIndex) knownIndices.get(indexName);

            if (index == null) {
                if (((Boolean) values.get("NON_UNIQUE")).booleanValue()) {
                    index = new NonUniqueIndex();
                } else {
                    index = new UniqueIndex();
                }

                index.setName(indexName);
                knownIndices.put(indexName, index);
            }

            IndexColumn indexColumn = new IndexColumn();

            String columnName = (String) values.get("COLUMN_NAME");
            if (columnName.startsWith("\"") && columnName.endsWith("\"")) {
                columnName = columnName.substring(1, columnName.length()-1);
            }
            indexColumn.setName(columnName);
            if (values.containsKey("ORDINAL_POSITION")) {
                indexColumn.setOrdinalPosition(((Short) values.get("ORDINAL_POSITION")).intValue());
            }
            index.addColumn(indexColumn);
        }
    }

    /*
     * Reads the indicated columns from the result set.
     * 
     * @param resultSet The result set
     * 
     * @param columnDescriptors The dscriptors of the columns to read
     * 
     * @return The read values keyed by the column name
     */
    protected Map<String, Object> readMetaData(ResultSet resultSet,
            List<MetaDataColumnDescriptor> columnDescriptors) throws SQLException {
        HashMap<String, Object> values = new HashMap<String, Object>();
        for (Iterator<MetaDataColumnDescriptor> it = columnDescriptors.iterator(); it.hasNext();) {
            MetaDataColumnDescriptor descriptor = it.next();

            values.put(descriptor.getName(), descriptor.readColumn(resultSet));
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
    public void determineAutoIncrementFromResultSetMetaData(Connection conn, Table table,
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
                appendIdentifier(query, columnsToCheck[idx].getName());
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
                ResultSet rs = stmt.executeQuery(query.toString());
                ResultSetMetaData rsMetaData = rs.getMetaData();

                for (int idx = 0; idx < columnsToCheck.length; idx++) {
                    if (log.isDebugEnabled()) {
                        log.debug(columnsToCheck[idx] + " is auto increment? " + rsMetaData.isAutoIncrement(idx + 1));
                    }
                    if (rsMetaData.isAutoIncrement(idx + 1)) {
                        columnsToCheck[idx].setAutoIncrement(true);
                    }
                }
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
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

    public StringBuilder appendIdentifier(StringBuilder query, String identifier) {
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
        String result = text;

        // we need special handling if the single quote is escaped via a double
        // single quote
        if (result != null) {
            if (escaped.equals("''")) {
                if ((result.length() > 2) && result.startsWith("'") && result.endsWith("'")) {
                    result = "'"
                            + StringUtils.replace(result.substring(1, result.length() - 1),
                                    escaped, unescaped) + "'";
                } else {
                    result = StringUtils.replace(result, escaped, unescaped);
                }
            } else {
                result = StringUtils.replace(result, escaped, unescaped);
            }
        }
        return result;
    }

    /*
     * Tries to find the schema to which the given table belongs.
     * 
     * @param connection The database connection
     * 
     * @param schemaPattern The schema pattern to limit the schemas to search in
     * 
     * @param table The table to search for
     * 
     * @return The schema name or <code>null</code> if the schema of the table
     * could not be found
     * 
     * @deprecated Will be removed once full schema support is in place
     */
    public String determineSchemaOf(Connection connection, String schemaPattern, Table table)
            throws SQLException {
        ResultSet tableData = null;
        ResultSet columnData = null;

        try {
            DatabaseMetaDataWrapper metaData = new DatabaseMetaDataWrapper();

            metaData.setMetaData(connection.getMetaData());
            metaData.setCatalog(getDefaultCatalogPattern());
            metaData.setSchemaPattern(schemaPattern == null ? getDefaultSchemaPattern()
                    : schemaPattern);
            metaData.setTableTypes(getDefaultTableTypes());

            String tablePattern = table.getName();

            if (getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn()) {
                tablePattern = tablePattern.toUpperCase();
            }

            tableData = metaData.getTables(tablePattern);

            boolean found = false;
            String schema = null;

            while (!found && tableData.next()) {
                Map<String, Object> values = readMetaData(tableData, getColumnsForTable());
                String tableName = (String) values.get(getResultSetCatalogName());

                if ((tableName != null) && (tableName.length() > 0)) {
                    schema = (String) values.get(getResultSetSchemaName());
                    columnData = metaData.getColumns(tableName, getDefaultColumnPattern());
                    found = true;

                    while (found && columnData.next()) {
                        values = readMetaData(columnData, getColumnsForColumn());

                        if (table.findColumn((String) values.get("COLUMN_NAME"), getPlatform()
                                .getDdlBuilder().isDelimitedIdentifierModeOn()) == null) {
                            found = false;
                        }
                    }
                    columnData.close();
                    columnData = null;
                }
            }
            return found ? schema : null;
        } finally {
            close(columnData);
            close(tableData);
        }
    }
}
