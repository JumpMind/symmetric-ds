package org.jumpmind.symmetric.jdbc.db;

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

import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.common.LogLevel;
import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.db.DbDialectInfo;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.db.SqlException;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Database;
import org.jumpmind.symmetric.core.model.ForeignKey;
import org.jumpmind.symmetric.core.model.Index;
import org.jumpmind.symmetric.core.model.IndexColumn;
import org.jumpmind.symmetric.core.model.NonUniqueIndex;
import org.jumpmind.symmetric.core.model.Reference;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.UniqueIndex;

/**
 * An utility class to create a Database model from a live database.
 */
public class JdbcTableReader {

    /** The Log to which logging calls will be made. */
    protected final Log log = LogFactory.getLog(getClass());

    /** The descriptors for the relevant columns in the table meta data. */
    private final List<MetaDataColumnDescriptor> columnsForTable;

    /** The descriptors for the relevant columns in the table column meta data. */
    private final List<MetaDataColumnDescriptor> columnsForColumn;

    /** The descriptors for the relevant columns in the primary key meta data. */
    private final List<MetaDataColumnDescriptor> columnsForPK;

    /** The descriptors for the relevant columns in the foreign key meta data. */
    private final List<MetaDataColumnDescriptor> columnsForFK;

    /** The descriptors for the relevant columns in the index meta data. */
    private final List<MetaDataColumnDescriptor> columnsForIndex;

    /** The platform that this model reader belongs to. */
    private IJdbcDbDialect dbDialect;
    /**
     * Contains default column sizes (minimum sizes that a JDBC-compliant db
     * must support).
     */
    private HashMap<Integer, String> defaultSizes = new HashMap<Integer, String>();

    /** The default database catalog to read. */
    private String defaultCatalogPattern = "%";

    /** The default database schema(s) to read. */
    private String defaultSchemaPattern = "%";

    /** The default pattern for reading all tables. */
    private String defaultTablePattern = "%";

    /** The default pattern for reading all columns. */
    private String defaultColumnPattern;

    /** The table types to recognize per default. */
    private String[] defaultTableTypes = { "TABLE" };

    protected JdbcSqlTemplate connection;

    /**
     * Creates a new model reader instance.
     * 
     * @param platform
     *            The platform this builder belongs to
     */
    public JdbcTableReader(IJdbcDbDialect platform) {
        this.dbDialect = platform;
        this.connection = new JdbcSqlTemplate(platform);

        defaultSizes.put(new Integer(Types.CHAR), "254");
        defaultSizes.put(new Integer(Types.VARCHAR), "254");
        defaultSizes.put(new Integer(Types.LONGVARCHAR), "254");
        defaultSizes.put(new Integer(Types.BINARY), "254");
        defaultSizes.put(new Integer(Types.VARBINARY), "254");
        defaultSizes.put(new Integer(Types.LONGVARBINARY), "254");
        defaultSizes.put(new Integer(Types.INTEGER), "32");
        defaultSizes.put(new Integer(Types.BIGINT), "64");
        defaultSizes.put(new Integer(Types.REAL), "7,0");
        defaultSizes.put(new Integer(Types.FLOAT), "15,0");
        defaultSizes.put(new Integer(Types.DOUBLE), "15,0");
        defaultSizes.put(new Integer(Types.DECIMAL), "15,15");
        defaultSizes.put(new Integer(Types.NUMERIC), "15,15");

        columnsForTable = initColumnsForTable();
        columnsForColumn = initColumnsForColumn();
        columnsForPK = initColumnsForPK();
        columnsForFK = initColumnsForFK();
        columnsForIndex = initColumnsForIndex();
    }

    /**
     * Returns the platform that this model reader belongs to.
     * 
     * @return The platform
     */
    public IDbDialect getDbDialect() {
        return dbDialect;
    }

    /**
     * Returns the platform specific settings.
     * 
     * @return The platform settings
     */
    public DbDialectInfo getDbDialectInfo() {
        return dbDialect.getDbDialectInfo();
    }

    /**
     * Returns descriptors for the columns that shall be read from the result
     * set when reading the meta data for a table. Note that the columns are
     * read in the order defined by this list.<br/>
     * Redefine this method if you want more columns or a different order.
     * 
     * @return The descriptors for the result set columns
     */
    protected List<MetaDataColumnDescriptor> initColumnsForTable() {
        List<MetaDataColumnDescriptor> result = new ArrayList<MetaDataColumnDescriptor>();

        result.add(new MetaDataColumnDescriptor("TABLE_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("TABLE_TYPE", Types.VARCHAR, "UNKNOWN"));
        result.add(new MetaDataColumnDescriptor("TABLE_CAT", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("TABLE_SCHEM", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("REMARKS", Types.VARCHAR));

        return result;
    }

    /**
     * Returns descriptors for the columns that shall be read from the result
     * set when reading the meta data for table columns. Note that the columns
     * are read in the order defined by this list.<br/>
     * Redefine this method if you want more columns or a different order.
     * 
     * @return The map column name -> descriptor for the result set columns
     */
    protected List<MetaDataColumnDescriptor> initColumnsForColumn() {
        List<MetaDataColumnDescriptor> result = new ArrayList<MetaDataColumnDescriptor>();

        // As suggested by Alexandre Borgoltz, we're reading the COLUMNDEF
        // first because Oracle
        // has problems otherwise (it seemingly requires a LONG column to be the
        // first to be read)
        // See also DDLUTILS-29
        result.add(new MetaDataColumnDescriptor("COLUMN_DEF", Types.VARCHAR));
        // we're also reading the table name so that a model reader impl can
        // filter manually
        result.add(new MetaDataColumnDescriptor("TABLE_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("COLUMN_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("TYPE_NAME", 12));
        result.add(new MetaDataColumnDescriptor("DATA_TYPE", Types.INTEGER, new Integer(
                java.sql.Types.OTHER)));
        result.add(new MetaDataColumnDescriptor("NUM_PREC_RADIX", Types.INTEGER, new Integer(10)));
        result.add(new MetaDataColumnDescriptor("DECIMAL_DIGITS", Types.INTEGER, new Integer(0)));
        result.add(new MetaDataColumnDescriptor("COLUMN_SIZE", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("IS_NULLABLE", Types.VARCHAR, "YES"));
        result.add(new MetaDataColumnDescriptor("REMARKS", Types.VARCHAR));
        
        return result;
    }

    /**
     * Returns descriptors for the columns that shall be read from the result
     * set when reading the meta data for primary keys. Note that the columns
     * are read in the order defined by this list.<br/>
     * Redefine this method if you want more columns or a different order.
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

    /**
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

    /**
     * Returns descriptors for the columns that shall be read from the result
     * set when reading the meta data for indices. Note that the columns are
     * read in the order defined by this list.<br/>
     * Redefine this method if you want more columns or a different order.
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

    /**
     * Returns the catalog(s) in the database to read per default.
     * 
     * @return The default catalog(s)
     */
    public String getDefaultCatalogPattern() {
        return defaultCatalogPattern;
    }

    /**
     * Sets the catalog(s) in the database to read per default.
     * 
     * @param catalogPattern
     *            The catalog(s)
     */
    public void setDefaultCatalogPattern(String catalogPattern) {
        defaultCatalogPattern = catalogPattern;
    }

    /**
     * Returns the schema(s) in the database to read per default.
     * 
     * @return The default schema(s)
     */
    public String getDefaultSchemaPattern() {
        return defaultSchemaPattern;
    }

    /**
     * Sets the schema(s) in the database to read per default.
     * 
     * @param schemaPattern
     *            The schema(s)
     */
    public void setDefaultSchemaPattern(String schemaPattern) {
        defaultSchemaPattern = schemaPattern;
    }

    /**
     * Returns the default pattern to read the relevant tables from the
     * database.
     * 
     * @return The table pattern
     */
    public String getDefaultTablePattern() {
        return defaultTablePattern;
    }

    /**
     * Sets the default pattern to read the relevant tables from the database.
     * 
     * @param tablePattern
     *            The table pattern
     */
    public void setDefaultTablePattern(String tablePattern) {
        defaultTablePattern = tablePattern;
    }

    /**
     * Returns the default pattern to read the relevant columns from the
     * database.
     * 
     * @return The column pattern
     */
    public String getDefaultColumnPattern() {
        return defaultColumnPattern;
    }

    /**
     * Sets the default pattern to read the relevant columns from the database.
     * 
     * @param columnPattern
     *            The column pattern
     */
    public void setDefaultColumnPattern(String columnPattern) {
        defaultColumnPattern = columnPattern;
    }

    /**
     * Returns the table types to recognize per default.
     * 
     * @return The default table types
     */
    public String[] getDefaultTableTypes() {
        return defaultTableTypes;
    }

    /**
     * Sets the table types to recognize per default. Typical types are "TABLE",
     * "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS",
     * "SYNONYM".
     * 
     * @param types
     *            The table types
     */
    public void setDefaultTableTypes(String[] types) {
        defaultTableTypes = types;
    }

    /**
     * Returns the descriptors for the columns to be read from the table meta
     * data result set.
     * 
     * @return The column descriptors
     */
    protected List<MetaDataColumnDescriptor> getColumnsForTable() {
        return columnsForTable;
    }

    /**
     * Returns the descriptors for the columns to be read from the column meta
     * data result set.
     * 
     * @return The column descriptors
     */
    protected List<MetaDataColumnDescriptor> getColumnsForColumn() {
        return columnsForColumn;
    }

    /**
     * Returns the descriptors for the columns to be read from the primary key
     * meta data result set.
     * 
     * @return The column descriptors
     */
    protected List<MetaDataColumnDescriptor> getColumnsForPK() {
        return columnsForPK;
    }

    /**
     * Returns the descriptors for the columns to be read from the foreign key
     * meta data result set.
     * 
     * @return The column descriptors
     */
    protected List<MetaDataColumnDescriptor> getColumnsForFK() {
        return columnsForFK;
    }

    /**
     * Returns the descriptors for the columns to be read from the index meta
     * data result set.
     * 
     * @return The column descriptors
     */
    protected List<MetaDataColumnDescriptor> getColumnsForIndex() {
        return columnsForIndex;
    }

    /**
     * Reads the database model
     * 
     * @param catalog
     *            The catalog to acess in the database; use <code>null</code>
     *            for the default value
     * @param schema
     *            The schema to acess in the database; use <code>null</code> for
     *            the default value
     * @param tableTypes
     *            The table types to process; use <code>null</code> or an empty
     *            list for the default ones
     * @return The database model
     */
    public Database getDatabase(final String catalog, final String schema, final String[] tableTypes) {
        return connection.execute(new IConnectionCallback<Database>() {
            public Database execute(Connection con) throws SQLException {
                Database db = new Database();
                try {
                    db.setName(con.getCatalog());
                } catch (Exception ex) {
                    log.log(LogLevel.INFO, ex, "Cannot determine the catalog name from connection.");
                }
                db.addTables(readTables(con, catalog, schema, tableTypes));

                // Note that we do this here instead of in readTable since
                // platforms may redefine the readTable method whereas it is
                // highly unlikely that this method gets redefined
                if (getDbDialect().getDbDialectInfo().isForeignKeysSorted()) {
                    sortForeignKeys(db);
                }
                db.initialize();
                return db;
            }
        });

    }

    /**
     * Reads the tables from the database metadata.
     * 
     * @param catalog
     *            The catalog to acess in the database; use <code>null</code>
     *            for the default value
     * @param schemaPattern
     *            The schema(s) to acess in the database; use <code>null</code>
     *            for the default value
     * @param tableTypes
     *            The table types to process; use <code>null</code> or an empty
     *            list for the default ones
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
                Map<String, Object> values = readColumns(tableData, getColumnsForTable());
                Table table = readTable(connection, metaData, values);

                if (table != null) {
                    tables.add(table);
                }
            }

            final Collator collator = Collator.getInstance();

            Collections.sort(tables, new Comparator<Table>() {
                public int compare(Table obj1, Table obj2) {
                    return collator.compare(obj1.getTableName().toUpperCase(), obj2.getTableName()
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

    protected String getDialectTableName(String catalogName, String schemaName, String tblName) {
        return tblName;
    }

    protected String getTableNamePattern(String tableName) {
        return tableName;
    }

    /**
     * Returns a new {@link Table} object.
     */
    public Table readTable(String catalogName, String schemaName, String tableName,
            boolean caseSensitive, boolean makeAllColumnsPKsIfNoneFound) {
        Table table = readTableCaseSensitive(catalogName, schemaName, tableName,
                makeAllColumnsPKsIfNoneFound);

        if (table == null && !caseSensitive) {
            table = readTableCaseSensitive(StringUtils.upperCase(catalogName),
                    StringUtils.upperCase(schemaName), StringUtils.upperCase(tableName),
                    makeAllColumnsPKsIfNoneFound);
            if (table == null) {
                table = readTableCaseSensitive(StringUtils.lowerCase(catalogName),
                        StringUtils.lowerCase(schemaName), StringUtils.lowerCase(tableName),
                        makeAllColumnsPKsIfNoneFound);
                if (table == null) {
                    table = readTableCaseSensitive(catalogName, schemaName,
                            StringUtils.upperCase(tableName), makeAllColumnsPKsIfNoneFound);
                    if (table == null) {
                        table = readTableCaseSensitive(catalogName, schemaName,
                                StringUtils.lowerCase(tableName), makeAllColumnsPKsIfNoneFound);
                        if (table == null) {
                            table = readTableCaseSensitive(catalogName, schemaName,
                                    getDialectTableName(catalogName, schemaName, tableName),
                                    makeAllColumnsPKsIfNoneFound);
                        }
                    }
                }
            }
        }
        return table;
    }

    protected Table readTableCaseSensitive(String catalogName, String schemaName,
            final String tableName, final boolean makeAllColumnsPKsIfNoneFound) {
        Table table = null;
        try {
            // If we don't provide a default schema or catalog, then on some
            // databases multiple results will be found in the metadata from
            // multiple schemas/catalogs
            final String schema = StringUtils.isBlank(schemaName) ? dbDialect.getDefaultSchema()
                    : schemaName;
            final String catalog = StringUtils.isBlank(catalogName) ? dbDialect.getDefaultCatalog()
                    : catalogName;
            table = dbDialect.getJdbcSqlConnection().execute(new IConnectionCallback<Table>() {
                public Table execute(Connection c) throws SQLException {
                    Table table = null;
                    DatabaseMetaDataWrapper metaData = new DatabaseMetaDataWrapper();
                    metaData.setMetaData(c.getMetaData());
                    metaData.setCatalog(catalog);
                    metaData.setSchemaPattern(schema);
                    metaData.setTableTypes(getDefaultTableTypes());
                    ResultSet tableData = null;
                    try {
                        tableData = metaData.getTables(getTableNamePattern(tableName));
                        while (tableData != null && tableData.next()) {
                            Map<String, Object> values = readColumns(tableData,
                                    initColumnsForTable());
                            table = readTable(c, metaData, values);
                        }
                    } finally {
                        JdbcSqlTemplate.close(tableData);
                    }

                    if (makeAllColumnsPKsIfNoneFound) {
                        makeAllColumnsPrimaryKeysIfNoPrimaryKeysFound(table);
                    }

                    return table;
                }
            });
        } catch (SqlException ex) {
            log.log(LogLevel.WARN, ex);
        }

        return table;
    }

    /**
     * Treat tables with no primary keys as a table with all primary keys.
     */
    protected void makeAllColumnsPrimaryKeysIfNoPrimaryKeysFound(Table table) {
        if (table != null && table.getPrimaryKeyColumns() != null
                && table.getPrimaryKeyColumns().size() == 0) {
            Column[] allCoumns = table.getColumns();
            for (Column column : allCoumns) {
                if (!column.isOfBinaryType()) {
                    column.setPrimaryKey(true);
                }
            }
        }
    }

    /**
     * Reads the next table from the meta data.
     * 
     * @param metaData
     *            The database meta data
     * @param values
     *            The table metadata values as defined by
     *            {@link #getColumnsForTable()}
     * @return The table or <code>null</code> if the result set row did not
     *         contain a valid table
     */
    protected Table readTable(Connection c, DatabaseMetaDataWrapper metaData,
            Map<String, Object> values) throws SQLException {
        String tableName = (String) values.get("TABLE_NAME");
        Table table = null;

        if ((tableName != null) && (tableName.length() > 0)) {
            table = new Table();

            table.setTableName(tableName);
            table.setType((String) values.get("TABLE_TYPE"));
            table.setCatalogName((String) values.get("TABLE_CAT"));
            table.setSchemaName((String) values.get("TABLE_SCHEM"));
            table.setDescription((String) values.get("REMARKS"));

            table.addColumns(readColumns(metaData, tableName));
            table.addForeignKeys(readForeignKeys(metaData, tableName));
            table.addIndices(readIndices(c, metaData, tableName));

            Collection<String> primaryKeys = readPrimaryKeyNames(metaData, tableName);

            for (Iterator<String> it = primaryKeys.iterator(); it.hasNext();) {
                table.findColumn(it.next(), true).setPrimaryKey(true);
            }

            if (getDbDialectInfo().isSystemIndicesReturned()) {
                removeSystemIndices(metaData, table);
            }
        }
        return table;
    }

    /**
     * Removes system indices (generated by the database for primary and foreign
     * keys) from the table.
     * 
     * @param metaData
     *            The database meta data
     * @param table
     *            The table
     */
    protected void removeSystemIndices(DatabaseMetaDataWrapper metaData, Table table)
            throws SQLException {
        removeInternalPrimaryKeyIndex(metaData, table);

        for (int fkIdx = 0; fkIdx < table.getForeignKeyCount(); fkIdx++) {
            removeInternalForeignKeyIndex(metaData, table, table.getForeignKey(fkIdx));
        }
    }

    /**
     * Tries to remove the internal index for the table's primary key.
     * 
     * @param metaData
     *            The database meta data
     * @param table
     *            The table
     */
    protected void removeInternalPrimaryKeyIndex(DatabaseMetaDataWrapper metaData, Table table)
            throws SQLException {
        List<Column> pks = table.getPrimaryKeyColumns();
        List<String> columnNames = new ArrayList<String>();

        for (Column col : pks) {
            columnNames.add(col.getName());
        }

        for (int indexIdx = 0; indexIdx < table.getIndexCount();) {
            Index index = table.getIndex(indexIdx);

            if (index.isUnique() && matches(index, columnNames)
                    && isInternalPrimaryKeyIndex(metaData, table, index)) {
                table.removeIndex(indexIdx);
            } else {
                indexIdx++;
            }
        }
    }

    /**
     * Tries to remove the internal index for the given foreign key.
     * 
     * @param metaData
     *            The database meta data
     * @param table
     *            The table where the table is defined
     * @param fk
     *            The foreign key
     */
    protected void removeInternalForeignKeyIndex(DatabaseMetaDataWrapper metaData, Table table,
            ForeignKey fk) throws SQLException {
        List<String> columnNames = new ArrayList<String>();
        boolean mustBeUnique = !getDbDialectInfo().isSystemForeignKeyIndicesAlwaysNonUnique();

        for (int columnIdx = 0; columnIdx < fk.getReferenceCount(); columnIdx++) {
            String name = fk.getReference(columnIdx).getLocalColumnName();
            Column localColumn = table.findColumn(name, getDbDialect().getDbDialectInfo()
                    .isDelimitedIdentifierModeOn());

            if (mustBeUnique && !localColumn.isPrimaryKey()) {
                mustBeUnique = false;
            }
            columnNames.add(name);
        }

        for (int indexIdx = 0; indexIdx < table.getIndexCount();) {
            Index index = table.getIndex(indexIdx);

            if ((mustBeUnique == index.isUnique()) && matches(index, columnNames)
                    && isInternalForeignKeyIndex(metaData, table, fk, index)) {
                fk.setAutoIndexPresent(true);
                table.removeIndex(indexIdx);
            } else {
                indexIdx++;
            }
        }
    }

    /**
     * Checks whether the given index matches the column list.
     * 
     * @param index
     *            The index
     * @param columnsToSearchFor
     *            The names of the columns that the index should be for
     * @return <code>true</code> if the index matches the columns
     */
    protected boolean matches(Index index, List<String> columnsToSearchFor) {
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

    /**
     * Tries to determine whether the index is the internal database-generated
     * index for the given table's primary key. Note that only unique indices
     * with the correct columns are fed to this method. Redefine this method for
     * specific platforms if there are better ways to determine internal
     * indices.
     * 
     * @param metaData
     *            The database meta data
     * @param table
     *            The table owning the index
     * @param index
     *            The index to check
     * @return <code>true</code> if the index seems to be an internal primary
     *         key one
     */
    protected boolean isInternalPrimaryKeyIndex(DatabaseMetaDataWrapper metaData, Table table,
            Index index) throws SQLException {
        return false;
    }

    /**
     * Tries to determine whether the index is the internal database-generated
     * index for the given foreign key. Note that only non-unique indices with
     * the correct columns are fed to this method. Redefine this method for
     * specific platforms if there are better ways to determine internal
     * indices.
     * 
     * @param metaData
     *            The database meta data
     * @param table
     *            The table owning the index and foreign key
     * @param fk
     *            The foreign key
     * @param index
     *            The index to check
     * @return <code>true</code> if the index seems to be an internal primary
     *         key one
     */
    protected boolean isInternalForeignKeyIndex(DatabaseMetaDataWrapper metaData, Table table,
            ForeignKey fk, Index index) throws SQLException {
        return false;
    }

    /**
     * Reads the column definitions for the indicated table.
     * 
     * @param metaData
     *            The database meta data
     * @param tableName
     *            The name of the table
     * @return The columns
     */
    protected Collection<Column> readColumns(DatabaseMetaDataWrapper metaData, String tableName)
            throws SQLException {
        ResultSet columnData = null;

        try {
            columnData = metaData.getColumns(tableName, getDefaultColumnPattern());

            List<Column> columns = new ArrayList<Column>();

            while (columnData.next()) {
                Map<String, Object> values = readColumns(columnData, getColumnsForColumn());

                columns.add(readColumn(metaData, values));
            }
            return columns;
        } finally {
            if (columnData != null) {
                columnData.close();
            }
        }
    }

    /**
     * Extracts a column definition from the result set.
     * 
     * @param metaData
     *            The database meta data
     * @param values
     *            The column meta data values as defined by
     *            {@link #getColumnsForColumn()}
     * @return The column
     */
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        Column column = new Column();

        column.setName((String) values.get("COLUMN_NAME"));
        column.setDefaultValue((String) values.get("COLUMN_DEF"));
        column.setTypeCode(mapJdbcTypeForColumn(((Integer) values.get("DATA_TYPE")).intValue(), values));
        column.setPrecisionRadix(((Integer) values.get("NUM_PREC_RADIX")).intValue());

        String size = (String) values.get("COLUMN_SIZE");
        int scale = ((Integer) values.get("DECIMAL_DIGITS")).intValue();

        if (size == null) {
            size = (String) defaultSizes.get(new Integer(column.getTypeCode()));
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
    
    protected int mapJdbcTypeForColumn(int typeCode, Map<String,Object> values) {
        return typeCode;
    }

    /**
     * Retrieves the names of the columns that make up the primary key for a
     * given table.
     * 
     * @param metaData
     *            The database meta data
     * @param tableName
     *            The name of the table from which to retrieve PK information
     * @return The primary key column names
     */
    protected Collection<String> readPrimaryKeyNames(DatabaseMetaDataWrapper metaData,
            String tableName) throws SQLException {
        List<String> pks = new ArrayList<String>();
        ResultSet pkData = null;

        try {
            pkData = metaData.getPrimaryKeys(tableName);
            while (pkData.next()) {
                Map<String, Object> values = readColumns(pkData, getColumnsForPK());

                pks.add(readPrimaryKeyName(metaData, values));
            }
        } finally {
            if (pkData != null) {
                pkData.close();
            }
        }
        return pks;
    }

    /**
     * Extracts a primary key name from the result set.
     * 
     * @param metaData
     *            The database meta data
     * @param values
     *            The primary key meta data values as defined by
     *            {@link #getColumnsForPK()}
     * @return The primary key name
     */
    protected String readPrimaryKeyName(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        return (String) values.get("COLUMN_NAME");
    }

    /**
     * Retrieves the foreign keys of the indicated table.
     * 
     * @param metaData
     *            The database meta data
     * @param tableName
     *            The name of the table from which to retrieve FK information
     * @return The foreign keys
     */
    protected Collection<ForeignKey> readForeignKeys(DatabaseMetaDataWrapper metaData,
            String tableName) throws SQLException {
        Map<String, ForeignKey> fks = new LinkedHashMap<String, ForeignKey>();
        ResultSet fkData = null;

        try {
            fkData = metaData.getForeignKeys(tableName);

            while (fkData.next()) {
                Map<String, Object> values = readColumns(fkData, getColumnsForFK());

                readForeignKey(metaData, values, fks);
            }
        } finally {
            if (fkData != null) {
                fkData.close();
            }
        }
        return fks.values();
    }

    /**
     * Reads the next foreign key spec from the result set.
     * 
     * @param metaData
     *            The database meta data
     * @param values
     *            The foreign key meta data as defined by
     *            {@link #getColumnsForFK()}
     * @param knownFks
     *            The already read foreign keys for the current table
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

    /**
     * Determines the indices for the indicated table.
     * 
     * @param metaData
     *            The database meta data
     * @param tableName
     *            The name of the table
     * @return The list of indices
     */
    protected Collection<Index> readIndices(Connection c, DatabaseMetaDataWrapper metaData,
            String tableName) throws SQLException {
        Map<String, Index> indices = new LinkedHashMap<String, Index>();
        ResultSet indexData = null;

        try {
            indexData = metaData.getIndices(tableName, false, false);

            while (indexData.next()) {
                Map<String, Object> values = readColumns(indexData, getColumnsForIndex());

                readIndex(metaData, values, indices);
            }
        } finally {
            if (indexData != null) {
                indexData.close();
            }
        }
        return indices.values();
    }

    /**
     * Reads the next index spec from the result set.
     * 
     * @param metaData
     *            The database meta data
     * @param values
     *            The index meta data as defined by
     *            {@link #getColumnsForIndex()}
     * @param knownIndices
     *            The already read indices for the current table
     */
    protected void readIndex(DatabaseMetaDataWrapper metaData, Map<String, Object> values,
            Map<String, Index> knownIndices) throws SQLException {
        Short indexType = (Short) values.get("TYPE");

        // we're ignoring statistic indices
        if ((indexType != null) && (indexType.shortValue() == DatabaseMetaData.tableIndexStatistic)) {
            return;
        }

        String indexName = (String) values.get("INDEX_NAME");

        if (indexName != null) {
            Index index = (Index) knownIndices.get(indexName);

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

            indexColumn.setName((String) values.get("COLUMN_NAME"));
            if (values.containsKey("ORDINAL_POSITION")) {
                indexColumn.setOrdinalPosition(((Short) values.get("ORDINAL_POSITION")).intValue());
            }
            index.addColumn(indexColumn);
        }
    }

    /**
     * Reads the indicated columns from the result set.
     * 
     * @param resultSet
     *            The result set
     * @param columnDescriptors
     *            The descriptors of the columns to read
     * @return The read values keyed by the column name
     */
    protected Map<String, Object> readColumns(ResultSet resultSet,
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

    /**
     * Helper method that determines the auto increment status for the given
     * columns via the {@link ResultSetMetaData#isAutoIncrement(int)} method.
     * 
     * Fix problems following problems: 1) identifiers that use keywords 2)
     * different catalog and schema 3) different catalog separator character * @param
     * table The table
     * 
     * @param columnsToCheck
     *            The columns to check (e.g. the primary key columns)
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

            if (table.getCatalogName() != null && !table.getCatalogName().trim().equals("")) {
                appendIdentifier(query, table.getCatalogName());
                query.append(catalogSeparator);
            }
            if (table.getSchemaName() != null && !table.getSchemaName().trim().equals("")) {
                appendIdentifier(query, table.getSchemaName()).append(".");
            }
            appendIdentifier(query, table.getTableName()).append(" t WHERE 1 = 0");

            Statement stmt = null;
            try {
                stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query.toString());
                ResultSetMetaData rsMetaData = rs.getMetaData();

                for (int idx = 0; idx < columnsToCheck.length; idx++) {
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
            log.log(LogLevel.WARN, msg.toString());
        }
    }

    public StringBuilder appendIdentifier(StringBuilder query, String identifier) {
        if (getDbDialect().getDbDialectInfo().isDelimitedIdentifierModeOn()) {
            query.append(getDbDialectInfo().getDelimiterToken());
        }
        query.append(identifier);
        if (getDbDialect().getDbDialectInfo().isDelimitedIdentifierModeOn()) {
            query.append(getDbDialectInfo().getDelimiterToken());
        }
        return query;
    }

    /**
     * Sorts the foreign keys in the tables of the model.
     * 
     * @param model
     *            The model
     */
    protected void sortForeignKeys(Database model) {
        for (int tableIdx = 0; tableIdx < model.getTableCount(); tableIdx++) {
            model.getTable(tableIdx).sortForeignKeys(
                    getDbDialect().getDbDialectInfo().isDelimitedIdentifierModeOn());
        }
    }

    /**
     * Replaces a specific character sequence in the given text with the
     * character sequence whose escaped version it is.
     * 
     * @param text
     *            The text
     * @param unescaped
     *            The unescaped string, e.g. "'"
     * @param escaped
     *            The escaped version, e.g. "''"
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

}
