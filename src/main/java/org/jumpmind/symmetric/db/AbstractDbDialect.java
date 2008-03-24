/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.db;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.ListOrderedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.IndexColumn;
import org.apache.ddlutils.model.NonUniqueIndex;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.UniqueIndex;
import org.apache.ddlutils.platform.DatabaseMetaDataWrapper;
import org.apache.ddlutils.platform.MetaDataColumnDescriptor;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.StatementCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

abstract public class AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(AbstractDbDialect.class);

    public static final int MAX_SYMMETRIC_SUPPORTED_TRIGGER_SIZE = 50;

    protected JdbcTemplate jdbcTemplate;

    protected Platform platform;

    protected Database cachedModel = new Database();

    protected SqlTemplate sqlTemplate;

    protected SQLErrorCodeSQLExceptionTranslator sqlErrorTranslator;

    private Map<Integer, String> _defaultSizes;

    protected String tablePrefix;

    private int streamingResultsFetchSize;

    private Boolean supportsGetGeneratedKeys;

    private TransactionTemplate transactionTemplate;

    private String engineName;

    protected AbstractDbDialect() {
        _defaultSizes = new HashMap<Integer, String>();
        _defaultSizes.put(new Integer(1), "254");
        _defaultSizes.put(new Integer(12), "254");
        _defaultSizes.put(new Integer(-1), "254");
        _defaultSizes.put(new Integer(-2), "254");
        _defaultSizes.put(new Integer(-3), "254");
        _defaultSizes.put(new Integer(-4), "254");
        _defaultSizes.put(new Integer(4), "32");
        _defaultSizes.put(new Integer(-5), "64");
        _defaultSizes.put(new Integer(7), "7,0");
        _defaultSizes.put(new Integer(6), "15,0");
        _defaultSizes.put(new Integer(8), "15,0");
        _defaultSizes.put(new Integer(3), "15,15");
        _defaultSizes.put(new Integer(2), "15,15");
    }

    public IColumnFilter getDatabaseColumnFilter() {
        return null;
    }

    public void prepareTableForDataLoad(Table table) {
    }

    public void cleanupAfterDataLoad(Table table) {
    }

    protected boolean allowsNullForIdentityColumn() {
        return true;
    }

    /**
     * Provide a default implementation of this method using DDLUtils, getMaxColumnNameLength()
     */
    public int getMaxTriggerNameLength() {
        int max = getPlatform().getPlatformInfo().getMaxColumnNameLength();
        return max < MAX_SYMMETRIC_SUPPORTED_TRIGGER_SIZE ? max : MAX_SYMMETRIC_SUPPORTED_TRIGGER_SIZE;
    }

    public String getDefaultCatalog() {
        return getDefaultSchema();
    }

    public void init(Platform pf) {
        this.jdbcTemplate = new JdbcTemplate(pf.getDataSource());
        this.platform = pf;
        this.sqlErrorTranslator = new SQLErrorCodeSQLExceptionTranslator(pf.getDataSource());
    }

    abstract protected void initForSpecificDialect();

    public void initConfigDb(String tablePrefix) {
        initForSpecificDialect();
        addPrefixAndCreateTablesIfNecessary(getConfigDdlDatabase());
        createRequiredFunctions();
    }

    final public boolean doesTriggerExist(String schema, String tableName, String triggerName) {
        try {
            return doesTriggerExistOnPlatform(schema, tableName, triggerName);
        } catch (Exception ex) {
            logger.warn("Could not figure out if the trigger exists.  Assuming that is does not.", ex);
            return false;
        }
    }

    protected void createRequiredFunctions() {
        String[] functions = sqlTemplate.getFunctionsToInstall();
        for (String funcName : functions) {
            if (jdbcTemplate.queryForInt(sqlTemplate.getFunctionInstalledSql(getDefaultSchema(), funcName)) == 0) {
                jdbcTemplate.update(sqlTemplate.getFunctionSql(funcName));
                logger.info("Just installed " + funcName);
            }
        }
    }

    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.NONE;
    }

    public boolean isBlobOverrideToBinary() {
        return false;
    }

    abstract protected boolean doesTriggerExistOnPlatform(String schema, String tableName, String triggerName);

    public String getTransactionTriggerExpression() {
        return "null";
    }

    public String createInitalLoadSqlFor(Node node, Trigger config) {
        return sqlTemplate.createInitalLoadSql(node, this, config,
                getMetaDataFor(getDefaultCatalog(), config.getSourceSchemaName(), config.getSourceTableName(), true))
                .trim();
    }

    public String createPurgeSqlFor(Node node, Trigger trig) {
        return sqlTemplate.createPurgeSql(node, this, trig);
    }

    public String createCsvDataSql(Trigger trig, String whereClause) {
        return sqlTemplate.createCsvDataSql(trig,
                getMetaDataFor(getDefaultCatalog(), trig.getSourceSchemaName(), trig.getSourceTableName(), true),
                whereClause).trim();
    }

    public String createCsvPrimaryKeySql(Trigger trig, String whereClause) {
        return sqlTemplate.createCsvPrimaryKeySql(trig,
                getMetaDataFor(getDefaultCatalog(), trig.getSourceSchemaName(), trig.getSourceTableName(), true),
                whereClause).trim();
    }

    /**
     * This method uses the ddlutil's model reader which uses the jdbc metadata to lookup up
     * table metadata.
     * <p/>
     * Dialect may optionally override this method to more efficiently lookup up table metadata
     * directly against information schemas. 
     */
    public Table getMetaDataFor(String catalog, String schema, String tableName, boolean useCache) {
        Table retTable = cachedModel.findTable(tableName);
        if (retTable == null || !useCache) {
            synchronized (this.getClass()) {
                try {
                    Table table = findTable(catalog, checkSchema(schema), tableName);

                    if (retTable != null) {
                        cachedModel.removeTable(retTable);
                    }

                    if (table != null) {
                        cachedModel.addTable(table);
                    }

                    retTable = table;
                } catch (RuntimeException ex) {
                    throw ex;
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        return retTable;
    }

    private String checkSchema(String schema) {
        return (schema == null || schema.trim().length() == 0) ? null : schema;
    }

    public Table findTable(String _catalog, String _schema, final String _tableName) throws Exception {
        final String schema = checkSchema(_schema);
        final String catalog = _catalog;
        return (Table) jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                Table table = null;
                DatabaseMetaDataWrapper metaData = new DatabaseMetaDataWrapper();
                metaData.setMetaData(c.getMetaData());
                metaData.setCatalog(catalog);
                metaData.setSchemaPattern(schema);
                metaData.setTableTypes(null);
                String tableName = _tableName;
                if (!supportsMixedCaseNamesInCatalog()) {
                    tableName = _tableName.toUpperCase();
                }
                ResultSet tableData = null;
                try {
                    tableData = metaData.getTables(tableName);
                    while (tableData != null && tableData.next()) {
                        Map<String, Object> values = readColumns(tableData, initColumnsForTable());
                        table = readTable(metaData, values);
                    }
                } finally {
                    JdbcUtils.closeResultSet(tableData);
                }
                return table;
            }
        });
    }

    @SuppressWarnings("unchecked")
    protected Table readTable(DatabaseMetaDataWrapper metaData, Map values) throws SQLException {
        String tableName = (String) values.get("TABLE_NAME");
        Table table = null;
        if (tableName != null && tableName.length() > 0) {
            table = new Table();
            table.setName(tableName);
            table.setType((String) values.get("TABLE_TYPE"));
            table.setCatalog((String) values.get("TABLE_CAT"));
            table.setSchema((String) values.get("TABLE_SCHEM"));
            table.setDescription((String) values.get("REMARKS"));
            table.addColumns(readColumns(metaData, tableName));
            table.addIndices(readIndices(metaData, tableName));
            Collection primaryKeys = readPrimaryKeyNames(metaData, tableName);
            for (Iterator it = primaryKeys.iterator(); it.hasNext(); table.findColumn((String) it.next(), true)
                    .setPrimaryKey(true))
                ;
            determineAutoIncrementFromResultSetMetaData(table, table.getColumns());
        }
        return table;
    }

    protected List<MetaDataColumnDescriptor> initColumnsForTable() {
        List<MetaDataColumnDescriptor> result = new ArrayList<MetaDataColumnDescriptor>();
        result.add(new MetaDataColumnDescriptor("TABLE_NAME", 12));
        result.add(new MetaDataColumnDescriptor("TABLE_TYPE", 12, "UNKNOWN"));
        result.add(new MetaDataColumnDescriptor("TABLE_CAT", 12));
        result.add(new MetaDataColumnDescriptor("TABLE_SCHEM", 12));
        result.add(new MetaDataColumnDescriptor("REMARKS", 12));
        return result;
    }

    protected List<MetaDataColumnDescriptor> initColumnsForColumn() {
        List<MetaDataColumnDescriptor> result = new ArrayList<MetaDataColumnDescriptor>();
        result.add(new MetaDataColumnDescriptor("COLUMN_DEF", 12));
        result.add(new MetaDataColumnDescriptor("TABLE_NAME", 12));
        result.add(new MetaDataColumnDescriptor("COLUMN_NAME", 12));
        result.add(new MetaDataColumnDescriptor("DATA_TYPE", 4, new Integer(1111)));
        result.add(new MetaDataColumnDescriptor("NUM_PREC_RADIX", 4, new Integer(10)));
        result.add(new MetaDataColumnDescriptor("DECIMAL_DIGITS", 4, new Integer(0)));
        result.add(new MetaDataColumnDescriptor("COLUMN_SIZE", 12));
        result.add(new MetaDataColumnDescriptor("IS_NULLABLE", 12, "YES"));
        result.add(new MetaDataColumnDescriptor("REMARKS", 12));
        return result;
    }

    protected List<MetaDataColumnDescriptor> initColumnsForPK() {
        List<MetaDataColumnDescriptor> result = new ArrayList<MetaDataColumnDescriptor>();
        result.add(new MetaDataColumnDescriptor("COLUMN_NAME", 12));
        result.add(new MetaDataColumnDescriptor("TABLE_NAME", 12));
        result.add(new MetaDataColumnDescriptor("PK_NAME", 12));
        return result;
    }

    @SuppressWarnings("unchecked")
    protected Collection<Column> readColumns(DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {
        ResultSet columnData = null;
        try {
            columnData = metaData.getColumns(tableName, null);
            List<Column> columns = new ArrayList<Column>();
            Map values = null;
            for (; columnData.next(); columns.add(readColumn(metaData, values))) {
                values = readColumns(columnData, initColumnsForColumn());
            }
            return columns;
        } finally {
            JdbcUtils.closeResultSet(columnData);
        }
    }

    @SuppressWarnings("unchecked")
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map values) throws SQLException {
        Column column = new Column();
        column.setName((String) values.get("COLUMN_NAME"));
        column.setDefaultValue((String) values.get("COLUMN_DEF"));
        column.setTypeCode(((Integer) values.get("DATA_TYPE")).intValue());
        column.setPrecisionRadix(((Integer) values.get("NUM_PREC_RADIX")).intValue());
        String size = (String) values.get("COLUMN_SIZE");
        int scale = ((Integer) values.get("DECIMAL_DIGITS")).intValue();
        if (size == null)
            size = (String) _defaultSizes.get(new Integer(column.getTypeCode()));
        column.setSize(size);
        if (scale != 0)
            column.setScale(scale);
        column.setRequired("NO".equalsIgnoreCase(((String) values.get("IS_NULLABLE")).trim()));
        column.setDescription((String) values.get("REMARKS"));
        return column;
    }

    protected void determineAutoIncrementFromResultSetMetaData(Table table, final Column columnsToCheck[])
            throws SQLException {
        StringBuffer query;
        if (columnsToCheck == null || columnsToCheck.length == 0)
            return;
        query = new StringBuffer();
        query.append("SELECT ");
        for (int idx = 0; idx < columnsToCheck.length; idx++) {
            if (idx > 0)
                query.append(",");
            if (getPlatform().isDelimitedIdentifierModeOn())
                query.append(platform.getPlatformInfo().getDelimiterToken());
            query.append("t.").append(columnsToCheck[idx].getName());
            if (getPlatform().isDelimitedIdentifierModeOn())
                query.append(platform.getPlatformInfo().getDelimiterToken());
        }

        query.append(" FROM ");
        if (getPlatform().isDelimitedIdentifierModeOn())
            query.append(platform.getPlatformInfo().getDelimiterToken());
        if (table.getSchema() != null && ! table.getSchema().trim().equals("")) {
            query.append(table.getSchema() + ".");
        }
        query.append(table.getName());
        if (getPlatform().isDelimitedIdentifierModeOn())
            query.append(platform.getPlatformInfo().getDelimiterToken());
        query.append(" t WHERE 1 = 0");
        final String finalQuery = query.toString();
        jdbcTemplate.execute(new StatementCallback() {
            public Object doInStatement(Statement stmt) throws SQLException, DataAccessException {
                ResultSet rs = stmt.executeQuery(finalQuery);
                ResultSetMetaData rsMetaData = rs.getMetaData();
                for (int idx = 0; idx < columnsToCheck.length; idx++)
                    if (rsMetaData.isAutoIncrement(idx + 1))
                        columnsToCheck[idx].setAutoIncrement(true);
                return null;
            }
        });
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> readColumns(ResultSet resultSet, List columnDescriptors) throws SQLException {
        HashMap<String, Object> values = new HashMap<String, Object>();
        MetaDataColumnDescriptor descriptor;
        for (Iterator it = columnDescriptors.iterator(); it.hasNext(); values.put(descriptor.getName(), descriptor
                .readColumn(resultSet)))
            descriptor = (MetaDataColumnDescriptor) it.next();

        return values;
    }

    @SuppressWarnings("unchecked")
    protected Collection<String> readPrimaryKeyNames(DatabaseMetaDataWrapper metaData, String tableName)
            throws SQLException {
        ResultSet pkData = null;
        try {
            List<String> pks = new ArrayList<String>();
            Map values;
            for (pkData = metaData.getPrimaryKeys(tableName); pkData.next(); pks.add(readPrimaryKeyName(metaData,
                    values))) {
                values = readColumns(pkData, initColumnsForPK());
            }
            return pks;
        } finally {
            JdbcUtils.closeResultSet(pkData);
        }

    }

    @SuppressWarnings("unchecked")
    protected String readPrimaryKeyName(DatabaseMetaDataWrapper metaData, Map values) throws SQLException {
        return (String) values.get("COLUMN_NAME");
    }

    @SuppressWarnings("unchecked")
    protected List initColumnsForIndex() {
        List result = new ArrayList();

        result.add(new MetaDataColumnDescriptor("INDEX_NAME", Types.VARCHAR));
        // we're also reading the table name so that a model reader impl can
        // filter manually
        result.add(new MetaDataColumnDescriptor("TABLE_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("NON_UNIQUE", Types.BIT, Boolean.TRUE));
        result.add(new MetaDataColumnDescriptor("ORDINAL_POSITION", Types.TINYINT, new Short((short) 0)));
        result.add(new MetaDataColumnDescriptor("COLUMN_NAME", Types.VARCHAR));
        result.add(new MetaDataColumnDescriptor("TYPE", Types.TINYINT));

        return result;
    }

    @SuppressWarnings("unchecked")
    protected Collection readIndices(DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {
        Map indices = new ListOrderedMap();
        ResultSet indexData = null;

        try {
            indexData = metaData.getIndices(tableName, false, false);

            while (indexData.next()) {
                Map values = readColumns(indexData, initColumnsForIndex());

                readIndex(metaData, values, indices);
            }
        } finally {
            if (indexData != null) {
                indexData.close();
            }
        }
        return indices.values();
    }

    @SuppressWarnings("unchecked")
    protected void readIndex(DatabaseMetaDataWrapper metaData, Map values, Map knownIndices) throws SQLException {
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
     * Create the configured trigger. The catalog will be changed to the source
     * schema if the source schema is configured.
     */
    public void initTrigger(final DataEventType dml, final Trigger trigger, final TriggerHistory audit,
            final String tablePrefix, final Table table) {
        jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection con) throws SQLException, DataAccessException {
                String previousSourceSchema = trigger.getSourceSchemaName();
                logger.info("Creating " + dml.toString() + " trigger for "
                        + (previousSourceSchema != null ? (previousSourceSchema + ".") : "")
                        + trigger.getSourceTableName());
                String previousCatalog = null;
                try {
                    previousCatalog = switchSchemasForTriggerInstall(previousSourceSchema, con);
                    Statement stmt = con.createStatement();
                    String triggerSql = createTriggerDDL(dml, trigger, audit, tablePrefix, table);
                    try {
                        stmt.executeUpdate(triggerSql);
                    } catch (SQLException ex) {
                        logger.error("Failed to create trigger: " + triggerSql);
                        throw ex;
                    }
                    String postTriggerDml = createPostTriggerDDL(dml, trigger, audit, tablePrefix, table);
                    if (postTriggerDml != null) {
                        try {
                            stmt.executeUpdate(postTriggerDml);
                        } catch (SQLException ex) {
                            logger.error("Failed to create post trigger: " + postTriggerDml);
                            throw ex;
                        }
                    }
                    stmt.close();

                } finally {
                    if (previousSourceSchema != null && !previousSourceSchema.equalsIgnoreCase(previousCatalog)) {
                        switchSchemasForTriggerInstall(previousCatalog, con);
                    }
                }
                return null;
            }
        });
    }

    /**
     * Provide the option switch a connection's schema for trigger installation.
     */
    protected String switchSchemasForTriggerInstall(String schema, Connection c) throws SQLException {
        return null;
    }

    public String createTriggerDDL(DataEventType dml, Trigger config, TriggerHistory audit, String tablePrefix,
            Table table) {
        return sqlTemplate.createTriggerDDL(this, dml, config, audit, tablePrefix, table, getDefaultSchema());
    }

    public String createPostTriggerDDL(DataEventType dml, Trigger config, TriggerHistory audit, String tablePrefix,
            Table table) {
        return sqlTemplate.createPostTriggerDDL(this, dml, config, audit, tablePrefix, table, getDefaultSchema());
    }

    public String getCreateSymmetricDDL() {
        Database db = getConfigDdlDatabase();
        prefixConfigDatabase(db);
        return platform.getCreateTablesSql(db, true, true);
    }

    public String getCreateTableSQL(Trigger trig) {
        Table table = getMetaDataFor(null, trig.getSourceSchemaName(), trig.getSourceTableName(), true);
        String sql = null;
        try {
            StringWriter buffer = new StringWriter();
            platform.getSqlBuilder().setWriter(buffer);
            platform.getSqlBuilder().createTable(cachedModel, table);
            sql = buffer.toString();
        } catch (IOException e) {
        }
        return sql;
    }

    public String getCreateTableXML(Trigger trig) {
        Table table = getMetaDataFor(null, trig.getSourceSchemaName(), trig.getSourceTableName(), true);
        Database db = new Database();
        db.setName(trig.getSourceSchemaName() != null ? trig.getSourceSchemaName() : getDefaultSchema());
        db.addTable(table);
        StringWriter buffer = new StringWriter();
        DatabaseIO xmlWriter = new DatabaseIO();
        xmlWriter.write(db, buffer);
        // TODO: remove when these bugs are fixed in DdlUtils 
        return buffer.toString().replaceAll("&apos;", "").replaceAll("default=\"empty_blob\\(\\) *\"", "");
    }

    public void createTables(String xml) {
        StringReader reader = new StringReader(xml);
        Database db = new DatabaseIO().read(reader);
        platform.createTables(db, true, true);
    }

    protected boolean prefixConfigDatabase(Database targetTables) {
        try {
            String tblPrefix = this.tablePrefix.toLowerCase() + "_";

            Table[] tables = targetTables.getTables();

            boolean createTables = false;
            for (Table table : tables) {
                table.setName(tblPrefix + table.getName().toLowerCase());
                fixForeignKeys(table, tblPrefix, false);

                if (getMetaDataFor(getDefaultCatalog(), getDefaultSchema(), table.getName(), false) == null) {
                    createTables = true;
                }
            }

            return createTables;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    protected void addPrefixAndCreateTablesIfNecessary(Database targetTables) {
        try {
            boolean createTables = prefixConfigDatabase(targetTables);
            if (createTables) {
                logger.info("About to create symmetric tables.");
                platform.createTables(targetTables, false, true);
            } else {
                logger.info("No need to create symmetric tables.  They already exist.");
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected Database getConfigDdlDatabase() {
        try {
            return new DatabaseIO().read(new InputStreamReader(getConfigDdlXml().openStream()));
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected URL getConfigDdlXml() {
        return AbstractDbDialect.class.getResource("/ddl-config.xml");
    }

    protected void fixForeignKeys(Table table, String tablePrefix, boolean clone) throws CloneNotSupportedException {
        ForeignKey[] keys = table.getForeignKeys();
        for (ForeignKey key : keys) {
            if (clone) {
                table.removeForeignKey(key);
                key = (ForeignKey) key.clone();
                table.addForeignKey(key);
            }
            String prefixedName = tablePrefix + key.getForeignTableName().toLowerCase();
            key.setForeignTableName(prefixedName);
        }
    }

    public Platform getPlatform() {
        return this.platform;
    }

    public String getName() {
        return (String) jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                return c.getMetaData().getDatabaseProductName();
            }
        });
    }

    public String getVersion() {
        return (String) jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData meta = c.getMetaData();
                return meta.getDatabaseMajorVersion() + "." + meta.getDatabaseMinorVersion();
            }
        });
    }

    public String replaceTemplateVariables(DataEventType dml, Trigger trigger, TriggerHistory history,
            String targetString) {
        return sqlTemplate.replaceTemplateVariables(this, dml, trigger, history, tablePrefix, getMetaDataFor(
                getDefaultCatalog(), trigger.getSourceSchemaName(), trigger.getSourceTableName(), true),
                getDefaultSchema(), targetString);
    }

    public boolean supportsGetGeneratedKeys() {
        if (supportsGetGeneratedKeys == null) {
            supportsGetGeneratedKeys = (Boolean) jdbcTemplate.execute(new ConnectionCallback() {
                public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                    return conn.getMetaData().supportsGetGeneratedKeys();
                }
            });
        }
        return supportsGetGeneratedKeys;
    }

    public String getSelectLastInsertIdSql(String sequenceName) {
        throw new UnsupportedOperationException();
    }

    public long insertWithGeneratedKey(final String sql, final String sequenceName) {
        return insertWithGeneratedKey(sql, sequenceName, null);
    }

    public long insertWithGeneratedKey(final String sql, final String sequenceName,
            final PreparedStatementCallback callback) {
        return (Long) jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                long key = 0;
                PreparedStatement ps = null;
                boolean supportsGetGeneratedKeys = supportsGetGeneratedKeys();
                if (allowsNullForIdentityColumn()) {
                    ps = conn.prepareStatement(sql, new int[] { 1 });
                } else {
                    String replaceSql = sql.replaceFirst("\\(\\w*,", "(").replaceFirst("\\(null,", "(");
                    if (supportsGetGeneratedKeys) {
                        ps = conn.prepareStatement(replaceSql, Statement.RETURN_GENERATED_KEYS);
                    } else {
                        ps = conn.prepareStatement(replaceSql);
                    }
                }
                ps.setQueryTimeout(jdbcTemplate.getQueryTimeout());
                if (callback != null) {
                    callback.doInPreparedStatement(ps);
                }
                ps.executeUpdate();

                if (supportsGetGeneratedKeys) {
                    ResultSet rs = ps.getGeneratedKeys();
                    if (rs.next()) {
                        key = rs.getLong(1);
                    }
                    JdbcUtils.closeResultSet(rs);
                } else {
                    Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(getSelectLastInsertIdSql(sequenceName));
                    if (rs.next()) {
                        key = rs.getLong(1);
                    }
                    JdbcUtils.closeResultSet(rs);
                    JdbcUtils.closeStatement(st);
                }
                JdbcUtils.closeStatement(ps);
                return key;
            }
        });
    }

    public Object createSavepoint() {
        return transactionTemplate.execute(new TransactionCallback() {
            public Object doInTransaction(TransactionStatus transactionstatus) {
                return transactionstatus.createSavepoint();
            }
        });
    }

    public Object createSavepointForFallback() {
        if (requiresSavepointForFallback()) {
            return createSavepoint();
        }
        return null;
    }

    public void rollbackToSavepoint(final Object savepoint) {
        if (savepoint != null) {
            transactionTemplate.execute(new TransactionCallbackWithoutResult() {
                protected void doInTransactionWithoutResult(TransactionStatus transactionstatus) {
                    transactionstatus.rollbackToSavepoint(savepoint);
                }
            });
        }
    }

    public void releaseSavepoint(final Object savepoint) {
        if (savepoint != null) {
            transactionTemplate.execute(new TransactionCallbackWithoutResult() {
                protected void doInTransactionWithoutResult(TransactionStatus transactionstatus) {
                    transactionstatus.releaseSavepoint(savepoint);
                }
            });
        }
    }

    public boolean requiresSavepointForFallback() {
        return false;
    }

    public boolean supportsTransactionId() {
        return false;
    }

    public boolean isBlobSyncSupported() {
        return true;
    }

    public boolean isClobSyncSupported() {
        return true;
    }

    public void setSqlTemplate(SqlTemplate sqlTemplate) {
        this.sqlTemplate = sqlTemplate;
    }

    public SQLErrorCodeSQLExceptionTranslator getSqlErrorTranslator() {
        return sqlErrorTranslator;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public int getStreamingResultsFetchSize() {
        return streamingResultsFetchSize;
    }

    public void setStreamingResultsFetchSize(int streamingResultsFetchSize) {
        this.streamingResultsFetchSize = streamingResultsFetchSize;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public void setTransactionTemplate(TransactionTemplate transactionTemplate) {
        this.transactionTemplate = transactionTemplate;
    }

    public String getEngineName() {
        return engineName;
    }

    public void setEngineName(String engineName) {
        this.engineName = engineName;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

}
