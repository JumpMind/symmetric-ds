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

import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.DatabaseMetaDataWrapper;
import org.apache.ddlutils.platform.MetaDataColumnDescriptor;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;

abstract public class AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(AbstractDbDialect.class);

    protected JdbcTemplate jdbcTemplate;

    protected Platform platform;

    protected Database cachedModel = new Database();

    protected SqlTemplate sqlTemplate;

    protected SQLErrorCodeSQLExceptionTranslator sqlErrorTranslator;

    private Map<Integer, String> _defaultSizes;

    protected String tablePrefix;

    private int streamingResultsFetchSize;

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

    public void init(Platform pf) {
        this.jdbcTemplate = new JdbcTemplate(pf.getDataSource());
        this.platform = pf;
        this.sqlErrorTranslator = new SQLErrorCodeSQLExceptionTranslator(pf.getDataSource());
    }

    abstract protected void initForSpecificDialect();

    public void initConfigDb(String tablePrefix) {
        initForSpecificDialect();
        addPrefixAndCreateTableIfNecessary(getConfigDdlDatabase(), tablePrefix);
    }

    final public boolean doesTriggerExist(String schema, String tableName, String triggerName) {
        try {
            return doesTriggerExistOnPlatform(schema, tableName, triggerName);
        } catch (Exception ex) {
            logger.warn("Could not figure out if the trigger exists.  Assuming that is does not.", ex);
            return false;
        }
    }

    abstract protected boolean doesTriggerExistOnPlatform(String schema, String tableName, String triggerName);

    public String getTransactionTriggerExpression() {
        return "null";
    }

    public String createInitalLoadSqlFor(Node node, Trigger config) {
        return sqlTemplate.createInitalLoadSql(node, this, config,
                getMetaDataFor(config.getSourceSchemaName(), config.getSourceTableName(), true)).trim();
    }

    public String createPurgeSqlFor(Node node, Trigger trig) {
        return sqlTemplate.createPurgeSql(node, this, trig);
    }

    public String createCsvDataSql(Trigger trig, String whereClause) {
        return sqlTemplate.createCsvDataSql(trig,
                getMetaDataFor(trig.getSourceSchemaName(), trig.getSourceTableName(), true), whereClause).trim();
    }

    public String createCsvPrimaryKeySql(Trigger trig, String whereClause) {
        return sqlTemplate.createCsvPrimaryKeySql(trig,
                getMetaDataFor(trig.getSourceSchemaName(), trig.getSourceTableName(), true), whereClause).trim();
    }

    /**
     * This method uses the ddlutil's model reader which uses the jdbc metadata to lookup up
     * table metadata.
     * <p/>
     * Dialect may optionally override this method to more efficiently lookup up table metadata
     * directly against information schemas. 
     */
    public Table getMetaDataFor(String schema, String tbNm, boolean useCache) {
        final String tableName = tbNm.toUpperCase();
        Table retTable = cachedModel.findTable(tableName);
        if (retTable == null || !useCache) {
            synchronized (this.getClass()) {
                try {
                    Table table = findTable(checkSchema(schema), tableName);

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
        return (schema == null || schema.trim().length() == 0) ? null : schema.toUpperCase();
    }

    public Table findTable(String _schema, String _tableName) throws Exception {
        final String schema = checkSchema(_schema);
        final String tableName = _tableName.toUpperCase();
        return (Table) jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                Table table = null;
                DatabaseMetaDataWrapper metaData = new DatabaseMetaDataWrapper();
                metaData.setMetaData(c.getMetaData());
                metaData.setCatalog(schema);
                metaData.setSchemaPattern(schema);
                metaData.setTableTypes(null);
                ResultSet tableData = metaData.getTables(tableName);
                while (tableData != null && tableData.next()) {
                    Map<String, Object> values = readColumns(tableData, initColumnsForTable());
                    table = readTable(metaData, values);
                }
                JdbcUtils.closeResultSet(tableData);
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
            Collection primaryKeys = readPrimaryKeyNames(metaData, tableName);
            for (Iterator it = primaryKeys.iterator(); it.hasNext(); table.findColumn((String) it.next(), true)
                    .setPrimaryKey(true))
                ;
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

    /**
     * Create the configured trigger.  The catalog will be changed to the source schema if the source schema 
     * is configured.
     */
    public void initTrigger(final DataEventType dml, final Trigger trigger, final TriggerHistory audit,
            final String tablePrefix, final Table table) {
        jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection con) throws SQLException, DataAccessException {
                String catalog = trigger.getSourceSchemaName();
                logger.info("Creating " + dml.toString() + " trigger for "
                        + (catalog != null ? (catalog + ".") : "") + trigger.getSourceTableName());
                String previousCatalog = con.getCatalog();
                try {
                    if (catalog != null) {
                        con.setCatalog(catalog);
                    }
                    Statement stmt = con.createStatement();
                    stmt.executeUpdate(createTriggerDDL(dml, trigger, audit, tablePrefix, table));
                    stmt.close();
                } finally {
                    if (catalog != null && !catalog.equalsIgnoreCase(previousCatalog)) {
                        con.setCatalog(previousCatalog);
                    }
                }
                return null;
            }
        });
    }

    public String createTriggerDDL(DataEventType dml, Trigger config, TriggerHistory audit, String tablePrefix,
            Table table) {
        return sqlTemplate.createTriggerDDL(this, dml, config, audit, tablePrefix, table, getDefaultSchema());
    }

    protected void addPrefixAndCreateTableIfNecessary(Database targetTables, String tablePrefix) {
        try {
            tablePrefix = tablePrefix.toLowerCase() + "_";

            Table[] tables = targetTables.getTables();

            boolean createTables = false;
            for (Table table : tables) {
                table.setName(tablePrefix.toUpperCase() + table.getName().toUpperCase());
                fixForeignKeys(table, tablePrefix, false);

                if (getMetaDataFor(getDefaultSchema(), table.getName().toUpperCase(), false) == null) {
                    createTables = true;
                }
            }

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
}
