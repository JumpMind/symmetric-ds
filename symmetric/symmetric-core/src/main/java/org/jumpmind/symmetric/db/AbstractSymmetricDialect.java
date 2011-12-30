/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
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
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.BinaryEncoding;
import org.jumpmind.db.IDatabasePlatform;
import org.jumpmind.db.IDdlBuilder;
import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.SqlConstants;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.IDatabaseUpgradeListener;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.util.FormatUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.lob.LobHandler;

/*
 * The abstract class for database dialects.
 */
abstract public class AbstractSymmetricDialect implements ISymmetricDialect {

    protected ILog log = LogFactory.getLog(getClass());

    public static final int MAX_SYMMETRIC_SUPPORTED_TRIGGER_SIZE = 50;

    protected JdbcTemplate jdbcTemplate;

    protected IDatabasePlatform platform;

    protected TriggerText triggerText;

    protected IParameterService parameterService;

    protected String tablePrefix;

    protected int streamingResultsFetchSize;

    protected Boolean supportsGetGeneratedKeys;

    protected String databaseName;

    protected String driverVersion;

    protected String driverName;

    protected int databaseMajorVersion;

    protected int databaseMinorVersion;

    protected String databaseProductVersion;

    protected Set<String> sqlKeywords;

    protected LobHandler lobHandler;

    protected boolean supportsTransactionViews = false;

    protected int queryTimeoutInSeconds = 300;


    protected List<IDatabaseUpgradeListener> databaseUpgradeListeners = new ArrayList<IDatabaseUpgradeListener>();

    protected AbstractSymmetricDialect() {
    }

    public String encodeForCsv(byte[] data) {
        if (data != null) {
            BinaryEncoding encoding = getBinaryEncoding();
            if (BinaryEncoding.BASE64.equals(encoding)) {
                return new String(Base64.encodeBase64(data));
            } else if (BinaryEncoding.HEX.equals(encoding)) {
                return new String(Hex.encodeHex(data));
            } else {
                throw new NotImplementedException();
            }
        } else {
            return null;
        }
    }

    public String toFormattedTimestamp(java.util.Date time) {
        StringBuilder ts = new StringBuilder("{ts '");
        ts.append(SqlConstants.JDBC_TIMESTAMP_FORMATTER.format(time));
        ts.append("'}");
        return ts.toString();
    }

    protected boolean allowsNullForIdentityColumn() {
        return true;
    }

    public boolean requiresAutoCommitFalseToSetFetchSize() {
        return false;
    }

    /*
     * Provide a default implementation of this method using DDLUtils,
     * getMaxColumnNameLength()
     */
    public int getMaxTriggerNameLength() {
        int max = getPlatform().getPlatformInfo().getMaxColumnNameLength();
        return max < MAX_SYMMETRIC_SUPPORTED_TRIGGER_SIZE && max > 0 ? max
                : MAX_SYMMETRIC_SUPPORTED_TRIGGER_SIZE;
    }

    public void init(IDatabasePlatform pf, int queryTimeout, JdbcTemplate jdbcTemplate) {
        log.info("DbDialectInUse", this.getClass().getName());
        this.jdbcTemplate = jdbcTemplate;
        this.queryTimeoutInSeconds = queryTimeout;
        this.jdbcTemplate.setQueryTimeout(queryTimeout);
        this.platform = pf;
        jdbcTemplate.execute(new ConnectionCallback<Object>() {
            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData meta = c.getMetaData();
                databaseName = meta.getDatabaseProductName();
                databaseMajorVersion = meta.getDatabaseMajorVersion();
                databaseMinorVersion = meta.getDatabaseMinorVersion();
                databaseProductVersion = meta.getDatabaseProductVersion();
                driverName = meta.getDriverName();
                driverVersion = meta.getDriverVersion();
                return null;
            }
        });
        this.initLobHandler();
    }

    public int getQueryTimeoutInSeconds() {
        return queryTimeoutInSeconds;
    }

    protected void initTablesAndFunctionsForSpecificDialect() {
    }

    public void initTablesAndFunctions() {
        initTablesAndFunctionsForSpecificDialect();
        createTablesIfNecessary();
        createRequiredFunctions();
        platform.resetCachedTableModel();
    }

    final public boolean doesTriggerExist(String catalogName, String schema, String tableName,
            String triggerName) {
        try {
            return doesTriggerExistOnPlatform(catalogName, schema, tableName, triggerName);
        } catch (Exception ex) {
            log.warn("TriggerMayExist", ex);
            return false;
        }
    }

    protected void createRequiredFunctions() {
        String[] functions = triggerText.getFunctionsToInstall();
        for (int i = 0; i < functions.length; i++) {
            String funcName = tablePrefix + "_" + functions[i];
            if (jdbcTemplate.queryForInt(triggerText.getFunctionInstalledSql(funcName,
                    platform.getDefaultSchema())) == 0) {
                jdbcTemplate.update(triggerText.getFunctionSql(functions[i], funcName,
                        platform.getDefaultSchema()));
                log.info("FunctionInstalled", funcName);
            }
        }
    }

    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.NONE;
    }

    abstract protected boolean doesTriggerExistOnPlatform(String catalogName, String schema,
            String tableName, String triggerName);

    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        return "null";
    }

    public String createInitialLoadSqlFor(Node node, TriggerRouter trigger, Table table,
            TriggerHistory triggerHistory, Channel channel) {
        return triggerText.createInitalLoadSql(node, this, trigger, table, triggerHistory, channel)
                .trim();
    }

    public String createPurgeSqlFor(Node node, TriggerRouter triggerRouter) {
        return String.format(
                parameterService.getString(ParameterConstants.INITIAL_LOAD_DELETE_FIRST_SQL),
                triggerRouter.qualifiedTargetTableName());
    }

    public String createCsvDataSql(Trigger trigger, TriggerHistory triggerHistory, Channel channel,
            String whereClause) {
        return triggerText.createCsvDataSql(
                this,
                trigger,
                triggerHistory,
                platform.getTableFromCache(trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                        trigger.getSourceTableName(), false), channel, whereClause).trim();
    }

    public String createCsvPrimaryKeySql(Trigger trigger, TriggerHistory triggerHistory,
            Channel channel, String whereClause) {
        return triggerText.createCsvPrimaryKeySql(
                this,
                trigger,
                triggerHistory,
                platform.getTableFromCache(trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                        trigger.getSourceTableName(), false), channel, whereClause).trim();
    }

    public Set<String> getSqlKeywords() {
        if (sqlKeywords == null) {
            jdbcTemplate.execute(new ConnectionCallback<Object>() {
                public Object doInConnection(Connection con) throws SQLException,
                        DataAccessException {
                    DatabaseMetaData metaData = con.getMetaData();
                    sqlKeywords = new HashSet<String>(Arrays.asList(metaData.getSQLKeywords()
                            .split(",")));
                    return null;
                }
            });
        }
        return sqlKeywords;
    }

    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName,
            String triggerName, String tableName, TriggerHistory oldHistory) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        final String sql = "drop trigger " + schemaName + triggerName;
        logSql(sql, sqlBuffer);
        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            try {
                jdbcTemplate.update(sql);
            } catch (Exception e) {
                log.warn("TriggerDoesNotExist");
            }
        }
    }

    final protected void logSql(String sql, StringBuilder sqlBuffer) {
        if (sqlBuffer != null) {
            sqlBuffer.append(sql);
            sqlBuffer.append(System.getProperty("line.separator"));
            sqlBuffer.append(System.getProperty("line.separator"));
        }
    }

    /*
     * Create the configured trigger. The catalog will be changed to the source
     * schema if the source schema is configured.
     */
    public void createTrigger(final StringBuilder sqlBuffer, final DataEventType dml,
            final Trigger trigger, final TriggerHistory hist, final Channel channel,
            final String tablePrefix, final Table table) {
        jdbcTemplate.execute(new ConnectionCallback<Object>() {
            public Object doInConnection(Connection con) throws SQLException, DataAccessException {
                log.info("TriggerCreating", hist.getTriggerNameForDmlType(dml),
                        trigger.getSourceTableName());

                String previousCatalog = null;
                String sourceCatalogName = trigger.getSourceCatalogName();
                String defaultCatalog = platform.getDefaultCatalog();
                String defaultSchema = platform.getDefaultSchema();
                try {
                    previousCatalog = switchCatalogForTriggerInstall(sourceCatalogName, con);

                    String triggerSql = triggerText.createTriggerDDL(AbstractSymmetricDialect.this, dml,
                            trigger, hist, channel, tablePrefix, table, defaultCatalog,
                            defaultSchema);

                    if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
                        Statement stmt = con.createStatement();
                        stmt.setQueryTimeout(jdbcTemplate.getQueryTimeout());
                        try {
                            log.debug("Sql", triggerSql);
                            stmt.executeUpdate(triggerSql);
                        } catch (SQLException ex) {
                            log.error("TriggerCreateFailed", triggerSql);
                            throw ex;
                        }
                        String postTriggerDml = createPostTriggerDDL(dml, trigger, hist, channel,
                                tablePrefix, table);
                        if (postTriggerDml != null) {
                            try {
                                stmt.executeUpdate(postTriggerDml);
                            } catch (SQLException ex) {
                                log.error("PostTriggerCreateFailed", postTriggerDml);
                                throw ex;
                            }
                        }
                        stmt.close();
                    }

                    logSql(triggerSql, sqlBuffer);

                } finally {
                    if (sourceCatalogName != null
                            && !sourceCatalogName.equalsIgnoreCase(previousCatalog)) {
                        switchCatalogForTriggerInstall(previousCatalog, con);
                    }
                }
                return null;
            }
        });
    }

    /*
     * Provide the option switch a connection's schema for trigger installation.
     */
    protected String switchCatalogForTriggerInstall(String catalog, Connection c)
            throws SQLException {
        return null;
    }

    protected String createPostTriggerDDL(DataEventType dml, Trigger trigger, TriggerHistory hist,
            Channel channel, String tablePrefix, Table table) {
        return triggerText.createPostTriggerDDL(this, dml, trigger, hist, channel, tablePrefix,
                table, platform.getDefaultCatalog(), platform.getDefaultSchema());
    }

    public String getCreateSymmetricDDL() {
        Database database = readSymmetricSchemaFromXml();
        prefixConfigDatabase(database);
        IDdlBuilder builder = platform.getDdlBuilder();
        return builder.createTables(database, true);
    }

    public String getCreateTableSQL(TriggerRouter triggerRouter) {
        Table table = platform.getTableFromCache(null, triggerRouter.getTrigger().getSourceSchemaName(),
                triggerRouter.getTrigger().getSourceTableName(), false);
        return platform.getDdlBuilder().createTable(table);
    }

    private void setDatabaseName(TriggerRouter triggerRouter, Database db) {
        db.setName(triggerRouter.getTargetSchema(platform.getDefaultSchema()));
        if (db.getName() == null) {
            db.setName(platform.getDefaultCatalog());
        }
        if (db.getName() == null) {
            db.setName("DDL");
        }
    }

    public String getCreateTableXML(TriggerRouter triggerRouter) {
        Table table = getTable(triggerRouter.getTrigger(), true);
        table.setName(triggerRouter.getTargetTable());
        Database db = new Database();
        setDatabaseName(triggerRouter, db);
        db.addTable(table);
        StringWriter buffer = new StringWriter();
        DatabaseIO xmlWriter = new DatabaseIO();
        xmlWriter.write(db, buffer);
        // TODO: remove when these bugs are fixed in DdlUtils
        String xml = buffer.toString().replaceAll("&apos;", "");
        xml = xml.replaceAll("default=\"empty_blob\\(\\) *\"", "");
        xml = xml.replaceAll("unique name=\"PRIMARY\"", "unique name=\"PRIMARYINDEX\"");
        // on postgres, this is a "text" column
        xml = xml.replaceAll("type=\"VARCHAR\" size=\"2147483647\"", "type=\"LONGVARCHAR\"");
        xml = xml.replaceAll("type=\"BINARY\" size=\"2147483647\"", "type=\"LONGVARBINARY\"");
        return xml;
    }

    public void createTables(String xml) {
        StringReader reader = new StringReader(xml);
        Database db = new DatabaseIO().read(reader);
        platform.createDatabase(db, true, true);
    }

    public boolean doesDatabaseNeedConfigured() {
        return prefixConfigDatabase(readSymmetricSchemaFromXml());
    }        

    protected boolean prefixConfigDatabase(Database targetTables) {
        try {
            String tblPrefix = this.tablePrefix + "_";

            Table[] tables = targetTables.getTables();

            boolean createTables = false;
            for (Table table : tables) {
                table.setName(tblPrefix + table.getName());
                fixForeignKeys(table, tblPrefix);
                fixIndexes(table, tblPrefix);
                if (platform.getTableFromCache(platform.getDefaultCatalog(), platform.getDefaultSchema(), table.getName(), true) == null) {
                    createTables = true;
                }
            }

            return createTables;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
    
    public Table getTable(Trigger trigger, boolean useCache) {
        return platform.getTableFromCache(trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                trigger.getSourceTableName(), !useCache);
    }

    /*
     * @return true if SQL was executed.
     */
    protected boolean createTablesIfNecessary() {

        Database modelFromXml = readSymmetricSchemaFromXml();

        String extraTablesXml = parameterService
                .getString(ParameterConstants.AUTO_CONFIGURE_EXTRA_TABLES);
        if (StringUtils.isNotBlank(extraTablesXml)) {
            try {
                modelFromXml = merge(modelFromXml, readDatabaseFromXml(extraTablesXml));
            } catch (Exception ex) {
                log.error(ex);
            }
        }

        try {
            log.info("TablesAutoUpdatingStart");
            Database modelFromDatabase = new Database();

            Table[] tablesFromXml = modelFromXml.getTables();
            for (Table tableFromXml : tablesFromXml) {
                Table tableFromDatabase = platform.getTableFromCache(platform.getDefaultCatalog(),
                        platform.getDefaultSchema(), tableFromXml.getName(), true);
                if (tableFromDatabase != null) {
                    modelFromDatabase.addTable(tableFromDatabase);
                }
            }

            IDdlBuilder builder = platform.getDdlBuilder();

            if (builder.isAlterDatabase(modelFromDatabase, modelFromXml)) {
                log.info("TablesAutoUpdatingFoundTablesToAlter");
                String delimiter = platform.getPlatformInfo().getSqlCommandDelimiter();

                for (IDatabaseUpgradeListener listener : databaseUpgradeListeners) {
                    String sql = listener.beforeUpgrade(this, tablePrefix, modelFromDatabase,
                            modelFromXml);
                    new SqlScript(sql, getPlatform().getSqlTemplate(), true, delimiter, null).execute();
                }

                String alterSql = builder.alterDatabase(modelFromDatabase, modelFromXml);
                
                if (log.isDebugEnabled()) {
                    log.debug("TablesAutoUpdatingAlterSql", alterSql);
                }
                new SqlScript(alterSql, getPlatform().getSqlTemplate(), true, delimiter, null).execute();

                for (IDatabaseUpgradeListener listener : databaseUpgradeListeners) {
                    String sql = listener.afterUpgrade(this, tablePrefix, modelFromXml);
                    new SqlScript(sql, getPlatform().getSqlTemplate(), true, delimiter, null).execute();
                }

                log.info("TablesAutoUpdatingDone");
                return true;
            } else {
                return false;
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected Database readSymmetricSchemaFromXml() {
        try {
            Database database = merge(readDatabaseFromXml("/symmetric-schema.xml"),
                    readDatabaseFromXml("/console-schema.xml"));

            if (prefixConfigDatabase(database)) {
                log.info("TablesMissing");
            }
            return database;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected Database readDatabaseFromXml(String resourceName) throws IOException {
        URL url = AbstractSymmetricDialect.class.getResource(resourceName);
        if (url != null) {
            DatabaseIO io = new DatabaseIO();
            io.setValidateXml(false);
            return io.read(new InputStreamReader(url.openStream()));
        } else {
            return new Database();
        }
    }

    protected Database merge(Database... databases) {
        Database database = new Database();
        if (databases != null) {
            for (Database db : databases) {
                Table[] tables = db.getTables();
                for (Table table : tables) {
                    database.addTable(table);
                }
            }
        }
        return database;
    }

    protected void fixForeignKeys(Table table, String tablePrefix)
            throws CloneNotSupportedException {
        ForeignKey[] keys = table.getForeignKeys();
        for (ForeignKey key : keys) {
            String prefixedName = tablePrefix + key.getForeignTableName();
            key.setForeignTableName(prefixedName);
            key.setName(tablePrefix + key.getName());
        }
    }

    protected void fixIndexes(Table table, String tablePrefix) throws CloneNotSupportedException {
        IIndex[] indexes = table.getIndices();
        if (indexes != null) {
            for (IIndex index : indexes) {
                String prefixedName = tablePrefix + index.getName();
                index.setName(prefixedName);
            }
        }
    }

    public IDatabasePlatform getPlatform() {
        return this.platform;
    }

    public String getName() {
        return databaseName;
    }

    public String getVersion() {
        return databaseMajorVersion + "." + databaseMinorVersion;
    }

    public int getMajorVersion() {
        return databaseMajorVersion;
    }

    public int getMinorVersion() {
        return databaseMinorVersion;
    }

    public String getProductVersion() {
        return databaseProductVersion;
    }

    public boolean supportsGetGeneratedKeys() {
        if (supportsGetGeneratedKeys == null) {
            supportsGetGeneratedKeys = jdbcTemplate.execute(new ConnectionCallback<Boolean>() {
                public Boolean doInConnection(Connection conn) throws SQLException,
                        DataAccessException {
                    return conn.getMetaData().supportsGetGeneratedKeys();
                }
            });
        }
        return supportsGetGeneratedKeys;
    }

    public boolean supportsTransactionViews() {
        return supportsTransactionViews;
    }

    public boolean supportsReturningKeys() {
        return false;
    }

    public String getSelectLastInsertIdSql(String sequenceName) {
        throw new UnsupportedOperationException();
    }

    public long insertWithGeneratedKey(final String sql, final SequenceIdentifier sequenceId) {
        return insertWithGeneratedKey(jdbcTemplate, sql, sequenceId, null);
    }

    protected String getSequenceName(SequenceIdentifier identifier) {
        switch (identifier) {
        case OUTGOING_BATCH:
            return "sym_outgoing_batch_batch_id";
        case DATA:
            return "sym_data_data_id";
        case TRIGGER_HIST:
            return "sym_trigger_his_ger_hist_id";
        }
        return null;
    }

    protected String getSequenceKeyName(SequenceIdentifier identifier) {
        switch (identifier) {
        case OUTGOING_BATCH:
            return "batch_id";
        case DATA:
            return "data_id";
        case TRIGGER_HIST:
            return "trigger_hist_id";
        }
        return null;
    }

    @Deprecated
    public Column[] orderColumns(String[] columnNames, Table table) {
        Column[] unorderedColumns = table.getColumns();
        Column[] orderedColumns = new Column[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            String name = columnNames[i];
            for (Column column : unorderedColumns) {
                if (column.getName().equalsIgnoreCase(name)) {
                    orderedColumns[i] = column;
                    break;
                }
            }
        }
        return orderedColumns;
    }
    
    public long insertWithGeneratedKey(final String sql, final SequenceIdentifier sequenceId,
            final PreparedStatementCallback<Object> callback) {
        return insertWithGeneratedKey(jdbcTemplate, sql, sequenceId, callback);
    }

    public long insertWithGeneratedKey(final JdbcTemplate jdbcTemplate, final String sql,
            final SequenceIdentifier sequenceId, final PreparedStatementCallback<Object> callback) {
        return jdbcTemplate.execute(new ConnectionCallback<Long>() {
            public Long doInConnection(Connection conn) throws SQLException, DataAccessException {

                long key = 0;
                PreparedStatement ps = null;
                try {
                    boolean supportsGetGeneratedKeys = supportsGetGeneratedKeys();
                    boolean supportsReturningKeys = supportsReturningKeys();
                    if (allowsNullForIdentityColumn()) {
                        if (supportsGetGeneratedKeys) {
                            ps = conn.prepareStatement(sql, new int[] { 1 });
                        } else if (supportsReturningKeys) {
                            ps = conn.prepareStatement(sql + " returning "
                                    + getSequenceKeyName(sequenceId));
                        } else {
                            ps = conn.prepareStatement(sql);
                        }
                    } else {
                        String replaceSql = sql.replaceFirst("\\(\\w*,", "(").replaceFirst(
                                "\\(null,", "(");
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

                    ResultSet rs = null;
                    if (supportsGetGeneratedKeys) {
                        ps.executeUpdate();
                        try {
                            rs = ps.getGeneratedKeys();
                            if (rs.next()) {
                                key = rs.getLong(1);
                            }
                        } finally {
                            JdbcUtils.closeResultSet(rs);
                        }
                    } else if (supportsReturningKeys) {
                        try {
                            rs = ps.executeQuery();
                            if (rs.next()) {
                                key = rs.getLong(1);
                            }
                        } finally {
                            JdbcUtils.closeResultSet(rs);
                        }
                    } else {
                        Statement st = null;
                        ps.executeUpdate();
                        try {
                            st = conn.createStatement();
                            rs = st.executeQuery(getSelectLastInsertIdSql(getSequenceName(sequenceId)));
                            if (rs.next()) {
                                key = rs.getLong(1);
                            }
                        } finally {
                            JdbcUtils.closeResultSet(rs);
                            JdbcUtils.closeStatement(st);
                        }
                    }
                } finally {
                    JdbcUtils.closeStatement(ps);
                }
                return key;
            }
        });
    }

    public Object createSavepoint(JdbcTemplate jdbcTemplate) {
        return jdbcTemplate.execute(new ConnectionCallback<Object>() {
            public Object doInConnection(Connection con) throws SQLException, DataAccessException {
                return con.setSavepoint();
            }
        });
    }

    public Object createSavepointForFallback(JdbcTemplate jdbcTemplate) {
        if (requiresSavepointForFallback()) {
            return createSavepoint(jdbcTemplate);
        }
        return null;
    }

    public void rollbackToSavepoint(JdbcTemplate jdbcTemplate, final Object savepoint) {
        if (savepoint != null && savepoint instanceof Savepoint) {
            jdbcTemplate.execute(new ConnectionCallback<Object>() {
                public Object doInConnection(Connection con) throws SQLException,
                        DataAccessException {
                    con.rollback((Savepoint) savepoint);
                    return null;
                }
            });
        }
    }

    public void releaseSavepoint(JdbcTemplate jdbcTemplate, final Object savepoint) {
        if (savepoint != null && savepoint instanceof Savepoint) {
            jdbcTemplate.execute(new ConnectionCallback<Object>() {
                public Object doInConnection(Connection con) throws SQLException,
                        DataAccessException {
                    con.releaseSavepoint((Savepoint) savepoint);
                    return null;
                }
            });
        }
    }

    public boolean requiresSavepointForFallback() {
        return false;
    }

    public void disableSyncTriggers(ISqlTransaction transaction) {
        disableSyncTriggers(transaction, null);
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

    public boolean isTransactionIdOverrideSupported() {
        return true;
    }

    public void setTriggerText(TriggerText triggerText) {
        this.triggerText = triggerText;
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

    public String getEngineName() {
        return parameterService.getString(ParameterConstants.ENGINE_NAME);
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
        this.log = LogFactory.getLog(parameterService);
    }

    public boolean supportsOpenCursorsAcrossCommit() {
        return true;
    }

    public String getInitialLoadTableAlias() {
        return "t";
    }

    public String preProcessTriggerSqlClause(String sqlClause) {
        return sqlClause;
    }

    public int getRouterDataPeekAheadCount() {
        return parameterService.getInt(ParameterConstants.ROUTING_PEEK_AHEAD_WINDOW);
    }

    public void truncateTable(String tableName) {
        String quote = platform.isDelimitedIdentifierModeOn() ? platform.getPlatformInfo()
                .getDelimiterToken() : "";
        boolean success = false;
        int tryCount = 5;
        while (!success && tryCount > 0) {
            try {
                jdbcTemplate.update("truncate table " + quote + tableName + quote);
                success = true;
            } catch (DataAccessException ex) {
                log.warn(ex);
                AppUtils.sleep(5000);
                tryCount--;
            }
        }
    }

    /*
     * @return the lobHandler.
     */
    public LobHandler getLobHandler() {
        return lobHandler;
    }

    /*
     * @param lobHandler The lobHandler to set.
     */
    public void setLobHandler(LobHandler lobHandler) {
        this.lobHandler = lobHandler;
    }

    public boolean areDatabaseTransactionsPendingSince(long time) {
        throw new UnsupportedOperationException();
    }

    public long getDatabaseTime() {
        try {
            String sql = "select current_timestamp from " + tablePrefix + "_node_identity";
            sql = FormatUtils.replaceTokens(sql, getSqlScriptReplacementTokens(), false);
            return jdbcTemplate.queryForObject(sql, java.util.Date.class).getTime();

        } catch (Exception ex) {
            log.error(ex);
            return System.currentTimeMillis();
        }
    }

    public String getSourceNodeExpression() {
        return null;
    }

    final public String getDataHasChangedCondition(Trigger trigger) {
        if (parameterService.is(ParameterConstants.TRIGGER_UPDATE_CAPTURE_CHANGED_DATA_ONLY)) {
            return getDbSpecificDataHasChangedCondition(trigger);
        } else {
            return Constants.ALWAYS_TRUE_CONDITION;
        }
    }

    protected String getDbSpecificDataHasChangedCondition(Trigger trigger) {
        return Constants.ALWAYS_TRUE_CONDITION;
    }

    public Map<String, String> getSqlScriptReplacementTokens() {
        return null;
    }

    public boolean needsToSelectLobData() {
        return false;
    }

    public boolean canGapsOccurInCapturedDataIds() {
        return true;
    }

    public String massageDataExtractionSql(String sql, Channel channel) {
        return sql;
    }

    public String getDriverName() {
        return driverName;
    }

    public String getDriverVersion() {
        return driverVersion;
    }

    public String scrubSql(String sql) {
        Map<String, String> replacementTokens = getSqlScriptReplacementTokens();
        if (replacementTokens != null) {
            return FormatUtils.replaceTokens(sql, replacementTokens, false).trim();
        } else {
            return sql;
        }
    }

    public StringBuilder scrubSql(StringBuilder sql) {
        Map<String, String> replacementTokens = getSqlScriptReplacementTokens();
        if (replacementTokens != null) {
            return new StringBuilder(scrubSql(sql.toString()));
        } else {
            return sql;
        }
    }

    public String massageForLob(String sql, Channel channel) {
        return sql;
    }

    public boolean escapesTemplatesForDatabaseInserts() {
        return false;
    }

    public String getMasterCollation() {
        return parameterService.getString(ParameterConstants.DB_MASTER_COLLATION, "");
    }

    public boolean supportsBatchUpdates() {
        return true;
    }

    public void cleanupTriggers() {
    }

    public void addDatabaseUpgradeListener(IDatabaseUpgradeListener listener) {
        databaseUpgradeListeners.add(listener);
    }

    /*
     * Override this method to configure a database specific LOB handler for
     * updates
     */
    protected void initLobHandler() {
    }
    
    public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}
}