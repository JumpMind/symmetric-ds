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
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.interbase.InterbaseDbDialect;
import org.jumpmind.symmetric.db.sybase.SybaseDbDialect;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.io.DatabaseIO;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.ForeignKey;
import org.jumpmind.symmetric.ddl.model.Index;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.platform.SqlBuilder;
import org.jumpmind.symmetric.ext.IDatabaseUpgradeListener;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.load.StatementBuilder;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.AppUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.lob.LobHandler;

/*
 * The abstract class for database dialects.
 */
abstract public class AbstractDbDialect implements IDbDialect {

    protected ILog log = LogFactory.getLog(getClass());

    public static final String REQUIRED_FIELD_NULL_SUBSTITUTE = " ";

    public static final String[] TIMESTAMP_PATTERNS = { "yyyy-MM-dd HH:mm:ss.S",
            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy-MM-dd" };

    public static final String[] TIME_PATTERNS = { "HH:mm:ss.S", "HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss.S", "yyyy-MM-dd HH:mm:ss" };

    public static final FastDateFormat JDBC_TIMESTAMP_FORMATTER = FastDateFormat
            .getInstance("yyyy-MM-dd hh:mm:ss.SSS");

    public static final int MAX_SYMMETRIC_SUPPORTED_TRIGGER_SIZE = 50;

    protected JdbcTemplate jdbcTemplate;

    protected Platform platform;

    protected DatabaseModel cachedModel = new DatabaseModel();

    protected long lastTimeCachedModelClearedInMs = System.currentTimeMillis();

    protected SqlTemplate sqlTemplate;

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

    protected String identifierQuoteString = "";

    protected Set<String> sqlKeywords;

    private String defaultSchema;

    protected LobHandler lobHandler;

    protected boolean supportsTransactionViews = false;

    protected int queryTimeoutInSeconds = 300;

    protected int[] primaryKeyViolationCodes;

    protected String[] primaryKeyViolationSqlStates;

    protected List<IDatabaseUpgradeListener> databaseUpgradeListeners = new ArrayList<IDatabaseUpgradeListener>();

    protected AbstractDbDialect() {
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
        ts.append(JDBC_TIMESTAMP_FORMATTER.format(time));
        ts.append("'}");
        return ts.toString();
    }

    public IColumnFilter newDatabaseColumnFilter() {
        return null;
    }

    public void allowIdentityInserts(JdbcTemplate jdbcTemplate, Table table) {
    }

    public void revertAllowIdentityInserts(JdbcTemplate jdbcTemplate, Table table) {
    }

    protected boolean allowsNullForIdentityColumn() {
        return true;
    }

    public boolean requiresAutoCommitFalseToSetFetchSize() {
        return false;
    }

    public void resetCachedTableModel() {
        synchronized (this.getClass()) {
            this.cachedModel.resetTableIndexCache();
            Table[] tables = this.cachedModel.getTables();
            if (tables != null) {
                for (Table table : tables) {
                    this.cachedModel.removeTable(table);
                }
            }
        }
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

    public void init(Platform pf, int queryTimeout, JdbcTemplate jdbcTemplate) {
        log.info("DbDialectInUse", this.getClass().getName());
        this.jdbcTemplate = jdbcTemplate;
        this.queryTimeoutInSeconds = queryTimeout;
        this.jdbcTemplate.setQueryTimeout(queryTimeout);
        this.platform = pf;
        this.identifierQuoteString = "\"";
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
        resetCachedTableModel();
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
        String[] functions = sqlTemplate.getFunctionsToInstall();
        for (int i = 0; i < functions.length; i++) {
            String funcName = tablePrefix + "_" + functions[i];
            if (jdbcTemplate.queryForInt(sqlTemplate.getFunctionInstalledSql(funcName,
                    getDefaultSchema())) == 0) {
                jdbcTemplate.update(sqlTemplate.getFunctionSql(functions[i], funcName,
                        getDefaultSchema()));
                log.info("FunctionInstalled", funcName);
            }
        }
    }

    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.NONE;
    }

    public boolean isDateOverrideToTimestamp() {
        return false;
    }

    abstract protected boolean doesTriggerExistOnPlatform(String catalogName, String schema,
            String tableName, String triggerName);

    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        return "null";
    }

    public String createInitialLoadSqlFor(Node node, TriggerRouter trigger, Table table,
            TriggerHistory triggerHistory, Channel channel) {
        return sqlTemplate.createInitalLoadSql(node, this, trigger, table, triggerHistory, channel)
                .trim();
    }

    public String createPurgeSqlFor(Node node, TriggerRouter triggerRouter) {
        return String.format(
                parameterService.getString(ParameterConstants.INITIAL_LOAD_DELETE_FIRST_SQL),
                triggerRouter.qualifiedTargetTableName());
    }

    public String createCsvDataSql(Trigger trigger, TriggerHistory triggerHistory, Channel channel,
            String whereClause) {
        return sqlTemplate.createCsvDataSql(
                this,
                trigger,
                triggerHistory,
                getTable(trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                        trigger.getSourceTableName(), true), channel, whereClause).trim();
    }

    public String createCsvPrimaryKeySql(Trigger trigger, TriggerHistory triggerHistory,
            Channel channel, String whereClause) {
        return sqlTemplate.createCsvPrimaryKeySql(
                this,
                trigger,
                triggerHistory,
                getTable(trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                        trigger.getSourceTableName(), true), channel, whereClause).trim();
    }

    public Table getTable(Trigger trigger, boolean useCache) {
        return getTable(trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                trigger.getSourceTableName(), useCache);
    }

    /*
     * This method uses the ddlutil's model reader which uses the jdbc metadata
     * to lookup up table metadata. <p/> Dialect may optionally override this
     * method to more efficiently lookup up table metadata directly against
     * information schemas.
     */
    public Table getTable(String catalogName, String schemaName, String tableName, boolean useCache) {
        if (System.currentTimeMillis() - lastTimeCachedModelClearedInMs > parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_TABLES_IN_MS)) {
            synchronized (this.getClass()) {
                cachedModel = new DatabaseModel();
                lastTimeCachedModelClearedInMs = System.currentTimeMillis();
            }
        }
        DatabaseModel model = cachedModel;
        Table retTable = model != null ? model.findTable(catalogName, schemaName, tableName) : null;
        if (retTable == null || !useCache) {
            synchronized (this.getClass()) {
                try {
                    Table table = getTable(catalogName, schemaName, tableName);

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

    /*
     * Returns a new {@link Table} object.
     */
    protected Table getTable(String catalogName, String schemaName, String tblName) {
        Table table = getTableCaseSensitive(catalogName, schemaName, tblName, true);

        try {
            if (table == null && parameterService.is(ParameterConstants.DB_METADATA_IGNORE_CASE)) {
                table = getTableCaseSensitive(StringUtils.upperCase(catalogName),
                        StringUtils.upperCase(schemaName), StringUtils.upperCase(tblName), true);
                if (table == null) {
                    table = getTableCaseSensitive(StringUtils.lowerCase(catalogName),
                            StringUtils.lowerCase(schemaName), StringUtils.lowerCase(tblName), true);
                    if (table == null) {
                        table = getTableCaseSensitive(catalogName, schemaName,
                                StringUtils.upperCase(tblName), true);
                        if (table == null) {
                            table = getTableCaseSensitive(catalogName, schemaName,
                                    StringUtils.lowerCase(tblName), true);
                            if (table == null) {
                                table = getTableCaseSensitive(catalogName, schemaName, tblName,
                                        true);
                            }
                        }
                    }
                }
            }
        } catch (UncategorizedSQLException e) {
            if (this instanceof SybaseDbDialect && e.getCause() instanceof SQLException
                    && ((SQLException) e.getCause()).getErrorCode() == 911) {
                // Sybase is case sensitive and it will throw an exception
                // if you ask for a catalog or schema that does not exist
            } else {
                throw e;
            }
        }
        return table;
    }

    protected Table getTableCaseSensitive(String catalogName, String schemaName,
            final String tablename, final boolean massageTableForChangeDataCapture) {
        Table table = null;
        try {
            // If we don't provide a default schema or catalog, then on some
            // databases multiple results will be found in the metadata from
            // multiple schemas/catalogs
            final String schema = StringUtils.isBlank(schemaName) ? getDefaultSchema() : schemaName;
            final String catalog = StringUtils.isBlank(catalogName) ? getDefaultCatalog()
                    : catalogName;
            table = (Table) jdbcTemplate.execute(new ConnectionCallback<Table>() {
                public Table doInConnection(Connection c) throws SQLException, DataAccessException {
                    Table table = platform.readTableFromDatabase(c, catalog, schema, tablename);
                    if (table != null && massageTableForChangeDataCapture) {
                        boolean treatDateTimeFieldsAsVarchar = parameterService.is(
                                ParameterConstants.DB_TREAT_DATE_TIME_AS_VARCHAR, false);
                        Column[] columns = table.getColumns();
                        for (Column column : columns) {
                            int typeCode = column.getTypeCode();
                            if (treatDateTimeFieldsAsVarchar
                                    && (typeCode == Types.DATE || typeCode == Types.TIME || typeCode == Types.TIMESTAMP)) {
                                typeCode = Types.VARCHAR;
                            }
                            column.setTypeCode(typeCode);
                        }
                    }
                    return table;
                }
            });
        } catch (DataAccessResourceFailureException ex) {
            log.debug("Message", ex.getMessage());
        } catch (TransientDataAccessResourceException ex) {
            log.debug("Message", ex.getMessage());
        }

        return table;
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
                String defaultCatalog = getDefaultCatalog();
                String defaultSchema = getDefaultSchema();
                try {
                    previousCatalog = switchCatalogForTriggerInstall(sourceCatalogName, con);

                    String triggerSql = sqlTemplate.createTriggerDDL(AbstractDbDialect.this, dml,
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
        return sqlTemplate.createPostTriggerDDL(this, dml, trigger, hist, channel, tablePrefix,
                table, getDefaultCatalog(), getDefaultSchema());
    }

    public String getCreateSymmetricDDL() {
        Database db = readSymmetricSchemaFromXml();
        prefixConfigDatabase(db);
        return platform.getCreateTablesSql(db, true, true);
    }

    public String getCreateTableSQL(TriggerRouter trig) {
        Table table = getTable(null, trig.getTrigger().getSourceSchemaName(), trig.getTrigger()
                .getSourceTableName(), true);
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

    private void setDatabaseName(TriggerRouter triggerRouter, Database db) {
        db.setName(triggerRouter.getTargetSchema(getDefaultSchema()));
        if (db.getName() == null) {
            db.setName(getDefaultCatalog());
        }
        if (db.getName() == null) {
            db.setName("DDL");
        }
    }

    public String getCreateTableXML(TriggerRouter triggerRouter) {
        Table table = getTable(null, triggerRouter.getTrigger().getSourceSchemaName(),
                triggerRouter.getTrigger().getSourceTableName());
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
        platform.createTables(db, true, true);
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
                if (getTable(getDefaultCatalog(), getDefaultSchema(), table.getName(), false) == null) {
                    createTables = true;
                }
            }

            return createTables;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public Database readPlatformDatabase(boolean includeSymmetricTables) {
        String schema = getDefaultSchema();
        String catalog = getDefaultCatalog();
        Database database = platform.readModelFromDatabase(!StringUtils.isBlank(schema) ? schema
                : (!StringUtils.isBlank(catalog) ? catalog : "database"), catalog, schema, null);
        if (!includeSymmetricTables) {
            Database symmetricTables = readSymmetricSchemaFromXml();
            Table[] tables = symmetricTables.getTables();
            for (Table symTable : tables) {
                for (Table table : database.getTables()) {
                    if (table.getName().equalsIgnoreCase(symTable.getName())) {
                        database.removeTable(table);
                    }
                }
            }

            Table[] allTables = database.getTables();
            for (Table table : allTables) {
                // Remove SYM_ON_ trigger tables for embedded databases
                if (table.getName().startsWith(tablePrefix.toUpperCase() + "_ON_")
                        || table.getName().equalsIgnoreCase(
                                tablePrefix + "_" + InterbaseDbDialect.CONTEXT_TABLE_NAME)) {
                    database.removeTable(table);
                }
            }
        }

        return database;
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
                Table tableFromDatabase = getTableCaseSensitive(getDefaultCatalog(),
                        getDefaultSchema(), tableFromXml.getName(), false);
                if (tableFromDatabase != null) {
                    modelFromDatabase.addTable(tableFromDatabase);
                }
            }

            SqlBuilder builder = platform.getSqlBuilder();

            if (builder.isAlterDatabase(modelFromDatabase, modelFromXml, null)) {
                log.info("TablesAutoUpdatingFoundTablesToAlter");
                DataSource ds = jdbcTemplate.getDataSource();
                String delimiter = platform.getPlatformInfo().getSqlCommandDelimiter();

                for (IDatabaseUpgradeListener listener : databaseUpgradeListeners) {
                    String sql = listener.beforeUpgrade(this, tablePrefix, modelFromDatabase,
                            modelFromXml);
                    new SqlScript(sql, ds, true, delimiter, null).execute();
                }

                StringWriter writer = new StringWriter();
                builder.setWriter(writer);
                builder.alterDatabase(modelFromDatabase, modelFromXml, null);
                String alterSql = writer.toString();
                if (log.isDebugEnabled()) {
                    log.debug("TablesAutoUpdatingAlterSql", alterSql);
                }
                new SqlScript(alterSql, ds, true, delimiter, null).execute();

                for (IDatabaseUpgradeListener listener : databaseUpgradeListeners) {
                    String sql = listener.afterUpgrade(this, tablePrefix, modelFromXml);
                    new SqlScript(sql, ds, true, delimiter, null).execute();
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

    protected String getAlterSql(Database databaseTables) throws IOException {
        Database currentModel = new Database();
        Table[] tables = databaseTables.getTables();
        Database existingModel = readPlatformDatabase(true);
        for (Table table : tables) {
            Table currentVersion = existingModel.findTable(table.getName());
            if (currentVersion != null) {
                currentModel.addTable(currentVersion);
            }
        }
        SqlBuilder builder = platform.getSqlBuilder();
        StringWriter writer = new StringWriter();
        builder.setWriter(writer);
        builder.alterDatabase(currentModel, databaseTables, null);
        return writer.toString();
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
        URL url = AbstractDbDialect.class.getResource(resourceName);
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
        Index[] indexes = table.getIndices();
        if (indexes != null) {
            for (Index index : indexes) {
                String prefixedName = tablePrefix + index.getName();
                index.setName(prefixedName);
            }
        }
    }

    public Platform getPlatform() {
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

    public Object[] getObjectValues(BinaryEncoding encoding, Table table, String[] columnNames,
            String[] values) {
        Column[] metaData = orderColumns(columnNames, table);
        return getObjectValues(encoding, values, metaData);
    }

    public Object[] getObjectValues(BinaryEncoding encoding, String[] values,
            Column[] orderedMetaData) {
        List<Object> list = new ArrayList<Object>(values.length);
        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            Object objectValue = value;
            Column column = orderedMetaData[i];
            try {
                if (column != null) {
                    int type = column.getTypeCode();
                    if ((value == null || (isEmptyStringNulled() && value.equals("")))
                            && column.isRequired() && column.isOfTextType()) {
                        objectValue = REQUIRED_FIELD_NULL_SUBSTITUTE;
                    }
                    if (value != null) {
                        if (type == Types.DATE && !isDateOverrideToTimestamp()) {
                            objectValue = getDate(value, TIMESTAMP_PATTERNS);
                        } else if (type == Types.TIMESTAMP
                                || (type == Types.DATE && isDateOverrideToTimestamp())) {
                            objectValue = getTime(value, TIMESTAMP_PATTERNS);                            
                        } else if (type == Types.CHAR) {
                            String charValue = value.toString();
                            if ((StringUtils.isBlank(charValue) && isBlankCharColumnSpacePadded())
                                    || (StringUtils.isNotBlank(charValue) && isNonBlankCharColumnSpacePadded())) {
                                objectValue = StringUtils.rightPad(value.toString(),
                                        column.getSizeAsInt(), ' ');
                            }
                        } else if (type == Types.INTEGER || type == Types.SMALLINT
                                || type == Types.BIT) {
                            objectValue = Integer.valueOf(value);
                        } else if (type == Types.NUMERIC || type == Types.DECIMAL
                                || type == Types.FLOAT || type == Types.DOUBLE) {
                            // The number will have either one period or one
                            // comma
                            // for the decimal point, but we need a period
                            objectValue = new BigDecimal(value.replace(',', '.'));
                        } else if (type == Types.BOOLEAN) {
                            objectValue = value.equals("1") ? Boolean.TRUE : Boolean.FALSE;
                        } else if (type == Types.BLOB || type == Types.LONGVARBINARY
                                || type == Types.BINARY || type == Types.VARBINARY ||
                                // SQLServer ntext type
                                type == -10) {
                            if (encoding == BinaryEncoding.NONE) {
                                objectValue = value.getBytes();
                            } else if (encoding == BinaryEncoding.BASE64) {
                                objectValue = Base64.decodeBase64(value.getBytes());
                            } else if (encoding == BinaryEncoding.HEX) {
                                objectValue = Hex.decodeHex(value.toCharArray());
                            }
                        } else if (type == Types.TIME) {
                            objectValue = new Time(getDate(value, TIME_PATTERNS).getTime());
                        } else if (type == Types.ARRAY) {
                            objectValue = createArray(column, value);
                        }
                    }
                    if (objectValue instanceof String) {
                        objectValue = cleanTextForTextBasedColumns((String) objectValue);
                    }
                    list.add(objectValue);
                }
            } catch (Exception ex) {
                log.error("DbDialectTroubleConvertingColumnValue", value, column.getName(),
                        column.getType());
                throw new RuntimeException(ex);
            }
        }

        return list.toArray();
    }

    protected String cleanTextForTextBasedColumns(String text) {
        return text;
    }

    protected Array createArray(Column column, final String value) {
        return null;
    }

    public boolean isBlankCharColumnSpacePadded() {
        return isNonBlankCharColumnSpacePadded();
    }

    final private java.util.Date getDate(String value, String[] pattern) {
        try {
            return DateUtils.parseDate(value, pattern);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    final private Timestamp getTime(String value, String[] pattern) {
    	try {
    		return Timestamp.valueOf(value);
    	} catch (Exception ex) {
    		return new Timestamp(getDate(value, pattern).getTime());	
    	}        
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

    public void disableSyncTriggers(JdbcTemplate jdbcTemplate) {
        disableSyncTriggers(jdbcTemplate, null);
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

    public void setSqlTemplate(SqlTemplate sqlTemplate) {
        this.sqlTemplate = sqlTemplate;
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

    public String getIdentifierQuoteString() {
        return identifierQuoteString;
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

    /*
     * Returns the current schema name
     * 
     * @return String
     */
    public String getDefaultSchema() {
        return StringUtils.isBlank(defaultSchema) ? null : defaultSchema;
    }

    /*
     * Sets the current schema name from properties file
     * 
     * @param currentSchema
     */
    public void setDefaultSchema(String currentSchema) {
        this.defaultSchema = currentSchema;
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
            sql = AppUtils.replaceTokens(sql, getSqlScriptReplacementTokens(), false);
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

    public boolean isClob(int type) {
        return type == Types.CLOB || type == Types.LONGVARCHAR;
    }

    public boolean isBlob(int type) {
        return type == Types.BLOB || type == Types.BINARY || type == Types.VARBINARY
                || type == Types.LONGVARBINARY || type == -10;
    }

    public boolean isLob(int type) {
        return type == Types.CLOB || type == Types.BLOB || type == Types.BINARY
                || type == Types.VARBINARY || type == Types.LONGVARBINARY
                || type == Types.LONGNVARCHAR ||
                // SQL-Server ntext binary type
                type == -10;
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
            return AppUtils.replaceTokens(sql, replacementTokens, false).trim();
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

    public StatementBuilder createStatementBuilder(DmlType type, String tableName, Column[] keys,
            Column[] columns, Column[] preFilteredColumns) {
        return new StatementBuilder(type, tableName, keys, columns, preFilteredColumns,
                isDateOverrideToTimestamp(), getIdentifierQuoteString());
    }

    public void setPrimaryKeyViolationCodes(int[] primaryKeyViolationCodes) {
        this.primaryKeyViolationCodes = primaryKeyViolationCodes;
    }

    public void setPrimaryKeyViolationSqlStates(String[] primaryKeyViolationSqlStates) {
        this.primaryKeyViolationSqlStates = primaryKeyViolationSqlStates;
    }

    public boolean isPrimaryKeyViolation(Exception ex) {
        boolean primaryKeyViolation = false;
        if (primaryKeyViolationCodes != null || primaryKeyViolationSqlStates != null) {
            SQLException sqlEx = findSQLException(ex);
            if (sqlEx != null) {
                if (primaryKeyViolationCodes != null) {
                    int errorCode = sqlEx.getErrorCode();
                    for (int primaryKeyViolationCode : primaryKeyViolationCodes) {
                        if (primaryKeyViolationCode == errorCode) {
                            primaryKeyViolation = true;
                            break;
                        }
                    }
                }

                if (primaryKeyViolationSqlStates != null) {
                    String sqlState = sqlEx.getSQLState();
                    if (sqlState != null) {
                        for (String primaryKeyViolationSqlState : primaryKeyViolationSqlStates) {
                            if (primaryKeyViolationSqlState != null
                                    && primaryKeyViolationSqlState.equals(sqlState)) {
                                primaryKeyViolation = true;
                                break;
                            }
                        }
                    }
                }
            }
        }

        return primaryKeyViolation;
    }

    protected SQLException findSQLException(Throwable ex) {
        if (ex instanceof SQLException) {
            return (SQLException) ex;
        } else {
            Throwable cause = ex.getCause();
            if (cause != null && !cause.equals(ex)) {
                return findSQLException(cause);
            }
        }
        return null;
    }
    
    public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}
    
}