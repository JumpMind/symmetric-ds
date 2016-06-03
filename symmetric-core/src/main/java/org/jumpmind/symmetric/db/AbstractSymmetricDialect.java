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
package org.jumpmind.symmetric.db;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IAlterDatabaseInterceptor;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.ISqlResultsListener;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.LogSqlResultsListener;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.ext.IDatabaseUpgradeListener;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.impl.TransformService.TransformTableNodeGroupLink;
import org.jumpmind.util.AppUtils;
import org.jumpmind.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * The abstract class for database dialects.
 */
abstract public class AbstractSymmetricDialect implements ISymmetricDialect {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    public static final int MAX_SYMMETRIC_SUPPORTED_TRIGGER_SIZE = 50;

    protected IDatabasePlatform platform;

    protected AbstractTriggerTemplate triggerTemplate;

    protected IParameterService parameterService;

    protected IExtensionService extensionService;

    protected Boolean supportsGetGeneratedKeys;

    protected String databaseName;

    protected String driverVersion;

    protected String driverName;

    protected int databaseMajorVersion;

    protected int databaseMinorVersion;

    protected String databaseProductVersion;

    protected Set<String> sqlKeywords;

    protected boolean supportsTransactionViews = false;
    
    protected boolean supportsSubselectsInDelete = true;
    
    protected boolean supportsSubselectsInUpdate = true;
    
    protected Map<String,String> sqlReplacementTokens = new HashMap<String, String>();

    public AbstractSymmetricDialect() {
    }
    
    public AbstractSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        this.parameterService = parameterService;
        this.platform = platform;

        log.info("The DbDialect being used is {}", this.getClass().getName());

        buildSqlReplacementTokens();
        
        ISqlTemplate sqlTemplate = this.platform.getSqlTemplate();
        this.databaseMajorVersion = sqlTemplate.getDatabaseMajorVersion();
        this.databaseMinorVersion = sqlTemplate.getDatabaseMinorVersion();
        this.databaseName = sqlTemplate.getDatabaseProductName();
        this.databaseProductVersion = sqlTemplate.getDatabaseProductVersion();
        this.driverName = sqlTemplate.getDriverName();
        this.driverVersion = sqlTemplate.getDriverVersion();
                
    }

    public boolean requiresAutoCommitFalseToSetFetchSize() {
        return false;
    }
    
    protected void buildSqlReplacementTokens() {
        sqlReplacementTokens.put("selectDataUsingGapsSqlHint", "");
    }
    
    public Map<String, String> getSqlReplacementTokens() {
        return sqlReplacementTokens;
    }

    /*
     * Provide a default implementation of this method using DDLUtils,
     * getMaxColumnNameLength()
     */
    public int getMaxTriggerNameLength() {
        int max = getPlatform().getDatabaseInfo().getMaxColumnNameLength();
        return max < MAX_SYMMETRIC_SUPPORTED_TRIGGER_SIZE && max > 0 ? max
                : MAX_SYMMETRIC_SUPPORTED_TRIGGER_SIZE;
    }
    
    public void verifyDatabaseIsCompatible() {
    }

    public void initTablesAndDatabaseObjects() {
        createOrAlterTablesIfNecessary();
        createRequiredDatabaseObjects();
        platform.resetCachedTableModel();
    }
    
    protected String replaceTokens(String sql, String objectName) {
        String ddl = FormatUtils.replace("functionName", objectName, sql);
        ddl = FormatUtils.replace("version", Version.versionWithUnderscores(), ddl);
        ddl = FormatUtils.replace("defaultSchema", platform.getDefaultSchema(), ddl);
        return ddl;        
    }
    
    protected boolean installed(String sql, String objectName) {
        return platform.getSqlTemplate().queryForInt(replaceTokens(sql, objectName)) > 0;
    }
    
    protected void install(String sql, String objectName) {
        sql = replaceTokens(sql, objectName);
        log.info("Installing SymmetricDS database object:\n{}", sql);
        platform.getSqlTemplate().update(sql);
        log.info("Just installed {}", objectName);
    }
    
    protected void uninstall(String sql, String objectName) {
        sql = replaceTokens(sql, objectName);
        platform.getSqlTemplate().update(sql);
        log.info("Just uninstalled {}", objectName);
    }    
    
    public void dropTablesAndDatabaseObjects() {
        Database modelFromDatabase = readSymmetricSchemaFromDatabase(); 
        platform.dropDatabase(modelFromDatabase, true);
        dropRequiredDatabaseObjects();        
    }

    final public boolean doesTriggerExist(String catalogName, String schema, String tableName,
            String triggerName) {
        if (StringUtils.isNotBlank(triggerName)) {
            try {
                return doesTriggerExistOnPlatform(catalogName, schema, tableName, triggerName);
            } catch (Exception ex) {
                log.warn("Could not figure out if the trigger exists.  Assuming that is does not",
                        ex);
                return false;
            }
        } else {
            return false;
        }
    }
    
    public abstract void dropRequiredDatabaseObjects();
    
    public abstract void createRequiredDatabaseObjects();

    abstract public BinaryEncoding getBinaryEncoding();

    abstract protected boolean doesTriggerExistOnPlatform(String catalogName, String schema,
            String tableName, String triggerName);

    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        return "null";
    }

    public String createInitialLoadSqlFor(Node node, TriggerRouter trigger, Table table,
            TriggerHistory triggerHistory, Channel channel, String overrideSelectSql) {
        return triggerTemplate.createInitalLoadSql(node, trigger, table, triggerHistory, channel, overrideSelectSql)
                .trim();
    }
    

    public String createPurgeSqlFor(Node node, TriggerRouter triggerRouter, TriggerHistory triggerHistory) {
        return createPurgeSqlFor(node, triggerRouter, triggerHistory, null);
    }

    @Override
    public String createPurgeSqlFor(Node node, TriggerRouter triggerRouter, TriggerHistory triggerHistory, List<TransformTableNodeGroupLink> transforms) {
        String sql = null;        	    	
        if (StringUtils.isEmpty(triggerRouter.getInitialLoadDeleteStmt())) {
            List<String> tableNames = new ArrayList<String>();
            if (transforms != null) {
                for (TransformTableNodeGroupLink transform : transforms) {
                    tableNames.add(transform.getFullyQualifiedTargetTableName());
                }
            } else {
                tableNames.add(triggerRouter.qualifiedTargetTableName(triggerHistory));
            }
            
            StringBuilder statements = new StringBuilder(128);
            
            for (String tableName : tableNames) {
                statements.append(String.format(
                        parameterService.getString(ParameterConstants.INITIAL_LOAD_DELETE_FIRST_SQL),
                        tableName)).append(";");
            }
            
            statements.setLength(statements.length()-1); // Lose the last ;
            sql = statements.toString();
        } else {
            sql = triggerRouter.getInitialLoadDeleteStmt();
        }
        return sql;
    }
    
    public String createCsvDataSql(Trigger trigger, TriggerHistory triggerHistory, Channel channel,
            String whereClause) {
        return triggerTemplate.createCsvDataSql(
                trigger,
                triggerHistory,
                platform.getTableFromCache(trigger.getSourceCatalogName(),
                        trigger.getSourceSchemaName(), trigger.getSourceTableName(), false),
                channel, whereClause).trim();
    }

    public String createCsvPrimaryKeySql(Trigger trigger, TriggerHistory triggerHistory,
            Channel channel, String whereClause) {
        return triggerTemplate.createCsvPrimaryKeySql(
                trigger,
                triggerHistory,
                platform.getTableFromCache(trigger.getSourceCatalogName(),
                        trigger.getSourceSchemaName(), trigger.getSourceTableName(), false),
                channel, whereClause).trim();
    }

    public Set<String> getSqlKeywords() {
        if (sqlKeywords == null) {
            this.sqlKeywords = this.platform.getSqlTemplate().getSqlKeywords();
        }
        return sqlKeywords;
    }
    
    protected String getDropTriggerSql(StringBuilder sqlBuffer, String catalogName, String schemaName,
            String triggerName, String tableName) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        return "drop trigger " + schemaName + triggerName;
    }

    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName,
            String triggerName, String tableName) {
        String sql = getDropTriggerSql(sqlBuffer, catalogName, schemaName, triggerName, tableName);
        logSql(sql, sqlBuffer);
        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            try {
                this.platform.getSqlTemplate().update(sql);
            } catch (Exception e) {
                log.warn("Tried to remove trigger using: {} and failed because: {}", sql, e.getMessage());
            }
        }
    }

    final protected void logSql(String sql, StringBuilder sqlBuffer) {
        if (sqlBuffer != null && StringUtils.isNotBlank(sql)) {
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
        log.info("Creating {} trigger for {}", hist.getTriggerNameForDmlType(dml),
                table.getFullyQualifiedTableName());

        String previousCatalog = null;
        String sourceCatalogName = table.getCatalog();
        String defaultCatalog = platform.getDefaultCatalog();
        String defaultSchema = platform.getDefaultSchema();

        String triggerSql = triggerTemplate.createTriggerDDL(dml, trigger, hist, channel,
                tablePrefix, table, defaultCatalog, defaultSchema);

        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            ISqlTransaction transaction = null;
            try {
                transaction = this.platform.getSqlTemplate().startSqlTransaction(
                        platform.getDatabaseInfo().isRequiresAutoCommitForDdl());
                previousCatalog = switchCatalogForTriggerInstall(sourceCatalogName, transaction);

                try {
                    log.debug("Running: {}", triggerSql);
                    logSql(triggerSql, sqlBuffer);
                    transaction.execute(triggerSql);
                } catch (SqlException ex) {
                    log.info("Failed to create trigger: {}", triggerSql);
                    throw ex;
                }

                postCreateTrigger(transaction, sqlBuffer, dml, trigger, hist, channel, tablePrefix, table);
                transaction.commit();
            } catch (SqlException ex) {
                transaction.rollback();
                throw ex;
            } finally {
                try {
                    if (sourceCatalogName != null
                            && !sourceCatalogName.equalsIgnoreCase(previousCatalog)) {
                        switchCatalogForTriggerInstall(previousCatalog, transaction);
                    }
                } finally {
                    transaction.close();
                }

            }
        }
    }

    protected void postCreateTrigger(ISqlTransaction transaction, StringBuilder sqlBuffer, DataEventType dml,
            Trigger trigger, TriggerHistory hist, Channel channel, String tablePrefix, Table table) {
        String postTriggerDml = createPostTriggerDDL(dml, trigger, hist, channel, tablePrefix, table);
        if (StringUtils.isNotBlank(postTriggerDml)) {
            try {
                log.debug("Running: {}", postTriggerDml);
                logSql(postTriggerDml, sqlBuffer);
                transaction.execute(postTriggerDml);
            } catch (SqlException ex) {
                log.info("Failed to create post trigger: {}", postTriggerDml);
                throw ex;
            }
        }
    }

    /*
     * Provide the option switch a connection's schema for trigger installation.
     */
    protected String switchCatalogForTriggerInstall(String catalog, ISqlTransaction transaction) {
        return null;
    }

    protected String createPostTriggerDDL(DataEventType dml, Trigger trigger, TriggerHistory hist,
            Channel channel, String tablePrefix, Table table) {
        return triggerTemplate.createPostTriggerDDL(dml, trigger, hist, channel, tablePrefix,
                table, platform.getDefaultCatalog(), platform.getDefaultSchema());
    }

    public String getCreateSymmetricDDL() {
        Database database = readSymmetricSchemaFromXml();
        prefixConfigDatabase(database);
        IDdlBuilder builder = platform.getDdlBuilder();
        return builder.createTables(database, true);
    }

    protected void prefixConfigDatabase(Database targetTables) {
        platform.prefixDatabase(parameterService.getTablePrefix(), targetTables);
    }

    public Table getTable(TriggerHistory triggerHistory, boolean useCache) {
        if (triggerHistory != null) {
            return platform.getTableFromCache(triggerHistory.getSourceCatalogName(),
                    triggerHistory.getSourceSchemaName(), triggerHistory.getSourceTableName(),
                    !useCache);
        } else {
            return null;
        }
    }
    
    /*
     * @return true if SQL was executed.
     */
    public boolean createOrAlterTablesIfNecessary(String... tableNames) {
        try {            
            log.info("Checking if SymmetricDS tables need created or altered");
            
            Database modelFromXml = readSymmetricSchemaFromXml();
            Database modelFromDatabase = readSymmetricSchemaFromDatabase();

            if (tableNames != null && tableNames.length > 0) {
                tableNames = platform.alterCaseToMatchDatabaseDefaultCase(tableNames);
                modelFromXml.removeAllTablesExcept(tableNames);
                modelFromDatabase.removeAllTablesExcept(tableNames);
            }

            IDdlBuilder builder = platform.getDdlBuilder();
            
            List<IAlterDatabaseInterceptor> alterDatabaseInterceptors = extensionService.getExtensionPointList(IAlterDatabaseInterceptor.class);
            IAlterDatabaseInterceptor[] interceptors = alterDatabaseInterceptors.toArray(new IAlterDatabaseInterceptor[alterDatabaseInterceptors.size()]);
            if (builder.isAlterDatabase(modelFromDatabase, modelFromXml, interceptors)) {
                String delimiter = platform.getDatabaseInfo().getSqlCommandDelimiter();

                ISqlResultsListener resultsListener = new LogSqlResultsListener(log);
                List<IDatabaseUpgradeListener> databaseUpgradeListeners = extensionService
                        .getExtensionPointList(IDatabaseUpgradeListener.class);

                String alterSql = builder.alterDatabase(modelFromDatabase, modelFromXml,
                        interceptors);
                if (isNotBlank(alterSql)) {
                    log.info("There are SymmetricDS tables that needed altered");

                    for (IDatabaseUpgradeListener listener : databaseUpgradeListeners) {
                        String sql = listener.beforeUpgrade(this,
                                this.parameterService.getTablePrefix(), modelFromDatabase,
                                modelFromXml);
                        SqlScript script = new SqlScript(sql, getPlatform().getSqlTemplate(), true,
                                false, false, delimiter, null);
                        script.setListener(resultsListener);
                        script.execute(platform.getDatabaseInfo().isRequiresAutoCommitForDdl());
                    }

                    log.debug("Alter SQL generated: {}", alterSql);

                    SqlScript script = new SqlScript(alterSql, getPlatform().getSqlTemplate(),
                            true, false, false, delimiter, null);
                    script.setListener(resultsListener);
                    script.execute(platform.getDatabaseInfo().isRequiresAutoCommitForDdl());

                    for (IDatabaseUpgradeListener listener : databaseUpgradeListeners) {
                        String sql = listener.afterUpgrade(this,
                                this.parameterService.getTablePrefix(), modelFromXml);
                        script = new SqlScript(sql, getPlatform().getSqlTemplate(), true, false,
                                false, delimiter, null);
                        script.setListener(resultsListener);
                        script.execute(platform.getDatabaseInfo().isRequiresAutoCommitForDdl());
                    }

                    log.info("Done with auto update of SymmetricDS tables");
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public Database readSymmetricSchemaFromXml() {
        try {
            Database database = merge(readDatabaseFromXml("/symmetric-schema.xml"),
                    readDatabaseFromXml("/console-schema.xml"));
            prefixConfigDatabase(database);
            
            String extraTablesXml = parameterService
                    .getString(ParameterConstants.AUTO_CONFIGURE_EXTRA_TABLES);
            if (StringUtils.isNotBlank(extraTablesXml)) {
                try {
                    database = merge(database, readDatabaseFromXml(extraTablesXml));
                } catch (Exception ex) {
                    log.error("", ex);
                }
            }
            
            return database;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public Database readSymmetricSchemaFromDatabase() {
        return platform.readFromDatabase(readSymmetricSchemaFromXml().getTables());
    }

    protected Database readDatabaseFromXml(String resourceName) throws IOException {
        try {
            return platform.readDatabaseFromXml(resourceName, true);
        } catch (IoException ex) {
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

    public boolean supportsTransactionViews() {
        return supportsTransactionViews;
    }
    
    /*
     * Indicates if this dialect supports subselects in delete statements.
     */
    public boolean supportsSubselectsInDelete() {
        return supportsSubselectsInDelete;
    }
    
    /*
     * Indicates if this dialect supports subselects in update statements.
     */
    public boolean supportsSubselectsInUpdate() {
        return supportsSubselectsInUpdate;
    }

    public long insertWithGeneratedKey(String sql, SequenceIdentifier sequenceId) {
        return insertWithGeneratedKey(sql, sequenceId, null, null);
    }

    public long insertWithGeneratedKey(final String sql, final SequenceIdentifier identifier,
            Object... args) {
        return platform.getSqlTemplate().insertWithGeneratedKey(sql,
                getSequenceKeyName(identifier), getSequenceKeyName(identifier), args, null);
    }

    public String getSequenceName(SequenceIdentifier identifier) {
        switch (identifier) {
            case REQUEST:
                return "sym_extract_r_st_request_id";
            case DATA:
                return "sym_data_data_id";
            case TRIGGER_HIST:
                return "sym_trigger_his_ger_hist_id";
        }
        return null;
    }

    public String getSequenceKeyName(SequenceIdentifier identifier) {
        switch (identifier) {
            case REQUEST:
                return "request_id";
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

    public String getEngineName() {
        return parameterService.getString(ParameterConstants.ENGINE_NAME);
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

    public void truncateTable(String tableName) {
	   String quote = platform.getDdlBuilder().isDelimitedIdentifierModeOn() ? platform
               .getDatabaseInfo().getDelimiterToken() : "";
        boolean success = false;
        int tryCount = 5;
        while (!success && tryCount > 0) {
            try {
                Table table = platform.getTableFromCache(tableName, false);
                if (table != null) {
                    platform.getSqlTemplate().update(
                    		String.format("truncate table %s%s%s", quote, table.getName(), quote));
                    success = true;
                } else {
                    throw new RuntimeException(String.format("Could not find %s to trunate",
                            tableName));
                }
            } catch (SqlException ex) {
                log.warn("", ex);
                AppUtils.sleep(5000);
                tryCount--;
            }
        }
    }

    public boolean areDatabaseTransactionsPendingSince(long time) {
        throw new UnsupportedOperationException();
    }

    public Date getEarliestTransactionStartTime() {
        throw new UnsupportedOperationException();
    }

    public long getDatabaseTime() {
        try {
            String sql = "select current_timestamp from " + this.parameterService.getTablePrefix()
                    + "_node_identity";
            sql = FormatUtils.replaceTokens(sql, platform.getSqlScriptReplacementTokens(), false);
            Date dateTime = this.platform.getSqlTemplate()
                    .queryForObject(sql, java.util.Date.class);
            if (dateTime != null) {
                return dateTime.getTime();
            } else {
                return System.currentTimeMillis();
            }
        } catch (Exception ex) {
            log.error("", ex);
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

    public boolean needsToSelectLobData() {
        return false;
    }

    public boolean canGapsOccurInCapturedDataIds() {
        return true;
    }

    public String massageDataExtractionSql(String sql, Channel channel) {
        String textColumnExpression = parameterService.getString(ParameterConstants.DATA_EXTRACTOR_TEXT_COLUMN_EXPRESSION);
        if (isNotBlank(textColumnExpression)) {
            sql = sql.replace("d.old_data", textColumnExpression.replace("$(columnName)", "d.old_data"));
            sql = sql.replace("d.row_data", textColumnExpression.replace("$(columnName)", "d.row_data"));
            sql = sql.replace("d.pk_data", textColumnExpression.replace("$(columnName)", "d.pk_data"));
        }
        return sql;
    }

    public String getDriverName() {
        return driverName;
    }

    public String getDriverVersion() {
        return driverVersion;
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

    public AbstractTriggerTemplate getTriggerTemplate() {
        return triggerTemplate;
    }

    protected void close(ISqlTransaction transaction) {
        if (transaction != null) {
            transaction.close();
        }
    }

    public String getTablePrefix() {
        return parameterService.getTablePrefix();
    }
    
    public String getTemplateNumberPrecisionSpec() {
        return null;
    }
    
    public int getSqlTypeForIds() {
        return Types.NUMERIC;
    }
    
    @Override
    public IParameterService getParameterService() {
        return parameterService;
    }
    
    public void setExtensionService(IExtensionService extensionService) {
        this.extensionService = extensionService;
    }
}
