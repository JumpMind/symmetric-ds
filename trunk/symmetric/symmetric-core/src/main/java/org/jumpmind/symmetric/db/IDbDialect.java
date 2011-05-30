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
 * under the License.  */
package org.jumpmind.symmetric.db;

import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ext.IDatabaseUpgradeListener;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.support.lob.LobHandler;

/**
 * A dialect is the interface that insulates SymmetricDS from database implementation specifics. 
 */
public interface IDbDialect {

    public void createTrigger(StringBuilder sqlBuffer, DataEventType dml, 
            Trigger trigger, TriggerHistory hist, Channel channel, 
            String tablePrefix, Table table);

    /**
     * Get the name of this symmetric instance. This can be set in
     * symmetric.properties using the symmetric.runtime.engine.name property.
     */
    public String getEngineName();

    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName, String triggerName,
            String tableName, TriggerHistory oldHistory);

    public boolean doesTriggerExist(String catalogName, String schema, String tableName, String triggerName);

    /**
     * This is called by the data loader each time the table context changes,
     * giving the dialect an opportunity to do any pre loading work. Only one
     * table is active at any one point.
     */
    public void prepareTableForDataLoad(JdbcTemplate jdbcTemplate, Table table);

    /**
     * This is called by the data loader each time the table context changes
     * away from a table or when the the data loader is closed, giving the
     * dialect an opportunity to do any post loading work for the given table.
     * @param jdbcTemplate TODO
     */
    public void cleanupAfterDataLoad(JdbcTemplate jdbcTemplate, Table table);

    public Database readPlatformDatabase(boolean includeSymmetricTables);
    
    /**
     * For performance reasons, the as table metadata is read in, it is cached.
     * This method will clear that cache.
     */
    public void resetCachedTableModel();

    /**
     * Check to see if the database is configured for symmetric already, or if
     * it needs configured.
     * 
     * @return true if configuration tables need to be created.
     */
    public boolean doesDatabaseNeedConfigured();

    public void initTablesAndFunctions();

    public Platform getPlatform();

    public String getName();

    public String getVersion();

    public int getMajorVersion();

    public int getMinorVersion();

    public String getProductVersion();

    public BinaryEncoding getBinaryEncoding();

    public Table getTable(String catalogName, String schemaName, final String tableName, boolean useCache);

    public Table getTable(Trigger trigger, boolean useCache);

    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger);

    public String createInitialLoadSqlFor(Node node, TriggerRouter trigger, Table  table, TriggerHistory triggerHistory, Channel channel);

    public String createPurgeSqlFor(Node node, TriggerRouter triggerRouter);

    public String createCsvDataSql(Trigger trig, Channel channel, String whereClause);

    public String createCsvPrimaryKeySql(Trigger trig, Channel channel, String whereClause);

    /**
     * @return true if blank characters are padded out
     */
    public boolean isBlankCharColumnSpacePadded();
    
    /**
     * @return true if non-blank characters are padded out
     */
    public boolean isNonBlankCharColumnSpacePadded();

    public boolean isCharColumnSpaceTrimmed();

    public boolean isEmptyStringNulled();

    /**
     * Get the maximum size the name of a trigger can be for the database
     * platform. If the generated symmetric trigger name is greater than the max
     * trigger name, symmetric will truncate the name, then log a warning
     * suggesting that you might want to provide your own name.
     */
    public int getMaxTriggerNameLength();

    public boolean storesUpperCaseNamesInCatalog();

    public boolean storesLowerCaseNamesInCatalog();

    public boolean supportsTransactionId();
    
    public int getQueryTimeoutInSeconds();
    
    /**
     * Use this call to check to see if the implemented database dialect supports 
     * a way to check on pending database transactions.
     */
    public boolean supportsTransactionViews();

    public boolean requiresSavepointForFallback();

    public Object createSavepoint(JdbcTemplate jdbcTemplate);

    public Object createSavepointForFallback(JdbcTemplate jdbcTemplate);

    public void rollbackToSavepoint(JdbcTemplate jdbcTemplate, Object savepoint);

    public void releaseSavepoint(JdbcTemplate jdbcTemplate, Object savepoint);

    public IColumnFilter newDatabaseColumnFilter();

    /**
     * Implement this if the database has some type of cleanup functionality
     * that needs to be run when dropping database objects. An example is
     * Oracle's 'purge recyclebin'
     */
    public void purge();

    public void disableSyncTriggers(JdbcTemplate jdbcTemplate);

    public void disableSyncTriggers(JdbcTemplate jdbcTemplate, String nodeId);

    public void enableSyncTriggers(JdbcTemplate jdbcTemplate);

    public String getSyncTriggersExpression();
    
    public String getSourceNodeExpression();

    public String getDefaultSchema();

    public String getDefaultCatalog();

    public int getStreamingResultsFetchSize();

    public String getCreateSymmetricDDL();

    public String getCreateTableXML(TriggerRouter triggerRouter);

    public String getCreateTableSQL(TriggerRouter trig);

    public boolean isBlobSyncSupported();

    public boolean isDateOverrideToTimestamp();

    public boolean isClobSyncSupported();

    /**
     * An indicator as to whether the ability to override the default
     * transaction id provided by the dialect can be overridden in the trigger
     * configuration.
     */
    public boolean isTransactionIdOverrideSupported();

    public String getIdentifierQuoteString();

    public void createTables(String xml);

    public boolean supportsGetGeneratedKeys();

    public boolean supportsReturningKeys();

    public String getSelectLastInsertIdSql(String sequenceName);

    public long insertWithGeneratedKey(final String sql, final SequenceIdentifier sequenceId);

    public long insertWithGeneratedKey(final String sql, final SequenceIdentifier sequenceId,
            final PreparedStatementCallback<Object> psCallback);

    public long insertWithGeneratedKey(JdbcTemplate jdbcTemplate, final String sql,
            final SequenceIdentifier sequenceId, final PreparedStatementCallback<Object> psCallback);

    public Column[] orderColumns(String[] columnNames, Table table);
    
    public Object[] getObjectValues(BinaryEncoding encoding, String[] values, Column[] orderedMetaData);

    public Object[] getObjectValues(BinaryEncoding encoding, Table table, String[] columnNames, String[] values);

    /**
     * Get the string prepended to the Symmetric configuration tables.
     * 
     * @return
     */
    public String getTablePrefix();
    
    /**
     * Get the max number of data objects to load before processing.  This parameter typically comes
     * from the {@link ParameterConstants#ROUTING_PEEK_AHEAD_WINDOW} parameter, unless the dialect chooses
     * to override how it is retrieved.
     */
    public int getRouterDataPeekAheadCount();

    public boolean supportsOpenCursorsAcrossCommit();

    /**
     * Retrieves a list of keywords for the database.
     */
    public Set<String> getSqlKeywords();

    public String getInitialLoadTableAlias();

    public String preProcessTriggerSqlClause(String sqlClause);
    
    public String toFormattedTimestamp(Date time);
    
    public void truncateTable(String tableName);
    
    public long getDatabaseTime();
    
    public boolean areDatabaseTransactionsPendingSince(long time);
    
    public boolean requiresAutoCommitFalseToSetFetchSize();
    
    public LobHandler getLobHandler();
    
    /**
     * Returns true if the trigger select lob data back from the original table.
     */
    public boolean needsToSelectLobData();
    
    public boolean isLob(int type);
    
    /**
     * This is a SQL clause that compares the old data to the new data in a trigger.
     */
    public String getDataHasChangedCondition();
    
    public Map<String, String> getSqlScriptReplacementTokens();
    
    public String scrubSql(String sql);
    
    public StringBuilder scrubSql(StringBuilder sql);
    
    /**
     * Indicates whether captured data can contain gaps.
     */
    public boolean canGapsOccurInCapturedDataIds();
    
    public String massageDataExtractionSql(String sql, Channel channel);
    
    public String massageForLob(String sql, Channel channel);
    
    /**
     * Indicates that the dialect relies on SQL that is to be inserted into the database for use
     * by embedded Java triggers.  H2 is an example dialect that needs this feature.
     * @return
     */
    public boolean escapesTemplatesForDatabaseInserts();
    
    public String getMasterCollation();
 
    public boolean supportsBatchUpdates();
    
    public void cleanupTriggers();

    public void addDatabaseUpgradeListener(IDatabaseUpgradeListener listener);
    
}