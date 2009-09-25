/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
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

import java.util.Set;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;

public interface IDbDialect {

    public void createTrigger(StringBuilder sqlBuffer, DataEventType dml, Trigger trigger, TriggerHistory hist,
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
    public void prepareTableForDataLoad(Table table);

    /**
     * This is called by the data loader each time the table context changes
     * away from a table or when the the data loader is closed, giving the
     * dialect an opportunity to do any post loading work for the given table.
     */
    public void cleanupAfterDataLoad(Table table);

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

    public void initSupportDb();

    public Platform getPlatform();

    public String getName();

    public String getVersion();

    public int getMajorVersion();

    public int getMinorVersion();

    public String getProductVersion();

    public BinaryEncoding getBinaryEncoding();

    public Table getMetaDataFor(String catalog, String schema, final String tableName, boolean useCache);

    public Table getMetaDataFor(Trigger trigger, boolean useCache);

    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger);

    public String createInitalLoadSqlFor(Node node, TriggerRouter trigger);

    public String createPurgeSqlFor(Node node, TriggerRouter triggerRouter);

    public String createCsvDataSql(Trigger trig, String whereClause);

    public String createCsvPrimaryKeySql(Trigger trig, String whereClause);

    public boolean isCharSpacePadded();

    public boolean isCharSpaceTrimmed();

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

    public boolean requiresSavepointForFallback();

    public Object createSavepoint();

    public Object createSavepointForFallback();

    public void rollbackToSavepoint(Object savepoint);

    public void releaseSavepoint(Object savepoint);

    public IColumnFilter getDatabaseColumnFilter();

    /**
     * Implement this if the database has some type of cleanup functionality
     * that needs to be run when dropping database objects. An example is
     * Oracle's 'purge recyclebin'
     */
    public void purge();

    public SQLErrorCodeSQLExceptionTranslator getSqlErrorTranslator();

    public void disableSyncTriggers();

    public void disableSyncTriggers(String nodeId);

    public void enableSyncTriggers();

    public String getSyncTriggersExpression();

    public String getDefaultSchema();

    public String getDefaultCatalog();

    public int getStreamingResultsFetchSize();

    public JdbcTemplate getJdbcTemplate();

    public String getCreateSymmetricDDL();

    public String getCreateTableXML(TriggerRouter triggerRouter);

    public String getCreateTableSQL(TriggerRouter trig);

    public boolean isBlobSyncSupported();

    public boolean isBlobOverrideToBinary();

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
            final PreparedStatementCallback psCallback);

    public long insertWithGeneratedKey(JdbcTemplate jdbcTemplate, final String sql,
            final SequenceIdentifier sequenceId, final PreparedStatementCallback psCallback);

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

    /**
     * Give access to the templating mechanism that is used for trigger
     * creation.
     */
    public String replaceTemplateVariables(DataEventType dml, Trigger trigger, TriggerHistory history,
            String targetString);

    public String getTriggerName(DataEventType dml, String triggerPrefix, int maxTriggerNameLength, Trigger trigger,
            TriggerHistory hist);

    public boolean supportsOpenCursorsAcrossCommit();

    /**
     * Retrieves a list of keywords for the database.
     */
    public Set<String> getSqlKeywords();

    public String getInitialLoadTableAlias();

    public String preProcessTriggerSqlClause(String sqlClause);
}
