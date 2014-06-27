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
import java.util.Set;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.ext.IDatabaseUpgradeListener;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;

/*
 * A dialect is the interface that insulates SymmetricDS from database implementation specifics. 
 */
public interface ISymmetricDialect {

    public void createTrigger(StringBuilder sqlBuffer, DataEventType dml, 
            Trigger trigger, TriggerHistory hist, Channel channel, 
            String tablePrefix, Table table);

    /*
     * Get the name of this symmetric instance. This can be set in
     * symmetric.properties using the symmetric.runtime.engine.name property.
     */
    public String getEngineName();

    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName, String triggerName,
            String tableName, TriggerHistory oldHistory);

    public boolean doesTriggerExist(String catalogName, String schema, String tableName, String triggerName);

    public void initTablesAndFunctions();
    
    public boolean createOrAlterTablesIfNecessary();

    public IDatabasePlatform getPlatform();

    public String getName();

    public String getVersion();

    public int getMajorVersion();

    public int getMinorVersion();

    public String getProductVersion();

    public BinaryEncoding getBinaryEncoding();
    
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger);

    public String createInitialLoadSqlFor(Node node, TriggerRouter trigger, Table  table, TriggerHistory triggerHistory, Channel channel);

    public String createPurgeSqlFor(Node node, TriggerRouter triggerRouter);

    public String createCsvDataSql(Trigger trigger, TriggerHistory triggerHistory, Channel channel, String whereClause);

    public String createCsvPrimaryKeySql(Trigger trigger, TriggerHistory triggerHistory, Channel channel, String whereClause);

    /*
     * Get the maximum size the name of a trigger can be for the database
     * platform. If the generated symmetric trigger name is greater than the max
     * trigger name, symmetric will truncate the name, then log a warning
     * suggesting that you might want to provide your own name.
     */
    public int getMaxTriggerNameLength();

    public boolean supportsTransactionId();
    
    /*
     * Use this call to check to see if the implemented database dialect supports 
     * a way to check on pending database transactions.
     */
    public boolean supportsTransactionViews();

    /*
     * Implement this if the database has some type of cleanup functionality
     * that needs to be run when dropping database objects. An example is
     * Oracle's 'purge recyclebin'
     */
    public void purge();

    public void disableSyncTriggers(ISqlTransaction transaction);

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId);

    public void enableSyncTriggers(ISqlTransaction transaction);

    public String getSyncTriggersExpression();
    
    public String getSourceNodeExpression();

    public String getCreateSymmetricDDL();

    public String getCreateTableXML(TriggerRouter triggerRouter);

    public String getCreateTableSQL(TriggerRouter trig);

    public boolean isBlobSyncSupported();

    public boolean isClobSyncSupported();

    /*
     * An indicator as to whether the ability to override the default
     * transaction id provided by the dialect can be overridden in the trigger
     * configuration.
     */
    public boolean isTransactionIdOverrideSupported();

    public Table getTable(Trigger trigger, boolean useCache);

    public long insertWithGeneratedKey(final String sql, final SequenceIdentifier sequenceId);
    
    public long insertWithGeneratedKey(final String sql, final SequenceIdentifier identifier, Object... args);

    @Deprecated
    public Column[] orderColumns(String[] columnNames, Table table);
    
    /*
     * Get the max number of data objects to load before processing.  This parameter typically comes
     * from the {@link ParameterConstants#ROUTING_PEEK_AHEAD_WINDOW} parameter, unless the dialect chooses
     * to override how it is retrieved.
     */
    public int getRouterDataPeekAheadCount();

    public boolean supportsOpenCursorsAcrossCommit();

    /*
     * Retrieves a list of keywords for the database.
     */
    public Set<String> getSqlKeywords();

    public String getInitialLoadTableAlias();

    public String preProcessTriggerSqlClause(String sqlClause);
    
    public String toFormattedTimestamp(Date time);
    
    public void truncateTable(String tableName);
    
    public long getDatabaseTime();
    
    public boolean areDatabaseTransactionsPendingSince(long time);   
    
    /*
     * Returns true if the trigger select lob data back from the original table.
     */
    public boolean needsToSelectLobData();
    
    /*
     * This is a SQL clause that compares the old data to the new data in a trigger.
     */
    public String getDataHasChangedCondition(Trigger trigger);
    
    /*
     * Indicates whether captured data can contain gaps.
     */
    public boolean canGapsOccurInCapturedDataIds();
    
    public String massageDataExtractionSql(String sql, Channel channel);
    
    public String massageForLob(String sql, Channel channel);
    
    /*
     * Indicates that the dialect relies on SQL that is to be inserted into the database for use
     * by embedded Java triggers.  H2 is an example dialect that needs this feature.
     * @return
     */
    public boolean escapesTemplatesForDatabaseInserts();
    
    public String getMasterCollation();
 
    public boolean supportsBatchUpdates();
    
    public void cleanupTriggers();

    public void addDatabaseUpgradeListener(IDatabaseUpgradeListener listener);
    
    public String getDriverName();
    
    public String getDriverVersion();
    
    public String getSequenceName(SequenceIdentifier identifier);

    public String getSequenceKeyName(SequenceIdentifier identifier);
    
    public String getTablePrefix();
    
    public Database readSymmetricSchemaFromXml();
    
}