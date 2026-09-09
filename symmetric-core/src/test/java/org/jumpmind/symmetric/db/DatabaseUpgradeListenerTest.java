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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Types;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DatabaseUpgradeListenerTest {
    private DatabaseUpgradeListener listener;
    private ISymmetricEngine engine;
    private ITriggerRouterService triggerRouterService;
    private ISqlTemplate sqlTemplate;
    private Database currentModel;
    private StringBuilder sqlScript;

    @BeforeEach
    void setUp() {
        engine = mock(ISymmetricEngine.class);
        triggerRouterService = mock(ITriggerRouterService.class);
        when(engine.getTriggerRouterService()).thenReturn(triggerRouterService);
        sqlTemplate = mock(ISqlTemplate.class);
        listener = new DatabaseUpgradeListener();
        listener.setSymmetricEngine(engine);
        currentModel = new Database();
        currentModel.addTable(new Table("sym_test_table"));
        currentModel.addTable(new Table("sym_second_table"));
        sqlScript = new StringBuilder();
    }

    @Test
    void testDropTableDueToUpgrade_TableIsNull_ReturnsFalseAndNeverCallsSqlTemplate() {
        boolean result = listener.dropTableDueToUpgrade(null, currentModel, sqlTemplate, sqlScript);
        assertFalse(result, "A null table should not be dropped");
        verify(sqlTemplate, never()).update(anyString());
    }

    @Test
    void testDropTableDueToUpgrade_TableExists_DropsTableAndReturnsTrue() {
        Table table = currentModel.findTable("sym_test_table");
        boolean result = listener.dropTableDueToUpgrade(table, currentModel, sqlTemplate, sqlScript);
        assertTrue(result, "An existing table should be dropped");
        verify(sqlTemplate, times(1)).update("drop table sym_test_table");
    }

    @Test
    void testDropTableDueToUpgrade_SqlTemplateThrows_ReturnsFalse() {
        when(sqlTemplate.update(anyString())).thenThrow(new RuntimeException("table does not exist"));
        Table table = currentModel.findTable("sym_test_table");
        boolean result = listener.dropTableDueToUpgrade(table, currentModel, sqlTemplate, sqlScript);
        assertFalse(result, "A failed drop should not propagate the exception");
    }

    @Test
    void testTruncateTableDueToUpgrade_TableExists_TruncatesAndReturnsTrue() {
        Table table = currentModel.findTable("sym_test_table");
        boolean result = listener.truncateTableDueToUpgrade(table, sqlTemplate, sqlScript);
        assertTrue(result, "An existing table should be truncated");
        verify(sqlTemplate, times(1)).update("truncate table sym_test_table");
    }

    @Test
    void testDropPrimaryKeyConstraintDueToUpgrade_TableExists_QueriesConstraintNameAndDropsIt() {
        when(sqlTemplate.queryForString(anyString())).thenReturn("sym_pk_test_table");
        Table table = currentModel.findTable("sym_test_table");
        boolean result = listener.dropPrimaryKeyConstraintDueToUpgrade(table, sqlTemplate, sqlScript);
        assertTrue(result, "An existing table's primary key should be dropped");
        verify(sqlTemplate, times(1)).update("alter table sym_test_table drop constraint sym_pk_test_table");
    }

    @Test
    void testDropTables_AllTablesExist_ActsOnEachAndReturnsTrue() {
        String[] tableNames = { "sym_test_table", "sym_second_table" };
        boolean result = listener.dropTables(tableNames, currentModel, sqlTemplate, sqlScript);
        assertTrue(result, "Dropping all existing tables should succeed");
        verify(sqlTemplate, times(1)).update("drop table sym_test_table");
        verify(sqlTemplate, times(1)).update("drop table sym_second_table");
    }

    @Test
    void testDropTables_OneTableMissing_SkipsItAndReturnsTrue() {
        String[] tableNames = { "sym_test_table", "sym_missing_table" };
        boolean result = listener.dropTables(tableNames, currentModel, sqlTemplate, sqlScript);
        assertTrue(result, "A missing table should be skipped without affecting the overall result");
        verify(sqlTemplate, times(1)).update("drop table sym_test_table");
        verify(sqlTemplate, never()).update("drop table sym_missing_table");
    }

    @Test
    void testTruncateTables_OneTableMissing_SkipsItAndReturnsTrue() {
        String[] tableNames = { "sym_test_table", "sym_missing_table" };
        boolean result = listener.truncateTables(tableNames, currentModel, sqlTemplate, sqlScript);
        assertTrue(result, "A missing table should be skipped without affecting the overall result");
        verify(sqlTemplate, times(1)).update("truncate table sym_test_table");
        verify(sqlTemplate, never()).update("truncate table sym_missing_table");
    }

    @Test
    void testTruncateTables_AllTablesExist_ActsOnEachAndReturnsTrue() {
        String[] tableNames = { "sym_test_table", "sym_second_table" };
        boolean result = listener.truncateTables(tableNames, currentModel, sqlTemplate, sqlScript);
        assertTrue(result, "Truncating all existing tables should succeed");
        verify(sqlTemplate, times(1)).update("truncate table sym_test_table");
        verify(sqlTemplate, times(1)).update("truncate table sym_second_table");
    }

    @Test
    void testDropPkFromTables_OneTableMissing_SkipsItAndReturnsTrue() {
        when(sqlTemplate.queryForString(anyString())).thenReturn("sym_pk_test_table");
        String[] tableNames = { "sym_test_table", "sym_missing_table" };
        boolean result = listener.dropPkFromTables(tableNames, currentModel, sqlTemplate, sqlScript);
        assertTrue(result, "A missing table should be skipped without affecting the overall result");
        verify(sqlTemplate, times(1)).update("alter table sym_test_table drop constraint sym_pk_test_table");
    }

    @Test
    void testDropIndexFromTable_TableIsNull_ReturnsFalseAndNeverCallsSqlTemplate() {
        boolean result = listener.dropIndexFromTable(null, "sym_idx_test", sqlTemplate, sqlScript);
        assertFalse(result, "A null table should not have an index dropped");
        verify(sqlTemplate, never()).update(anyString());
    }

    @Test
    void testDropIndexFromTable_TableExists_DropsIndexAndReturnsTrue() {
        Table table = currentModel.findTable("sym_test_table");
        boolean result = listener.dropIndexFromTable(table, "sym_idx_test", sqlTemplate, sqlScript);
        assertTrue(result, "An existing table's index should be dropped");
        verify(sqlTemplate, times(1)).update("drop index sym_test_table.sym_idx_test");
    }

    @Test
    void testDropIndexFromTable_SqlTemplateThrows_ReturnsFalse() {
        when(sqlTemplate.update(anyString())).thenThrow(new RuntimeException("index does not exist"));
        Table table = currentModel.findTable("sym_test_table");
        boolean result = listener.dropIndexFromTable(table, "sym_idx_test", sqlTemplate, sqlScript);
        assertFalse(result, "A failed index drop should not propagate the exception");
    }

    @Test
    void testDeleteFromTableDueToUpgrade_TableExists_DeletesAndReturnsTrue() {
        Table table = currentModel.findTable("sym_test_table");
        boolean result = listener.deleteFromTableDueToUpgrade(table, sqlTemplate, sqlScript);
        assertTrue(result, "An existing table's rows should be deleted");
        verify(sqlTemplate, times(1)).update("delete from sym_test_table");
    }

    @Test
    void testDeleteFromTables_OneTableMissing_SkipsItAndReturnsTrue() {
        String[] tableNames = { "sym_test_table", "sym_missing_table" };
        boolean result = listener.deleteFromTables(tableNames, currentModel, sqlTemplate, sqlScript);
        assertTrue(result, "A missing table should be skipped without affecting the overall result");
        verify(sqlTemplate, times(1)).update("delete from sym_test_table");
        verify(sqlTemplate, never()).update("delete from sym_missing_table");
    }

    @Test
    void testDropConstraintFromTable_TableIsNull_ReturnsFalseAndNeverCallsSqlTemplate() {
        boolean result = listener.dropConstraintFromTable(null, "sym_fk_test", sqlTemplate, sqlScript);
        assertFalse(result, "A null table should not have a constraint dropped");
        verify(sqlTemplate, never()).update(anyString());
    }

    @Test
    void testDropConstraintFromTable_TableExists_DropsConstraintAndReturnsTrue() {
        Table table = currentModel.findTable("sym_test_table");
        boolean result = listener.dropConstraintFromTable(table, "sym_fk_test", sqlTemplate, sqlScript);
        assertTrue(result, "An existing table's constraint should be dropped");
        verify(sqlTemplate, times(1)).update("alter table sym_test_table drop constraint sym_fk_test");
    }

    @Test
    void testDropConstraintFromTable_SqlTemplateThrows_ReturnsFalse() {
        when(sqlTemplate.update(anyString())).thenThrow(new RuntimeException("constraint does not exist"));
        Table table = currentModel.findTable("sym_test_table");
        boolean result = listener.dropConstraintFromTable(table, "sym_fk_test", sqlTemplate, sqlScript);
        assertFalse(result, "A failed constraint drop should not propagate the exception");
    }

    @Test
    void testIsUpgradeFromPre3_8_MonitorTableMissingFromCurrentButPresentInDesired_ReturnsTrue() {
        Database currentModelForTest = new Database();
        currentModelForTest.addTable(new Table("sym_node"));
        Database desiredModel = new Database();
        desiredModel.addTable(new Table("sym_monitor"));
        boolean result = listener.isUpgradeFromPre3_8("sym", currentModelForTest, desiredModel);
        assertTrue(result, "A missing monitor table with an existing node table indicates a pre-3.8 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_8_MonitorTableAlreadyExists_ReturnsFalse() {
        Database currentModelForTest = new Database();
        currentModelForTest.addTable(new Table("sym_node"));
        currentModelForTest.addTable(new Table("sym_monitor"));
        Database desiredModel = new Database();
        desiredModel.addTable(new Table("sym_monitor"));
        boolean result = listener.isUpgradeFromPre3_8("sym", currentModelForTest, desiredModel);
        assertFalse(result, "An already-present monitor table means this is not a pre-3.8 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_8_NodeTableMissing_ReturnsFalse() {
        Database currentModelForTest = new Database();
        Database desiredModel = new Database();
        desiredModel.addTable(new Table("sym_monitor"));
        boolean result = listener.isUpgradeFromPre3_8("sym", currentModelForTest, desiredModel);
        assertFalse(result, "A missing node table means there is nothing to upgrade from");
    }

    @Test
    void testIsUpgradeFromPre3_10_NodeTableHasHeartbeatTimeColumn_ReturnsTrue() {
        Database currentModelForTest = new Database();
        Table nodeTable = new Table("sym_node");
        nodeTable.addColumn(new Column("heartbeat_time"));
        currentModelForTest.addTable(nodeTable);
        boolean result = listener.isUpgradeFromPre3_10("sym", currentModelForTest, new Database());
        assertTrue(result, "A node table with a heartbeat_time column indicates a pre-3.10 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_10_NodeTableMissingHeartbeatTimeColumn_ReturnsFalse() {
        Database currentModelForTest = new Database();
        currentModelForTest.addTable(new Table("sym_node"));
        boolean result = listener.isUpgradeFromPre3_10("sym", currentModelForTest, new Database());
        assertFalse(result, "A node table without a heartbeat_time column means this is not a pre-3.10 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_11_DataEventTableHasRouterIdColumn_ReturnsTrue() {
        Database currentModelForTest = new Database();
        Table dataEventTable = new Table("sym_data_event");
        dataEventTable.addColumn(new Column("router_id"));
        currentModelForTest.addTable(dataEventTable);
        boolean result = listener.isUpgradeFromPre3_11("sym", currentModelForTest, new Database());
        assertTrue(result, "A data_event table with a router_id column indicates a pre-3.11 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_11_DataEventTableMissing_ReturnsFalse() {
        boolean result = listener.isUpgradeFromPre3_11("sym", new Database(), new Database());
        assertFalse(result, "A missing data_event table means this is not a pre-3.11 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_12_NodeSecurityTableMissingFailedLoginsColumn_ReturnsTrue() {
        Database currentModelForTest = new Database();
        currentModelForTest.addTable(new Table("sym_node_security"));
        boolean result = listener.isUpgradeFromPre3_12("sym", currentModelForTest, new Database());
        assertTrue(result, "A node_security table without a failed_logins column indicates a pre-3.12 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_12_NodeSecurityTableHasFailedLoginsColumn_ReturnsFalse() {
        Database currentModelForTest = new Database();
        Table nodeSecurityTable = new Table("sym_node_security");
        nodeSecurityTable.addColumn(new Column("failed_logins"));
        currentModelForTest.addTable(nodeSecurityTable);
        boolean result = listener.isUpgradeFromPre3_12("sym", currentModelForTest, new Database());
        assertFalse(result, "A node_security table with a failed_logins column means this is not a pre-3.12 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_12_5_NodeSecurityTableMissingInitialLoadEndTimeColumn_ReturnsTrue() {
        Database currentModelForTest = new Database();
        currentModelForTest.addTable(new Table("sym_node_security"));
        boolean result = listener.isUpgradeFromPre3_12_5("sym", currentModelForTest, new Database());
        assertTrue(result, "A node_security table without an initial_load_end_time column indicates a pre-3.12.5 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_12_5_NodeSecurityTableHasInitialLoadEndTimeColumn_ReturnsFalse() {
        Database currentModelForTest = new Database();
        Table nodeSecurityTable = new Table("sym_node_security");
        nodeSecurityTable.addColumn(new Column("initial_load_end_time"));
        currentModelForTest.addTable(nodeSecurityTable);
        boolean result = listener.isUpgradeFromPre3_12_5("sym", currentModelForTest, new Database());
        assertFalse(result, "A node_security table with an initial_load_end_time column means this is not a pre-3.12.5 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_14_ExtractRequestTableMissingSourceNodeIdColumn_ReturnsTrue() {
        Database currentModelForTest = new Database();
        currentModelForTest.addTable(new Table("sym_extract_request"));
        boolean result = listener.isUpgradeFromPre3_14("sym", currentModelForTest, new Database());
        assertTrue(result, "An extract_request table without a source_node_id column indicates a pre-3.14 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_14_ExtractRequestTableHasSourceNodeIdColumn_ReturnsFalse() {
        Database currentModelForTest = new Database();
        Table extractRequestTable = new Table("sym_extract_request");
        extractRequestTable.addColumn(new Column("source_node_id"));
        currentModelForTest.addTable(extractRequestTable);
        boolean result = listener.isUpgradeFromPre3_14("sym", currentModelForTest, new Database());
        assertFalse(result, "An extract_request table with a source_node_id column means this is not a pre-3.14 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_15_TableReloadRequestTableMissing_ReturnsFalse() {
        boolean result = listener.isUpgradeFromPre3_15("sym", new Database());
        assertFalse(result, "A missing table_reload_request table means this is not a pre-3.15 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_15_CreateTimeColumnSizeIsTwo_ReturnsFalse() {
        Database currentModelForTest = new Database();
        Table tableReloadRequestTable = new Table("sym_table_reload_request");
        Column createTime = new Column("create_time");
        createTime.setSize("2");
        tableReloadRequestTable.addColumn(createTime);
        currentModelForTest.addTable(tableReloadRequestTable);
        boolean result = listener.isUpgradeFromPre3_15("sym", currentModelForTest);
        assertFalse(result, "A create_time column already sized at 2 means this is not a pre-3.15 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_15_CreateTimeColumnSizeIsNotTwo_ReturnsTrue() {
        Database currentModelForTest = new Database();
        Table tableReloadRequestTable = new Table("sym_table_reload_request");
        Column createTime = new Column("create_time");
        createTime.setSize("19");
        tableReloadRequestTable.addColumn(createTime);
        currentModelForTest.addTable(tableReloadRequestTable);
        boolean result = listener.isUpgradeFromPre3_15("sym", currentModelForTest);
        assertTrue(result, "A create_time column not yet sized at 2 indicates a pre-3.15 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_16_ExtractRequestTableMissingExtractThreadIdColumn_ReturnsTrue() {
        Database currentModelForTest = new Database();
        currentModelForTest.addTable(new Table("sym_extract_request"));
        boolean result = listener.isUpgradeFromPre3_16("sym", currentModelForTest);
        assertTrue(result, "An extract_request table without an extract_thread_id column indicates a pre-3.16 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_16_ExtractRequestTableHasExtractThreadIdColumn_ReturnsFalse() {
        Database currentModelForTest = new Database();
        Table extractRequestTable = new Table("sym_extract_request");
        extractRequestTable.addColumn(new Column("extract_thread_id"));
        currentModelForTest.addTable(extractRequestTable);
        boolean result = listener.isUpgradeFromPre3_16("sym", currentModelForTest);
        assertFalse(result, "An extract_request table with an extract_thread_id column means this is not a pre-3.16 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_17_NodeHostChannelStatsTableMissingDataReceivedColumn_ReturnsTrue() {
        Database currentModelForTest = new Database();
        currentModelForTest.addTable(new Table(TableConstants.getTableName("sym", TableConstants.SYM_NODE_HOST_CHANNEL_STATS)));
        boolean result = listener.isUpgradeFromPre3_17("sym", currentModelForTest);
        assertTrue(result, "A node_host_channel_stats table without a data_received column indicates a pre-3.17 upgrade");
    }

    @Test
    void testIsUpgradeFromPre3_17_NodeHostChannelStatsTableHasDataReceivedColumn_ReturnsFalse() {
        Database currentModelForTest = new Database();
        Table nodeHostChannelStatsTable = new Table(TableConstants.getTableName("sym", TableConstants.SYM_NODE_HOST_CHANNEL_STATS));
        nodeHostChannelStatsTable.addColumn(new Column("data_received"));
        currentModelForTest.addTable(nodeHostChannelStatsTable);
        boolean result = listener.isUpgradeFromPre3_17("sym", currentModelForTest);
        assertFalse(result, "A node_host_channel_stats table with a data_received column means this is not a pre-3.17 upgrade");
    }

    @Test
    void testDropTriggers_TwoArg_TableMissing_NeverLooksUpTriggerHistory() {
        listener.dropTriggers(currentModel, "sym_missing_table");
        verify(triggerRouterService, never()).findTriggerHistory(any(), any(), any());
    }

    @Test
    void testDropTriggers_TwoArg_TriggerHistoryFound_DropsTriggers() {
        TriggerHistory triggerHistory = new TriggerHistory("sym_test_table", "id", "id,name");
        when(triggerRouterService.findTriggerHistory(null, null, "sym_test_table")).thenReturn(triggerHistory);
        listener.dropTriggers(currentModel, "sym_test_table");
        verify(triggerRouterService, times(1)).dropTriggers(triggerHistory);
    }

    @Test
    void testDropTriggers_TwoArg_NoTriggerHistoryFound_NeverDropsTriggers() {
        listener.dropTriggers(currentModel, "sym_test_table");
        verify(triggerRouterService, never()).dropTriggers(any(TriggerHistory.class));
    }

    @Test
    void testDropTriggers_ThreeArg_ColumnMissing_NeverLooksUpTriggerHistory() {
        listener.dropTriggers(currentModel, "sym_test_table", "missing_column");
        verify(triggerRouterService, never()).findTriggerHistory(any(), any(), any());
    }

    @Test
    void testDropTriggers_ThreeArg_ColumnFoundAndTriggerHistoryFound_DropsTriggers() {
        Table table = currentModel.findTable("sym_test_table");
        table.addColumn(new Column("legacy_column"));
        TriggerHistory triggerHistory = new TriggerHistory("sym_test_table", "id", "id,legacy_column");
        when(triggerRouterService.findTriggerHistory(null, null, "sym_test_table")).thenReturn(triggerHistory);
        listener.dropTriggers(currentModel, "sym_test_table", "legacy_column");
        verify(triggerRouterService, times(1)).dropTriggers(triggerHistory);
    }

    @Test
    void testCheckForDroppedColumns_ColumnDroppedInDesiredModel_DropsTriggersForTable() {
        Table currentOrderTable = new Table("sym_order");
        currentOrderTable.addColumn(new Column("order_id"));
        currentOrderTable.addColumn(new Column("legacy_status"));
        currentModel.addTable(currentOrderTable);
        Database desiredModel = new Database();
        Table desiredOrderTable = new Table("sym_order");
        desiredOrderTable.addColumn(new Column("order_id"));
        desiredModel.addTable(desiredOrderTable);
        TriggerHistory triggerHistory = new TriggerHistory("sym_order", "order_id", "order_id,legacy_status");
        when(triggerRouterService.findTriggerHistory(null, null, "sym_order")).thenReturn(triggerHistory);
        listener.checkForDroppedColumns(currentModel, desiredModel);
        verify(triggerRouterService, times(1)).dropTriggers(triggerHistory);
    }

    @Test
    void testCheckForDroppedColumns_NoColumnDropped_NeverDropsTriggers() {
        Table currentOrderTable = new Table("sym_order");
        currentOrderTable.addColumn(new Column("order_id"));
        currentModel.addTable(currentOrderTable);
        Database desiredModel = new Database();
        Table desiredOrderTable = new Table("sym_order");
        desiredOrderTable.addColumn(new Column("order_id"));
        desiredModel.addTable(desiredOrderTable);
        listener.checkForDroppedColumns(currentModel, desiredModel);
        verify(triggerRouterService, never()).findTriggerHistory(any(), any(), any());
    }

    @Test
    void testFixInformixTriggerLongVarcharColumns_NotInformix_LeavesColumnUnchanged() {
        IDatabasePlatform databasePlatform = mock(IDatabasePlatform.class);
        when(databasePlatform.getName()).thenReturn(DatabaseNamesConstants.H2);
        when(engine.getDatabasePlatform()).thenReturn(databasePlatform);
        Database desiredModel = new Database();
        Table triggerTable = new Table("sym_trigger");
        Column longVarcharColumn = new Column("source_table_name", false, Types.LONGVARCHAR, 100, 0);
        triggerTable.addColumn(longVarcharColumn);
        desiredModel.addTable(triggerTable);
        listener.fixInformixTriggerLongVarcharColumns("sym", desiredModel);
        assertEquals(Types.LONGVARCHAR, longVarcharColumn.getMappedTypeCode(), "A non-Informix platform should leave the column type unchanged");
    }

    @Test
    void testFixInformixTriggerLongVarcharColumns_Informix_DowngradesLongVarcharColumnToVarchar255() {
        IDatabasePlatform databasePlatform = mock(IDatabasePlatform.class);
        when(databasePlatform.getName()).thenReturn(DatabaseNamesConstants.INFORMIX);
        when(engine.getDatabasePlatform()).thenReturn(databasePlatform);
        Database desiredModel = new Database();
        Table triggerTable = new Table("sym_trigger");
        Column longVarcharColumn = new Column("source_table_name", false, Types.LONGVARCHAR, 100, 0);
        triggerTable.addColumn(longVarcharColumn);
        desiredModel.addTable(triggerTable);
        listener.fixInformixTriggerLongVarcharColumns("sym", desiredModel);
        assertEquals(Types.VARCHAR, longVarcharColumn.getMappedTypeCode(), "Informix should downgrade a LONGVARCHAR column to VARCHAR");
        assertEquals(255, longVarcharColumn.getSizeAsInt(), "Informix should size the downgraded column at 255");
    }

    @Test
    void testMigrateOracleTransactionViewParameters_LegacyTransactionViewFlagEnabled_SavesNewParameter() {
        IParameterService parameterService = mock(IParameterService.class);
        when(engine.getParameterService()).thenReturn(parameterService);
        when(parameterService.is(ParameterConstants.DBDIALECT_ORACLE_USE_TRANSACTION_VIEW_LEGACY)).thenReturn(true);
        when(parameterService.getLong(ParameterConstants.DBDIALECT_ORACLE_TRANSACTION_VIEW_CLOCK_SYNC_THRESHOLD_MS_LEGACY, 60000)).thenReturn(60000L);
        listener.migrateOracleTransactionViewParameters();
        verify(parameterService, times(1)).saveParameter(ParameterConstants.ROUTING_GAPS_USE_TRANSACTION_VIEW, true, "upgrade");
        verify(parameterService, never()).saveParameter(eq(ParameterConstants.ROUTING_GAPS_TRANSACTION_VIEW_CLOCK_SYNC_THRESHOLD_MS), any(), anyString());
    }

    @Test
    void testMigrateOracleTransactionViewParameters_LegacyThresholdChanged_SavesNewParameter() {
        IParameterService parameterService = mock(IParameterService.class);
        when(engine.getParameterService()).thenReturn(parameterService);
        when(parameterService.getLong(ParameterConstants.DBDIALECT_ORACLE_TRANSACTION_VIEW_CLOCK_SYNC_THRESHOLD_MS_LEGACY, 60000)).thenReturn(30000L);
        listener.migrateOracleTransactionViewParameters();
        verify(parameterService, times(1)).saveParameter(ParameterConstants.ROUTING_GAPS_TRANSACTION_VIEW_CLOCK_SYNC_THRESHOLD_MS, 30000L, "upgrade");
        verify(parameterService, never()).saveParameter(eq(ParameterConstants.ROUTING_GAPS_USE_TRANSACTION_VIEW), any(), anyString());
    }

    @Test
    void testMigrateOracleTransactionViewParameters_NothingChanged_NeverSavesParameters() {
        IParameterService parameterService = mock(IParameterService.class);
        when(engine.getParameterService()).thenReturn(parameterService);
        when(parameterService.getLong(ParameterConstants.DBDIALECT_ORACLE_TRANSACTION_VIEW_CLOCK_SYNC_THRESHOLD_MS_LEGACY, 60000)).thenReturn(60000L);
        listener.migrateOracleTransactionViewParameters();
        verify(parameterService, never()).saveParameter(anyString(), any(), anyString());
    }
}
