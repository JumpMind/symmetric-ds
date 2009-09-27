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
package org.jumpmind.symmetric.test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.db2.Db2DbDialect;
import org.jumpmind.symmetric.db.firebird.FirebirdDbDialect;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticNameConstants;
import org.jumpmind.symmetric.test.ParameterizedSuite.ParameterExcluder;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.RowMapper;

public class SimpleIntegrationTest extends AbstractIntegrationTest {

    static final Log logger = LogFactory.getLog(SimpleIntegrationTest.class);

    static final String insertOrderHeaderSql = "insert into test_order_header (order_id, customer_id, status, deliver_date) values(?,?,?,?)";

    static final String updateOrderHeaderStatusSql = "update test_order_header set status = ? where order_id = ?";

    static final String selectOrderHeaderSql = "select order_id, customer_id, status, deliver_date from test_order_header where order_id = ?";

    static final String insertOrderDetailSql = "insert into test_order_detail (order_id, line_number, item_type, item_id, quantity, price) values(?,?,?,?,?,?)";

    static final String insertCustomerSql = "insert into test_customer (customer_id, name, is_active, address, city, state, zip, entry_timestamp, entry_time, notes, icon) values(?,?,?,?,?,?,?,?,?,?,?)";

    static final String insertTestTriggerTableSql = "insert into test_triggers_table (id, string_one_value, string_two_value) values(?,?,?)";

    static final String updateTestTriggerTableSql = "update test_triggers_table set string_one_value=?";

    static final String insertStoreStatusSql = "insert into test_store_status (store_id, register_id, status) values(?,?,?)";

    static final String updateStoreStatusSql = "update test_store_status set status = ? where store_id = ? and register_id = ?";

    static final String selectStoreStatusSql = "select status from test_store_status where store_id = ? and register_id = ?";

    static final String enableKeyWordTriggerSql = "update sym_trigger set sync_on_insert = 1, sync_on_update = 1, sync_on_delete = 1 where source_table_name = 'test_key_word'";

    static final String alterKeyWordSql = "alter table test_key_word add \"key word\" char(1)";

    static final String alterKeyWordSql2 = "alter table test_key_word add \"case\" char(1)";

    static final String insertKeyWordSql = "insert into test_key_word (id, \"key word\", \"case\") values (?, ?, ?)";

    static final String updateKeyWordSql = "update test_key_word set \"key word\" = ?, \"case\" = ? where id = ?";

    static final String selectKeyWordSql = "select \"key word\", \"case\" from test_key_word where id = ?";

    static final String nullSyncColumnLevelSql = "update test_sync_column_level set string_value = null, time_value = null, date_value = null, bigint_value = null, decimal_value = null where id = ?";

    static final String deleteSyncColumnLevelSql = "delete from test_sync_column_level where id = ?";

    static final String updateSyncColumnLevelSql = "update test_sync_column_level set $(column) = ? where id = ?";

    static final String selectSyncColumnLevelSql = "select count(*) from test_sync_column_level where id = ? and $(column) = ?";

    static final String isRegistrationClosedSql = "select count(*) from sym_node_security where registration_enabled=0 and node_id=?";

    static final byte[] BINARY_DATA = new byte[] { 0x01, 0x02, 0x03 };

    public SimpleIntegrationTest() throws Exception {
    }

    public SimpleIntegrationTest(String client, String root) throws Exception {
        super(client, root);
    }

    protected void checkForFailedTriggers() {
        ITriggerRouterService service = AppUtils.find(Constants.TRIGGER_ROUTER_SERVICE, getClientEngine());
        Assert.assertEquals(0, service.getFailedTriggers().size());
        service = AppUtils.find(Constants.TRIGGER_ROUTER_SERVICE, getRootEngine());
        Assert.assertEquals(0, service.getFailedTriggers().size());
    }

    @Test(timeout = 30000)
    public void registerClientWithRoot() {
        logTestRunning();
        INodeService rootNodeService = AppUtils.find(Constants.NODE_SERVICE, getRootEngine());
        getRootEngine().openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertTrue("The registration for the client should be opened now.", rootNodeService.findNodeSecurity(
                TestConstants.TEST_CLIENT_EXTERNAL_ID).isRegistrationEnabled());
        getClientEngine().start();
        getClientEngine().pull();
        Assert.assertTrue("The client did not register.", getClientEngine().isRegistered());
        Assert.assertFalse("The registration for the client should be closed now.", rootNodeService.findNodeSecurity(
                TestConstants.TEST_CLIENT_EXTERNAL_ID).isRegistrationEnabled());
        IStatisticManager statMgr = (IStatisticManager) getClientEngine().getApplicationContext().getBean(
                Constants.STATISTIC_MANAGER);
        statMgr.flush();

        checkForFailedTriggers();
    }

    @Test(timeout = 30000)
    public void initialLoad() {
        logTestRunning();
        IDbDialect rootDialect = getRootDbDialect();
        rootJdbcTemplate.update(insertCustomerSql, new Object[] { 301, "Linus", "1", "42 Blanket Street",
                "Santa Claus", "IN", 90009, new Date(), new Date(), "This is a test", BINARY_DATA });
        insertIntoTestTriggerTable(rootDialect, new Object[] { 1, "wow", "mom" });
        insertIntoTestTriggerTable(rootDialect, new Object[] { 2, "mom", "wow" });

        INodeService rootNodeService = AppUtils.find(Constants.NODE_SERVICE, getRootEngine());
        INodeService clientNodeService = AppUtils.find(Constants.NODE_SERVICE, getClientEngine());
        String nodeId = rootNodeService.findNodeByExternalId(TestConstants.TEST_CLIENT_NODE_GROUP,
                TestConstants.TEST_CLIENT_EXTERNAL_ID).getNodeId();

        getRootEngine().reloadNode(nodeId);
        IOutgoingBatchService rootOutgoingBatchService = AppUtils.find(Constants.OUTGOING_BATCH_SERVICE,
                getRootEngine());
        Assert.assertFalse(rootOutgoingBatchService.isInitialLoadComplete(nodeId));

        Assert.assertTrue(rootNodeService.findNodeSecurity(TestConstants.TEST_CLIENT_EXTERNAL_ID)
                .isInitialLoadEnabled());

        while (!rootOutgoingBatchService.isInitialLoadComplete(nodeId)) {
            getClientEngine().pull();
        }

        Assert.assertFalse(rootNodeService.findNodeSecurity(TestConstants.TEST_CLIENT_EXTERNAL_ID)
                .isInitialLoadEnabled());

        NodeSecurity clientNodeSecurity = clientNodeService.findNodeSecurity(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertFalse(clientNodeSecurity.isInitialLoadEnabled());
        Assert.assertNotNull(clientNodeSecurity.getInitialLoadTime());

        assertEquals(clientJdbcTemplate.queryForInt("select count(*) from sym_incoming_batch where status='ER'"), 0,
                "The initial load errored out." + printRootAndClientDatabases());
        assertEquals(clientJdbcTemplate.queryForInt("select count(*) from test_triggers_table"), 2,
                "test_triggers_table on the client did not contain the expected number of rows");
        assertEquals(clientJdbcTemplate.queryForInt("select count(*) from test_customer"), 2,
                "test_customer on the client did not contain the expected number of rows");
        assertEquals(clientJdbcTemplate
                .queryForInt("select count(*) from sym_node_security where initial_load_enabled=1"), 0,
                "Initial load was not successful according to the client");
        assertEquals(rootJdbcTemplate
                .queryForInt("select count(*) from sym_node_security where initial_load_enabled=1"), 0,
                "Initial load was not successful accordign to the root");
    }

    private void insertIntoTestTriggerTable(IDbDialect dialect, Object[] values) {
        Table testTriggerTable = dialect.getMetaDataFor(null, null, "test_triggers_table", true);
        try {
            dialect.prepareTableForDataLoad(testTriggerTable);
            dialect.getJdbcTemplate().update(insertTestTriggerTableSql, values);
        } finally {
            dialect.cleanupAfterDataLoad(testTriggerTable);
        }
    }

    @Test(timeout = 30000)
    public void syncToClient() {
        logTestRunning();
        // test pulling no data
        getClientEngine().pull();

        final byte[] BIG_BINARY = new byte[200];
        for (int i = 0; i < BIG_BINARY.length; i++) {
            BIG_BINARY[i] = 0x01;
        }

        final String TEST_CLOB = "This is my test's test";
        // now change some data that should be sync'd
        rootJdbcTemplate.update(insertCustomerSql, new Object[] { 101, "Charlie Brown", "1", "300 Grub Street",
                "New Yorl", "NY", 90009, new Date(), new Date(), TEST_CLOB, BIG_BINARY });

        getClientEngine().pull();
        assertEquals(clientJdbcTemplate.queryForInt("select count(*) from test_customer where customer_id=101"), 1,
                "The customer was not sync'd to the client." + printRootAndClientDatabases());

        if (getRootDbDialect().isClobSyncSupported()) {
            assertEquals(clientJdbcTemplate.queryForObject("select notes from test_customer where customer_id=101",
                    String.class), rootJdbcTemplate.queryForObject(
                    "select notes from test_customer where customer_id=101", String.class),
                    "The CLOB notes field on customer was not sync'd to the client.");
        }

        if (getRootDbDialect().isBlobSyncSupported()) {
            byte[] data = (byte[]) clientJdbcTemplate.queryForObject(
                    "select icon from test_customer where customer_id=101", new RowMapper() {
                        public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                            return rs.getBytes(1);
                        }
                    });
            Assert.assertTrue("The BLOB icon field on customer was not sync'd to the client.", ArrayUtils.isEquals(
                    data, BIG_BINARY));
        }

    }

    @Test(timeout = 30000)
    public void syncToRootAutoGeneratedPrimaryKey() {
        logTestRunning();
        final String NEW_VALUE = "unique new value one value";
        IDbDialect clientDialect = getClientDbDialect();
        insertIntoTestTriggerTable(clientDialect, new Object[] { 3, "value one", "value \" two" });
        getClientEngine().push();
        clientJdbcTemplate.update(updateTestTriggerTableSql, new Object[] { NEW_VALUE });
        getClientEngine().push();
        int syncCount = rootJdbcTemplate.queryForInt(
                "select count(*) from test_triggers_table where string_one_value=?", new Object[] { NEW_VALUE });
        assertEquals(syncCount, 3, syncCount + " of the rows were updated");
    }

    @Test(timeout = 30000)
    public void reopenRegistration() {
        logTestRunning();
        getRootEngine().reOpenRegistration(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        getClientEngine().pull();
        Assert.assertEquals(1, getRootDbDialect().getJdbcTemplate().queryForInt(isRegistrationClosedSql,
                new Object[] { TestConstants.TEST_CLIENT_EXTERNAL_ID }, new int[] { Types.VARCHAR }));
    }

    private void assertEquals(Object actual, Object expected, String failureMessage) {
        Assert.assertEquals(failureMessage, expected, actual);
    }

    private boolean turnOnNoKeysInUpdateParameter(boolean newValue) {
        IParameterService clientParameterService = (IParameterService) getClientEngine().getApplicationContext()
                .getBean(Constants.PARAMETER_SERVICE);
        IParameterService rootParameterService = (IParameterService) getRootEngine().getApplicationContext().getBean(
                Constants.PARAMETER_SERVICE);
        Assert.assertEquals(clientParameterService.is(ParameterConstants.DATA_LOADER_NO_KEYS_IN_UPDATE),
                rootParameterService.is(ParameterConstants.DATA_LOADER_NO_KEYS_IN_UPDATE));
        boolean oldValue = clientParameterService.is(ParameterConstants.DATA_LOADER_NO_KEYS_IN_UPDATE);
        clientParameterService.saveParameter(ParameterConstants.DATA_LOADER_NO_KEYS_IN_UPDATE, newValue);
        rootParameterService.saveParameter(ParameterConstants.DATA_LOADER_NO_KEYS_IN_UPDATE, newValue);
        return oldValue;
    }

    @Test(timeout = 30000)
    public void syncToRoot() throws ParseException {
        logTestRunning();
        turnOnNoKeysInUpdateParameter(true);
        Date date = DateUtils.parseDate("2007-01-03", new String[] { "yyyy-MM-dd" });
        clientJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "10", 100, null, date }, new int[] {
                Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
        clientJdbcTemplate.update(insertOrderDetailSql, new Object[] { "10", 1, "STK", "110000065", 3, 3.33 });
        getClientEngine().push();
    }

    @Test(timeout = 30000)
    public void syncInsertCondition() throws ParseException {
        logTestRunning();
        // Should not sync when status = null
        Date date = DateUtils.parseDate("2007-01-02", new String[] { "yyyy-MM-dd" });
        rootJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "11", 100, null, date }, new int[] {
                Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
        getClientEngine().pull();

        IOutgoingBatchService outgoingBatchService = findOnRoot(Constants.OUTGOING_BATCH_SERVICE);
        List<OutgoingBatch> batches = outgoingBatchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertEquals(batches.size(), 0, "There should be no outgoing batches, yet I found some.");

        assertEquals(clientJdbcTemplate.queryForList(selectOrderHeaderSql, new Object[] { "11" }).size(), 0,
                "The order record was sync'd when it should not have been.");

        // Should sync when status = C
        rootJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "12", 100, "C", date }, new int[] { Types.VARCHAR,
                Types.INTEGER, Types.CHAR, Types.DATE });
        getClientEngine().pull();
        assertEquals(clientJdbcTemplate.queryForList(selectOrderHeaderSql, new Object[] { "12" }).size(), 1,
                "The order record was not sync'd when it should have been.");
        // TODO: make sure event did not fire
    }

    @Test(timeout = 30000)
    public void oneColumnTableWithPrimaryKeyUpdate() throws Exception {
        logTestRunning();
        boolean oldValue = turnOnNoKeysInUpdateParameter(true);
        rootJdbcTemplate.update("insert into ONE_COLUMN_TABLE values(1)");
        Assert
                .assertTrue(clientJdbcTemplate
                        .queryForInt("select count(*) from ONE_COLUMN_TABLE where MY_ONE_COLUMN=1") == 0);
        getClientEngine().pull();
        Assert
                .assertTrue(clientJdbcTemplate
                        .queryForInt("select count(*) from ONE_COLUMN_TABLE where MY_ONE_COLUMN=1") == 1);
        rootJdbcTemplate.update("update ONE_COLUMN_TABLE set MY_ONE_COLUMN=1 where MY_ONE_COLUMN=1");
        getClientEngine().pull();
        IOutgoingBatchService outgoingBatchService = findOnRoot(Constants.OUTGOING_BATCH_SERVICE);
        List<OutgoingBatch> batches = outgoingBatchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertEquals(batches.size(), 0, "There should be no outgoing batches, yet I found some.");
        turnOnNoKeysInUpdateParameter(oldValue);
    }

    @Test(timeout = 30000)
    @SuppressWarnings("unchecked")
    public void syncUpdateCondition() {
        logTestRunning();
        rootJdbcTemplate.update(updateOrderHeaderStatusSql, new Object[] { "C", "1" });
        getClientEngine().pull();
        List list = clientJdbcTemplate.queryForList(selectOrderHeaderSql, new Object[] { "1" });
        assertEquals(list.size(), 1, "The order record should exist.");
        Map map = (Map) list.get(0);
        assertEquals(map.get("status"), "C", "Status should be complete");
        // TODO: make sure event did not fire
    }

    @Test//(timeout = 30000)
    public void ignoreNodeChannel() {
        logTestRunning();
        INodeService rootNodeService = (INodeService) getRootEngine().getApplicationContext().getBean("nodeService");
        IConfigurationService rootConfigService = (IConfigurationService) getRootEngine().getApplicationContext().getBean(
                "configurationService");
        rootNodeService.ignoreNodeChannelForExternalId(true, TestConstants.TEST_CHANNEL_ID,
                TestConstants.TEST_ROOT_NODE_GROUP, TestConstants.TEST_ROOT_EXTERNAL_ID);
        rootConfigService.reloadChannels();
        
        NodeChannel channel = rootConfigService.getNodeChannel(TestConstants.TEST_CHANNEL_ID, TestConstants.TEST_ROOT_EXTERNAL_ID);
        Assert.assertNotNull(channel);
        Assert.assertTrue(channel.isIgnored());
        Assert.assertFalse(channel.isSuspended());
        
        rootJdbcTemplate.update(insertCustomerSql, new Object[] { 201, "Charlie Dude", "1", "300 Grub Street",
                "New Yorl", "NY", 90009, new Date(), new Date(), "This is a test", BINARY_DATA });
        getClientEngine().pull();
        assertEquals(clientJdbcTemplate.queryForInt("select count(*) from test_customer where customer_id=201"), 0,
                "The customer was sync'd to the client.");
        rootNodeService.ignoreNodeChannelForExternalId(false, TestConstants.TEST_CHANNEL_ID,
                TestConstants.TEST_ROOT_NODE_GROUP, TestConstants.TEST_ROOT_EXTERNAL_ID);
        rootConfigService.reloadChannels();
    }

    // @Test(timeout = 30000)
    public void syncUpdateWithEmptyKey() {
        if (getClientDbDialect().isEmptyStringNulled()) {
            return;
        }
        clientJdbcTemplate.update(insertStoreStatusSql, new Object[] { "00001", "", 1 });
        getClientEngine().push();

        clientJdbcTemplate.update(updateStoreStatusSql, new Object[] { 2, "00001", "" });
        getClientEngine().push();

        int status = rootJdbcTemplate.queryForInt(selectStoreStatusSql, new Object[] { "00001", "   " });
        assertEquals(status, 2, "Wrong store status");
    }

    @Test(timeout = 30000)
    public void testPurge() throws Exception {
        logTestRunning();

        // do an extra push & pull to make sure we have events cleared out
        getClientEngine().pull();
        getClientEngine().push();
        
        Thread.sleep(2000);

        IParameterService parameterService = AppUtils.find(Constants.PARAMETER_SERVICE, getRootEngine());
        int purgeRetentionMinues = parameterService.getInt(ParameterConstants.PURGE_RETENTION_MINUTES);
        // set purge in the future just in case the database time is different than the current time
        parameterService.saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, -60*24);
        
        int beforePurge = rootJdbcTemplate.queryForInt("select count(*) from sym_data");
        getRootEngine().purge();
        int afterPurge = rootJdbcTemplate.queryForInt("select count(*) from sym_data");
        Timestamp maxCreateTime = (Timestamp) rootJdbcTemplate.queryForObject("select max(create_time) from sym_data",
                Timestamp.class);
        Timestamp minCreateTime = (Timestamp) rootJdbcTemplate.queryForObject("select min(create_time) from sym_data",
                Timestamp.class);
        Assert.assertTrue("Expected data rows to have been purged at the root.  There were " + beforePurge
                + " row before anf " + afterPurge + " rows after. The max create_time in sym_data was " + maxCreateTime
                + " and the min create_time in sym_data was " + minCreateTime
                + " and the current time of the server is " + new Date(), (beforePurge - afterPurge) > 0);
        
        parameterService.saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, purgeRetentionMinues);

        parameterService = AppUtils.find(Constants.PARAMETER_SERVICE, getClientEngine());
        purgeRetentionMinues = parameterService.getInt(ParameterConstants.PURGE_RETENTION_MINUTES);
        // set purge in the future just in case the database time is different than the current time
        parameterService.saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, -60*24);
        
        beforePurge = clientJdbcTemplate.queryForInt("select count(*) from sym_data");
        getClientEngine().purge();
        afterPurge = clientJdbcTemplate.queryForInt("select count(*) from sym_data");
        maxCreateTime = (Timestamp) clientJdbcTemplate.queryForObject("select max(create_time) from sym_data",
                Timestamp.class);
        minCreateTime = (Timestamp) clientJdbcTemplate.queryForObject("select min(create_time) from sym_data",
                Timestamp.class);
        Assert.assertTrue("Expected data rows to have been purged at the client.  There were " + beforePurge
                + " row before anf " + afterPurge + " rows after. . The max create_time in sym_data was "
                + maxCreateTime + " and the min create_time in sym_data was " + minCreateTime
                + " and the current time of the server is " + new Date(), (beforePurge - afterPurge) > 0);

        parameterService.saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, purgeRetentionMinues);
    }

    @Test
    public void testHeartbeat() throws Exception {
        logTestRunning();
        final String checkHeartbeatSql = "select heartbeat_time from sym_node where external_id='"
                + TestConstants.TEST_CLIENT_EXTERNAL_ID + "'";
        long ts = System.currentTimeMillis();
        Thread.sleep(1000);
        IParameterService parameterService = AppUtils.find(Constants.PARAMETER_SERVICE, getClientEngine());
        parameterService.saveParameter(ParameterConstants.START_HEARTBEAT_JOB, true);
        getClientEngine().heartbeat();
        parameterService.saveParameter(ParameterConstants.START_HEARTBEAT_JOB, false);
        Date time = (Date) clientJdbcTemplate.queryForObject(checkHeartbeatSql, Timestamp.class);
        Assert.assertTrue("The heartbeat time was not updated locally.", time != null && time.getTime() > ts);
        getClientEngine().push();
        time = (Date) rootJdbcTemplate.queryForObject(checkHeartbeatSql, Timestamp.class);
        Assert.assertTrue("The client node was not sync'd to the root as expected.", time != null
                && time.getTime() > ts);
    }

    @Test(timeout = 30000)
    public void testVirtualTransactionId() {
        logTestRunning();
        rootJdbcTemplate.update("insert into test_very_long_table_name_1234 values('42')");
        if (getRootDbDialect().isTransactionIdOverrideSupported()) {
            assertEquals(rootJdbcTemplate.queryForObject(
                    "select transaction_id from sym_data where data_id in (select max(data_id) from sym_data)",
                    String.class), "42", "The hardcoded transaction id was not found.");
            Assert.assertEquals(rootJdbcTemplate.update("delete from test_very_long_table_name_1234 where id='42'"), 1);
            assertEquals(rootJdbcTemplate.queryForObject(
                    "select transaction_id from sym_data where data_id in (select max(data_id) from sym_data)",
                    String.class), "42", "The hardcoded transaction id was not found.");
        }
    }

    @Test(timeout = 30000)
    public void testCaseSensitiveTableNames() {
        logTestRunning();
        rootJdbcTemplate.update("insert into TEST_ALL_CAPS values(1, 'HELLO')");
        getClientEngine().pull();
        assertEquals(clientJdbcTemplate.queryForInt("select count(*) from TEST_ALL_CAPS where ALL_CAPS_ID = 1"), 1,
                "Table name in all caps was not synced");
        rootJdbcTemplate.update("insert into Test_Mixed_Case values(1, 'Hello')");
        getClientEngine().pull();
        assertEquals(clientJdbcTemplate.queryForInt("select count(*) from Test_Mixed_Case where Mixed_Case_Id = 1"), 1,
                "Table name in mixed case was not synced");
    }

    /**
     * TODO test on MSSQL
     */
    @Test(timeout = 30000)
    @ParameterExcluder("mssql")
    public void testNoPrimaryKeySync() {
        logTestRunning();
        rootJdbcTemplate.update("insert into NO_PRIMARY_KEY_TABLE values(1, 2, 'HELLO')");
        getClientEngine().pull();
        assertEquals(clientJdbcTemplate.queryForInt("select TWO_COLUMN from NO_PRIMARY_KEY_TABLE where ONE_COLUMN=1"),
                2, "Table was not synced");
        rootJdbcTemplate.update("update NO_PRIMARY_KEY_TABLE set TWO_COLUMN=3 where ONE_COLUMN=1");
        getClientEngine().pull();
        assertEquals(clientJdbcTemplate.queryForInt("select TWO_COLUMN from NO_PRIMARY_KEY_TABLE where ONE_COLUMN=1"),
                3, "Table was not updated");
        rootJdbcTemplate.update("delete from NO_PRIMARY_KEY_TABLE");
        getClientEngine().pull();
        assertEquals(clientJdbcTemplate.queryForInt("select count(*) from NO_PRIMARY_KEY_TABLE"), 0,
                "Table was not deleted from");
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 30000)
    public void testReservedColumnNames() {
        logTestRunning();
        if (getRootDbDialect() instanceof Db2DbDialect || getClientDbDialect() instanceof Db2DbDialect
                || getRootDbDialect() instanceof FirebirdDbDialect || getClientDbDialect() instanceof FirebirdDbDialect) {
            return;
        }
        // alter the table to have column names that are not usually allowed
        String rquote = getRootDbDialect().getIdentifierQuoteString();
        String cquote = getClientDbDialect().getIdentifierQuoteString();
        rootJdbcTemplate.update(alterKeyWordSql.replaceAll("\"", rquote));
        rootJdbcTemplate.update(alterKeyWordSql2.replaceAll("\"", rquote));
        clientJdbcTemplate.update(alterKeyWordSql.replaceAll("\"", cquote));
        clientJdbcTemplate.update(alterKeyWordSql2.replaceAll("\"", cquote));

        getClientDbDialect().resetCachedTableModel();
        getRootDbDialect().resetCachedTableModel();

        // enable the trigger for the table and update the client with
        // configuration
        rootJdbcTemplate.update(enableKeyWordTriggerSql);
        getRootEngine().syncTriggers();
        getRootEngine().reOpenRegistration(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        getClientEngine().pull();

        rootJdbcTemplate.update(insertKeyWordSql.replaceAll("\"", rquote), new Object[] { 1, "x", "a" });
        getClientEngine().pull();

        rootJdbcTemplate.update(updateKeyWordSql.replaceAll("\"", rquote), new Object[] { "y", "b", 1 });
        getClientEngine().pull();

        List rowList = clientJdbcTemplate.queryForList(selectKeyWordSql.replaceAll("\"", cquote), new Object[] { 1 });
        Assert.assertTrue(rowList.size() > 0);
        Map columnMap = (Map) rowList.get(0);
        assertEquals(columnMap.get("key word"), "y", "Wrong key word value in table");
        assertEquals(columnMap.get("case"), "b", "Wrong case value in table");
    }

    @Test(timeout = 30000)
    public void testSyncColumnLevel() throws ParseException {
        logTestRunning();
        int id = 1;
        String[] columns = { "id", "string_value", "time_value", "date_value", "bigint_value", "decimal_value" };
        Object[] values = new Object[] { id, "moredata", getDate("2008-01-02 03:04:05"),
                getDate("2008-02-01 05:03:04"), 600, new BigDecimal("34.10") };

        // Null out columns, change each column and sync one at a time
        clientJdbcTemplate.update(nullSyncColumnLevelSql, new Object[] { id });

        for (int i = 1; i < columns.length; i++) {
            rootJdbcTemplate.update(replace("column", columns[i], updateSyncColumnLevelSql), new Object[] { values[i],
                    id });
            getClientEngine().pull();
            assertEquals(clientJdbcTemplate.queryForInt(replace("column", columns[i], selectSyncColumnLevelSql),
                    new Object[] { id, values[i] }), 1, "Table was not updated for column " + columns[i]);
        }
    }

    @Test(timeout = 30000)
    public void testSyncColumnLevelTogether() throws ParseException {
        logTestRunning();
        int id = 1;
        String[] columns = { "id", "string_value", "time_value", "date_value", "bigint_value", "decimal_value" };
        Object[] values = new Object[] { id, "moredata", getDate("2008-01-02 03:04:05"),
                getDate("2008-02-01 05:03:04"), 600, new BigDecimal("34.10") };

        // Null out columns, change all columns, sync all together
        rootJdbcTemplate.update(nullSyncColumnLevelSql, new Object[] { id });

        for (int i = 1; i < columns.length; i++) {
            rootJdbcTemplate.update(replace("column", columns[i], updateSyncColumnLevelSql), new Object[] { values[i],
                    id });
        }
        getClientEngine().pull();
    }

    @Test(timeout = 30000)
    public void testSyncColumnLevelFallback() throws ParseException {
        logTestRunning();
        int id = 1;
        String[] columns = { "id", "string_value", "time_value", "date_value", "bigint_value", "decimal_value" };
        Object[] values = new Object[] { id, "fallback on insert", getDate("2008-01-02 03:04:05"),
                getDate("2008-02-01 05:03:04"), 600, new BigDecimal("34.10") };

        // Force a fallback of an update to insert the row
        clientJdbcTemplate.update(deleteSyncColumnLevelSql, new Object[] { id });
        rootJdbcTemplate.update(replace("column", "string_value", updateSyncColumnLevelSql), new Object[] { values[1],
                id });
        getClientEngine().pull();

        for (int i = 1; i < columns.length; i++) {
            assertEquals(clientJdbcTemplate.queryForInt(replace("column", columns[i], selectSyncColumnLevelSql),
                    new Object[] { id, values[i] }), 1, "Table was not updated for column " + columns[i]);
        }
    }

    @Test(timeout = 30000)
    public void testSyncColumnLevelNoChange() throws ParseException {
        logTestRunning();
        int id = 1;

        // Change a column to the same value, which on some systems will be
        // captured
        rootJdbcTemplate.update(replace("column", "string_value", updateSyncColumnLevelSql),
                new Object[] { "same", id });
        rootJdbcTemplate.update(replace("column", "string_value", updateSyncColumnLevelSql),
                new Object[] { "same", id });
        clientJdbcTemplate.update(deleteSyncColumnLevelSql, new Object[] { id });
        getClientEngine().pull();
    }

    @Test
    public void testTargetTableNameSync() throws Exception {
        logTestRunning();
        Assert.assertEquals(0, clientJdbcTemplate.queryForInt("select count(*) from TEST_TARGET_TABLE_B"));
        rootJdbcTemplate.update("insert into TEST_TARGET_TABLE_A values('1','2')");
        getClientEngine().pull();
        Assert.assertEquals(1, clientJdbcTemplate.queryForInt("select count(*) from TEST_TARGET_TABLE_B"));
        Assert.assertEquals(0, clientJdbcTemplate.queryForInt("select count(*) from TEST_TARGET_TABLE_A"));
    }

    @Test
    public void testMaxRowsBeforeCommit() throws Exception {
        logTestRunning();
        IParameterService clientParameterService = (IParameterService) getClientEngine().getApplicationContext()
                .getBean(Constants.PARAMETER_SERVICE);
        long oldMaxRowsBeforeCommit = clientParameterService
                .getLong(ParameterConstants.DATA_LOADER_MAX_ROWS_BEFORE_COMMIT);
        clientParameterService.saveParameter(ParameterConstants.DATA_LOADER_MAX_ROWS_BEFORE_COMMIT, 5);
        int oldCount = clientJdbcTemplate.queryForInt("select count(*) from ONE_COLUMN_TABLE");
        IStatisticManager statisticManager = AppUtils.find(Constants.STATISTIC_MANAGER, getClientEngine());
        statisticManager.flush();
        Assert.assertEquals(0, statisticManager.getStatistic(StatisticNameConstants.INCOMING_MAX_ROWS_COMMITTED)
                .getCount());
        rootJdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection con) throws SQLException, DataAccessException {
                con.setAutoCommit(false);
                PreparedStatement stmt = con.prepareStatement("insert into ONE_COLUMN_TABLE values(?)");
                for (int i = 400; i < 450; i++) {
                    stmt.setInt(1, i);
                    Assert.assertEquals(1, stmt.executeUpdate());
                }
                con.commit();
                return null;
            }
        });
        int count = 0;
        do {
            if (count > 0) {
                logger
                        .warn("If you see this message more than once the root database isn't respecting the fact that auto commit is set to false!");
            }
            count++;
        } while (getClientEngine().pull());
        int newCount = clientJdbcTemplate.queryForInt("select count(*) from ONE_COLUMN_TABLE");
        Assert.assertEquals(50, newCount - oldCount);
        Assert.assertEquals(8, statisticManager.getStatistic(StatisticNameConstants.INCOMING_MAX_ROWS_COMMITTED)
                .getCount());
        statisticManager.getStatistic(StatisticNameConstants.INCOMING_MAX_ROWS_COMMITTED);
        clientParameterService.saveParameter(ParameterConstants.DATA_LOADER_MAX_ROWS_BEFORE_COMMIT,
                oldMaxRowsBeforeCommit);
    }

    @Test(timeout = 30000)
    public void cleanupAfterTests() {
        getClientEngine().pull();
        getClientEngine().purge();
        getRootEngine().purge();
    }

    private String replace(String prop, String replaceWith, String sourceString) {
        return StringUtils.replace(sourceString, "$(" + prop + ")", replaceWith);
    }

    private Date getDate(String dateString) throws ParseException {
        if (!getClientDbDialect().isDateOverrideToTimestamp() || !getRootDbDialect().isDateOverrideToTimestamp()) {
            return DateUtils.parseDate(dateString.split(" ")[0], new String[] { "yyyy-MM-dd" });
        } else {
            return DateUtils.parseDate(dateString, new String[] { "yyyy-MM-dd HH:mm:ss" });
        }
    }

    protected void testDeletes() {
    }

    protected void testMultiRowInsert() {
    }

    protected void testMultipleChannels() {
    }

    protected void testChannelInError() {
    }

    protected void testTableSyncConfigChangeForRoot() {
    }

    protected void testTableSyncConfigChangeForClient() {
    }

    protected void testDataChangeTableChangeDataChangeThenSync() {
    }

    protected void testTransactionalCommit() {
    }

    protected void testTransactionalCommitPastBatchBoundary() {
    }

    protected void testSyncingGlobalParametersFromRoot() {
    }

    protected void testRejectedRegistration() {
    }

}
