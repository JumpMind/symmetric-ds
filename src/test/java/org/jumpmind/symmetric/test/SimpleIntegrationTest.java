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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.test.ParameterizedSuite.ParameterExcluder;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jdbc.core.RowMapper;

public class SimpleIntegrationTest extends AbstractIntegrationTest {

    static final Log logger = LogFactory.getLog(SimpleIntegrationTest.class);

    static final String insertOrderHeaderSql = "insert into test_order_header (order_id, customer_id, status, deliver_date) values(?,?,?,?)";

    static final String updateOrderHeaderStatusSql = "update test_order_header set status = ? where order_id = ?";

    static final String selectOrderHeaderSql = "select order_id, customer_id, status, deliver_date from test_order_header where order_id = ?";

    static final String insertOrderDetailSql = "insert into test_order_detail (order_id, line_number, item_type, item_id, quantity, price) values(?,?,?,?,?,?)";

    static final String insertCustomerSql = "insert into test_customer (customer_id, name, is_active, address, city, state, zip, entry_time, notes, icon) values(?,?,?,?,?,?,?,?,?,?)";

    static final String insertTestTriggerTableSql = "insert into test_triggers_table (id, string_one_value, string_two_value) values(?,?,?)";

    static final String updateTestTriggerTableSql = "update test_triggers_table set string_one_value=?";

    static final String insertStoreStatusSql = "insert into test_store_status (store_id, register_id, status) values(?,?,?)";

    static final String updateStoreStatusSql = "update test_store_status set status = ? where store_id = ? and register_id = ?";

    static final String selectStoreStatusSql = "select status from test_store_status where store_id = ? and register_id = ?";

    static final byte[] BINARY_DATA = new byte[] { 0x01, 0x02, 0x03 };

    public SimpleIntegrationTest() throws Exception {
    }
    
    public SimpleIntegrationTest(String client, String root) throws Exception {
        super(client, root);
    }

    @Test(timeout = 30000)
    public void registerClientWithRoot() {
        getRootEngine().openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, TestConstants.TEST_CLIENT_EXTERNAL_ID);
        getClientEngine().start();
        Assert.assertTrue("The client did not register.", getClientEngine().isRegistered());
        IStatisticManager statMgr = (IStatisticManager) getClientEngine().getApplicationContext().getBean(
                Constants.STATISTIC_MANAGER);
        statMgr.flush();
    }

    @Test(timeout = 30000)
    public void initialLoad() {
        IDbDialect rootDialect = getRootDbDialect();
        rootJdbcTemplate.update(insertCustomerSql, new Object[] { 301, "Linus", "1", "42 Blanket Street",
                "Santa Claus", "IN", 90009, new Date(), "This is a test", BINARY_DATA });
        insertIntoTestTriggerTable(rootDialect, new Object[] { 1, "wow", "mom" });
        insertIntoTestTriggerTable(rootDialect, new Object[] { 2, "mom", "wow" });
        INodeService nodeService = (INodeService) getRootEngine().getApplicationContext().getBean(
                Constants.NODE_SERVICE);
        String nodeId = nodeService.findNodeByExternalId(TestConstants.TEST_CLIENT_NODE_GROUP,
                TestConstants.TEST_CLIENT_EXTERNAL_ID).getNodeId();
        getRootEngine().reloadNode(nodeId);
        IOutgoingBatchService outgoingBatchService = (IOutgoingBatchService) getRootEngine().getApplicationContext()
                .getBean(Constants.OUTGOING_BATCH_SERVICE);
        Assert.assertFalse(outgoingBatchService.isInitialLoadComplete(nodeId));
        getClientEngine().pull();
        Assert.assertTrue(outgoingBatchService.isInitialLoadComplete(nodeId));
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
        // test pulling no data
        getClientEngine().pull();

        final byte[] BIG_BINARY = new byte[200];
        for (int i = 0; i < BIG_BINARY.length; i++) {
            BIG_BINARY[i] = 0x01;
        }

        // now change some data that should be sync'd
        rootJdbcTemplate.update(insertCustomerSql, new Object[] { 101, "Charlie Brown", "1", "300 Grub Street",
                "New Yorl", "NY", 90009, new Date(), "This is a test", BIG_BINARY });

        getClientEngine().pull();
        assertEquals(clientJdbcTemplate.queryForInt("select count(*) from test_customer where customer_id=101"), 1,
                "The customer was not sync'd to the client." + printRootAndClientDatabases());

        if (getRootDbDialect().isClobSyncSupported()) {
            assertEquals(clientJdbcTemplate.queryForObject("select notes from test_customer where customer_id=101",
                    String.class), "This is a test", "The CLOB notes field on customer was not sync'd to the client.");
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
        turnOnNoKeysInUpdateParameter(true);
        Date date = DateUtils.parseDate("2007-01-03", new String[] { "yyyy-MM-dd" });
        clientJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "10", 100, null, date }, new int[] {
                Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
        clientJdbcTemplate.update(insertOrderDetailSql, new Object[] { "10", 1, "STK", "110000065", 3, 3.33 });
        getClientEngine().push();
    }

    @Test(timeout = 30000)
    public void syncInsertCondition() throws ParseException {
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

    @Test
    // (timeout = 30000)
    public void oneColumnTableWithPrimaryKeyUpdate() throws Exception {
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
        rootJdbcTemplate.update(updateOrderHeaderStatusSql, new Object[] { "I", "1" });
        getClientEngine().pull();
        assertEquals(clientJdbcTemplate.queryForList(selectOrderHeaderSql, new Object[] { "1" }).size(), 0,
                "The order record was sync'd when it should not have been.");

        rootJdbcTemplate.update(updateOrderHeaderStatusSql, new Object[] { "C", "1" });
        getClientEngine().pull();
        List list = clientJdbcTemplate.queryForList(selectOrderHeaderSql, new Object[] { "1" });
        assertEquals(list.size(), 1, "The order record should exist.");
        Map map = (Map) list.get(0);
        assertEquals(map.get("status"), "C", "Status should be complete");
        // TODO: make sure event did not fire
    }

    @Test(timeout = 30000)
    @SuppressWarnings("unchecked")
    public void ignoreNodeChannel() {
        INodeService nodeService = (INodeService) getRootEngine().getApplicationContext().getBean("nodeService");
        IConfigurationService configService = (IConfigurationService) getRootEngine().getApplicationContext().getBean(
                "configurationService");
        nodeService.ignoreNodeChannelForExternalId(true, TestConstants.TEST_CHANNEL_ID,
                TestConstants.TEST_ROOT_NODE_GROUP, TestConstants.TEST_ROOT_EXTERNAL_ID);
        configService.flushChannels();
        rootJdbcTemplate.update(insertCustomerSql, new Object[] { 201, "Charlie Dude", "1", "300 Grub Street",
                "New Yorl", "NY", 90009, new Date(), "This is a test", BINARY_DATA });
        getClientEngine().pull();
        assertEquals(clientJdbcTemplate.queryForInt("select count(*) from test_customer where customer_id=201"), 0,
                "The customer was sync'd to the client.");
        nodeService.ignoreNodeChannelForExternalId(false, TestConstants.TEST_CHANNEL_ID,
                TestConstants.TEST_ROOT_NODE_GROUP, TestConstants.TEST_ROOT_EXTERNAL_ID);
        configService.flushChannels();
    }

    @Test(timeout = 30000)
    @SuppressWarnings("unchecked")
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
        Thread.sleep(1000);
        getRootEngine().purge();
        getClientEngine().purge();
        assertEquals(rootJdbcTemplate.queryForInt("select count(*) from " + TestConstants.TEST_PREFIX + "data"), 0,
                "Expected all data rows to have been purged.");
        assertEquals(clientJdbcTemplate.queryForInt("select count(*) from " + TestConstants.TEST_PREFIX + "data"), 0,
                "Expected all data rows to have been purged.");

    }

    @Test(timeout = 30000)
    public void testHeartbeat() throws Exception {
        long ts = System.currentTimeMillis();
        Thread.sleep(1000);
        getClientEngine().heartbeat();
        getClientEngine().push();
        Date time = (Date) rootJdbcTemplate.queryForObject("select heartbeat_time from " + TestConstants.TEST_PREFIX
                + "node where external_id='" + TestConstants.TEST_CLIENT_EXTERNAL_ID + "'", Timestamp.class);
        Assert.assertTrue("The client node was not sync'd to the root as expected.", time != null
                && time.getTime() > ts);
    }

    @Test(timeout = 30000)
    public void testVirtualTransactionId() {
        rootJdbcTemplate.update("insert into test_very_long_table_name_1234 values('42')");
        if (getRootDbDialect().isTransactionIdOverrideSupported()) {
            assertEquals(rootJdbcTemplate.queryForObject(
                    "select transaction_id from sym_data_event where data_id in (select max(data_id) from sym_data)",
                    String.class), "42", "The hardcoded transaction id was not found.");
            Assert.assertEquals(rootJdbcTemplate.update("delete from test_very_long_table_name_1234 where id='42'"), 1);
            assertEquals(rootJdbcTemplate.queryForObject(
                    "select transaction_id from sym_data_event where data_id in (select max(data_id) from sym_data)",
                    String.class), "42", "The hardcoded transaction id was not found.");
        }
    }

    @Test(timeout = 30000)
    public void testCaseSensitiveTableNames() {
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
        assertEquals(clientJdbcTemplate.queryForInt("select count(*) from NO_PRIMARY_KEY_TABLE"),
                0, "Table was not deleted from");
        

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
