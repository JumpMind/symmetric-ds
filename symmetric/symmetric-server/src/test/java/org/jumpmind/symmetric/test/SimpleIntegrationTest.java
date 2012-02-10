package org.jumpmind.symmetric.test;

import java.util.Date;

import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricWebServer;
import org.jumpmind.symmetric.TestConstants;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.NodeStatus;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.junit.Assert;
import org.junit.Test;

public class SimpleIntegrationTest extends AbstractIntegrationTest {

    static final String THIS_IS_A_TEST = "This is a test";

    public static boolean testFlag = false;

    static final String insertOrderHeaderSql = "insert into test_order_header (order_id, customer_id, status, deliver_date) values(?,?,?,?)";

    static final String updateOrderHeaderStatusSql = "update test_order_header set status = ? where order_id = ?";

    static final String selectOrderHeaderSql = "select order_id, customer_id, status, deliver_date from test_order_header where order_id = ?";

    static final String insertOrderDetailSql = "insert into test_order_detail (order_id, line_number, item_type, item_id, quantity, price) values(?,?,?,?,?,?)";

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

    static final String makeHeartbeatOld = "update sym_node set heartbeat_time={ts '2000-01-01 01:01:01.000'} where node_id=?";

    static final String deleteNode = "delete from sym_node where node_id=?";

    static final byte[] BINARY_DATA = new byte[] { 0x01, 0x02, 0x03 };

    @Test
    public void createServer() {
        SymmetricWebServer server = getServer();
        Assert.assertNotNull(server);
        checkForFailedTriggers(true, false);

    }

    @Test(timeout = 120000)
    public void registerClientWithRoot() {
        logTestRunning();
        ISymmetricEngine rootEngine = getServer().getEngine();
        INodeService rootNodeService = rootEngine.getNodeService();
        rootEngine.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertTrue("The registration for the client should be opened now.", rootNodeService
                .findNodeSecurity(TestConstants.TEST_CLIENT_EXTERNAL_ID).isRegistrationEnabled());
        getClient().start();
        clientPull();
        Assert.assertTrue("The client did not register.", getClient().isRegistered());
        Assert.assertFalse("The registration for the client should be closed now.", rootNodeService
                .findNodeSecurity(TestConstants.TEST_CLIENT_EXTERNAL_ID).isRegistrationEnabled());
        IStatisticManager statMgr = getClient().getStatisticManager();
        statMgr.flush();

        checkForFailedTriggers(true, true);
    }

    @Test(timeout = 120000)
    public void initialLoad() {
        logTestRunning();
        serverTestService.insertIntoTestUseStreamLob(100, "test_use_stream_lob", THIS_IS_A_TEST);
        serverTestService.insertIntoTestUseStreamLob(100, "test_use_capture_lob", THIS_IS_A_TEST);

        Customer customer = new Customer(301, "Linus", true, "42 Blanket Street", "Santa Claus",
                "IN", 90009, new Date(), new Date(), THIS_IS_A_TEST, BINARY_DATA);
        serverTestService.insertCustomer(customer);

        serverTestService.insertIntoTestTriggerTable(new Object[] { 1, "wow", "mom" });

        serverTestService.insertIntoTestTriggerTable(new Object[] { 2, "mom", "wow" });

        INodeService rootNodeService = getServer().getEngine().getNodeService();
        INodeService clientNodeService = getClient().getNodeService();
        String nodeId = rootNodeService.findNodeByExternalId(TestConstants.TEST_CLIENT_NODE_GROUP,
                TestConstants.TEST_CLIENT_EXTERNAL_ID).getNodeId();

        getServer().getEngine().reloadNode(nodeId);
        IOutgoingBatchService rootOutgoingBatchService = getServer().getEngine()
                .getOutgoingBatchService();
        Assert.assertFalse(rootOutgoingBatchService.isInitialLoadComplete(nodeId));

        Assert.assertTrue(rootNodeService.findNodeSecurity(TestConstants.TEST_CLIENT_EXTERNAL_ID)
                .isInitialLoadEnabled());
        do {
            clientPull();
        } while (!rootOutgoingBatchService.isInitialLoadComplete(nodeId));

        Assert.assertFalse(rootNodeService.findNodeSecurity(TestConstants.TEST_CLIENT_EXTERNAL_ID)
                .isInitialLoadEnabled());

        NodeSecurity clientNodeSecurity = clientNodeService
                .findNodeSecurity(TestConstants.TEST_CLIENT_EXTERNAL_ID);

        Assert.assertFalse(clientNodeSecurity.isInitialLoadEnabled());
        Assert.assertNotNull(clientNodeSecurity.getInitialLoadTime());

        IIncomingBatchService clientIncomingBatchService = getClient().getIncomingBatchService();

        Assert.assertEquals("The initial load errored out." + printRootAndClientDatabases(), 0,
                clientIncomingBatchService.countIncomingBatchesInError());
        Assert.assertEquals(
                "test_triggers_table on the client did not contain the expected number of rows", 2,
                clientTestService.countTestTriggersTable());

        Assert.assertEquals(
                "test_customer on the client did not contain the expected number of rows", 2,
                clientTestService.countCustomers());
        Assert.assertEquals("Initial load was not successful according to the client",
                NodeStatus.DATA_LOAD_COMPLETED, clientNodeService.getNodeStatus());

        Assert.assertEquals("Initial load was not successful accordign to the root", false,
                rootNodeService.findNodeSecurity(TestConstants.TEST_CLIENT_EXTERNAL_ID)
                        .isInitialLoadEnabled());

        clientTestService.assertTestUseStreamBlobInDatabase(100, "test_use_stream_lob",
                THIS_IS_A_TEST);
        clientTestService.assertTestUseStreamBlobInDatabase(100, "test_use_capture_lob",
                THIS_IS_A_TEST);
    }

    @Test(timeout = 120000)
    public void syncToClient() {
        logTestRunning();

        // test pulling no data
        clientPull();

        final byte[] BIG_BINARY = new byte[200];
        for (int i = 0; i < BIG_BINARY.length; i++) {
            BIG_BINARY[i] = 0x01;
        }

        final String TEST_CLOB = "This is my test's test";
        // now change some data that should be sync'd

        serverTestService.insertCustomer(new Customer(101, "Charlie Brown", true,
                "300 Grub Street", "New Yorl", "NY", 90009, new Date(), new Date(), TEST_CLOB,
                BIG_BINARY));

        clientPull();

        Assert.assertTrue("The customer was not sync'd to the client."
                + printRootAndClientDatabases(), clientTestService.doesCustomerExist(101));

        if (getServer().getEngine().getSymmetricDialect().isClobSyncSupported()) {
            Assert.assertEquals("The CLOB notes field on customer was not sync'd to the client",
                    serverTestService.getCustomerNotes(101),
                    clientTestService.getCustomerNotes(101));
        }

        if (getServer().getEngine().getSymmetricDialect().isBlobSyncSupported()) {
            byte[] data = clientTestService.getCustomerIcon(101);
            Assert.assertTrue("The BLOB icon field on customer was not sync'd to the client",
                    ArrayUtils.isEquals(data, BIG_BINARY));
        }

    }

    @Test(timeout = 120000)
    public void syncToClientMultipleUpdates() {

        logTestRunning();
        // test pulling no data
        clientPull();

        final int NEW_ZIP = 44444;
        final String NEW_NAME = "JoJo Duh Doh";

        // now change some data that should be sync'd
        serverTestService.updateCustomer(100, "zip", NEW_ZIP);
        serverTestService.updateCustomer(100, "name", NEW_NAME);

        boolean didPullData = getClient().pull().wasDataProcessed();

        Assert.assertTrue(didPullData);

        Customer clientCustomer = clientTestService.getCustomer(100);
        Assert.assertEquals(NEW_ZIP, clientCustomer.getZip());
        Assert.assertEquals(NEW_NAME, clientCustomer.getName());

    }

    @Test(timeout = 120000)
    public void testInsertSqlEvent() {
        Assert.assertTrue(getClient().getSqlTemplate().queryForInt(
                "select count(*) from sym_node where schema_version='test'") == 0);
        getServer()
                .getEngine()
                .getDataService()
                .insertSqlEvent(TestConstants.TEST_CLIENT_NODE,
                        "update sym_node set schema_version='test'", false);
        clientPull();
        Assert.assertTrue(getClient().getSqlTemplate().queryForInt(
                "select count(*) from sym_node where schema_version='test'") > 0);
    }

    @Test(timeout = 120000)
    public void testEmptyNullLob() {
        Customer customer = new Customer(300, "Eric", true, "100 Main Street", "Columbus", "OH",
                43082, new Date(), new Date(), "", new byte[0]);

        serverTestService.insertCustomer(customer);

        clientPull();

        if (getServer().getEngine().getSymmetricDialect().isClobSyncSupported()) {
            if (DatabaseNamesConstants.INTERBASE.equals(getClient().getSymmetricDialect()
                    .getPlatform().getName())) {
                // Putting an empty string into a CLOB on Interbase results in a
                // NULL value
                Assert.assertNull("Expected null CLOB", clientTestService.getCustomerNotes(300));
            } else {
                Assert.assertEquals("Expected empty CLOB", "",
                        clientTestService.getCustomerNotes(300));
            }
        }

        if (getServer().getEngine().getSymmetricDialect().isBlobSyncSupported()) {
            byte[] bytes = clientTestService.getCustomerIcon(300);
            Assert.assertTrue("Expected empty BLOB", bytes != null && bytes.length == 0);
        }

        // Test null large object
        serverTestService.updateCustomer(300, "notes", null);
        serverTestService.updateCustomer(300, "icon", null);

        clientPull();

        if (getServer().getEngine().getSymmetricDialect().isClobSyncSupported()) {
            Assert.assertNull("Expected null CLOB", clientTestService.getCustomerNotes(300));
        }

        if (getServer().getEngine().getSymmetricDialect().isBlobSyncSupported()) {
            Assert.assertNull("Expected null BLOB", clientTestService.getCustomerIcon(300));
        }
    }

    // @Test(timeout = 120000)
    // public void testLargeLob() {
    // if (!(getRootDbDialect() instanceof OracleDbDialect)) {
    // return;
    // }
    // int blobType = Types.BINARY;
    // if (getRootDbDialect() instanceof OracleDbDialect) {
    // blobType = Types.BLOB;
    // }
    // String bigString = StringUtils.rightPad("Feeling tired... ", 6000, "Z");
    // Object[] args = new Object[] { 400, "Eric", "1", "100 Main Street",
    // "Columbus", "OH",
    // 43082, new Date(), new Date(), bigString, bigString.getBytes() };
    // int[] argTypes = new int[] { Types.INTEGER, Types.VARCHAR, Types.VARCHAR,
    // Types.VARCHAR,
    // Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.TIMESTAMP,
    // Types.TIMESTAMP,
    // Types.CLOB, blobType };
    //
    // rootJdbcTemplate.update(insertCustomerSql, new
    // ArgTypePreparedStatementSetter(args,
    // argTypes, getRootDbDialect().getLobHandler()));
    // clientPull();
    // }
    //
    // @Test(timeout = 120000)
    // public void testSuspendIgnorePushRemoteBatches() throws ParseException {
    //
    // // test suspend / ignore with remote database specifying the suspends
    // // and ignores
    // logTestRunning();
    // turnOnNoKeysInUpdateParameter(true);
    // Date date = DateUtils.parseDate("2007-01-03", new String[] { "yyyy-MM-dd"
    // });
    // clientJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "101",
    // 100, null, date },
    // new int[] { Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
    // clientJdbcTemplate.update(insertOrderDetailSql, new Object[] { "101", 1,
    // "STK",
    // "110000065", 3, 3.33 });
    //
    // boolean pushedData = clientPush();
    //
    // Assert.assertTrue("Client data was not batched and pushed", pushedData);
    // assertEquals(rootJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "101" })
    // .size(), 1,
    // "The inserted test_order_header client row was not synchronized.");
    //
    // IConfigurationService rootConfigurationService =
    // findOnRoot(Constants.CONFIG_SERVICE);
    // IOutgoingBatchService clientOutgoingBatchService =
    // findOnClient(Constants.OUTGOING_BATCH_SERVICE);
    //
    // NodeChannel c =
    // rootConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setSuspendEnabled(true);
    // rootConfigurationService.saveNodeChannel(c, true);
    //
    // date = DateUtils.parseDate("2007-01-03", new String[] { "yyyy-MM-dd" });
    // clientJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "102",
    // 100, null, date },
    // new int[] { Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
    // clientJdbcTemplate.update(insertOrderDetailSql, new Object[] { "102", 1,
    // "STK",
    // "110000065", 3, 3.33 });
    // clientPush();
    //
    // OutgoingBatches batches = clientOutgoingBatchService
    // .getOutgoingBatches(TestConstants.TEST_ROOT_NODE, false);
    //
    // assertEquals(batches.getBatches().size(), 1,
    // "There should be one outgoing batches.");
    // assertEquals(rootJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "102" })
    // .size(), 0, "The order record wasn't sync'd when it should have.");
    //
    // rootConfigurationService = findOnRoot(Constants.CONFIG_SERVICE);
    // c =
    // rootConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setIgnoreEnabled(true);
    // rootConfigurationService.saveNodeChannel(c, true);
    // clientPush();
    //
    // batches =
    // clientOutgoingBatchService.getOutgoingBatches(TestConstants.TEST_ROOT_NODE,
    // false);
    //
    // assertEquals(batches.getBatches().size(), 0,
    // "There should be no outgoing batches.");
    //
    // assertEquals(rootJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "102" })
    // .size(), 0, "The order record wasn't sync'd when it should have.");
    //
    // // Cleanup!
    // c =
    // rootConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setSuspendEnabled(false);
    // c.setIgnoreEnabled(false);
    // rootConfigurationService.saveNodeChannel(c, true);
    // clientPush();
    // }
    //
    // @Test(timeout = 120000)
    // public void testSuspendIgnorePushLocalBatches() throws ParseException {
    //
    // // test suspend / ignore with local database specifying the suspends
    // // and ignores
    // logTestRunning();
    // Date date = DateUtils.parseDate("2007-01-03", new String[] { "yyyy-MM-dd"
    // });
    // clientJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "105",
    // 100, null, date },
    // new int[] { Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
    // clientJdbcTemplate.update(insertOrderDetailSql, new Object[] { "105", 1,
    // "STK",
    // "110000065", 3, 3.33 });
    //
    // clientPush();
    //
    // assertEquals(rootJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "105" })
    // .size(), 1, "The order record wasn't sync'd when it should have.");
    //
    // IConfigurationService clientConfigurationService =
    // findOnClient(Constants.CONFIG_SERVICE);
    // IOutgoingBatchService clientOutgoingBatchService =
    // findOnClient(Constants.OUTGOING_BATCH_SERVICE);
    //
    // NodeChannel c =
    // clientConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setSuspendEnabled(true);
    // clientConfigurationService.saveNodeChannel(c, true);
    //
    // date = DateUtils.parseDate("2007-01-03", new String[] { "yyyy-MM-dd" });
    // clientJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "106",
    // 100, null, date },
    // new int[] { Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
    // clientJdbcTemplate.update(insertOrderDetailSql, new Object[] { "106", 1,
    // "STK",
    // "110000065", 3, 3.33 });
    // clientPush();
    //
    // OutgoingBatches batches = clientOutgoingBatchService
    // .getOutgoingBatches(TestConstants.TEST_ROOT_NODE, false);
    //
    // assertEquals(batches.getBatches().size(), 0,
    // "There should be no outgoing batches since suspended locally.");
    // assertEquals(rootJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "106" })
    // .size(), 0, "The order record wasn't sync'd when it should have.");
    //
    // c =
    // clientConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setIgnoreEnabled(true);
    // clientConfigurationService.saveNodeChannel(c, true);
    // clientPush();
    //
    // batches =
    // clientOutgoingBatchService.getOutgoingBatches(TestConstants.TEST_ROOT_NODE,
    // false);
    //
    // assertEquals(batches.getBatches().size(), 0,
    // "There should be no outgoing batches.");
    //
    // assertEquals(rootJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "106" })
    // .size(), 0, "The order record wasn't sync'd when it should have.");
    //
    // // Cleanup!
    // c =
    // clientConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setSuspendEnabled(false);
    // c.setIgnoreEnabled(false);
    // clientConfigurationService.saveNodeChannel(c, true);
    // clientPush();
    // }
    //
    // @Test(timeout = 120000)
    // public void testSuspendIgnorePullRemoteBatches() throws ParseException {
    //
    // // test suspend / ignore with remote database specifying the suspends
    // // and ignores
    //
    // logTestRunning();
    // // Should not sync when status = null
    // Date date = DateUtils.parseDate("2009-09-30", new String[] { "yyyy-MM-dd"
    // });
    // rootJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "42", 100,
    // "C", date },
    // new int[] { Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
    // clientPull();
    // IOutgoingBatchService rootOutgoingBatchService =
    // findOnRoot(Constants.OUTGOING_BATCH_SERVICE);
    // OutgoingBatches batches = rootOutgoingBatchService
    // .getOutgoingBatches(TestConstants.TEST_CLIENT_NODE, false);
    // assertEquals(clientJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "42" })
    // .size(), 1, "The order record wasn't sync'd when it should have.");
    // assertEquals(batches.getBatches().size(), 0,
    // "There should be no outgoing batches, yet I found some.");
    //
    // // Suspend the channel...
    //
    // IConfigurationService rootConfigurationService =
    // findOnRoot(Constants.CONFIG_SERVICE);
    // NodeChannel c =
    // rootConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setSuspendEnabled(true);
    // rootConfigurationService.saveNodeChannel(c, true);
    //
    // date = DateUtils.parseDate("2009-09-30", new String[] { "yyyy-MM-dd" });
    // rootJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "43", 100,
    // "C", date },
    // new int[] { Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
    //
    // clientPull();
    // batches =
    // rootOutgoingBatchService.getOutgoingBatches(TestConstants.TEST_CLIENT_NODE,
    // false);
    // assertEquals(batches.getBatches().size(), 1,
    // "There should be 1 outgoing batch.");
    // assertEquals(clientJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "43" })
    // .size(), 0, "The order record was sync'd when it shouldn't have.");
    //
    // c =
    // rootConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setIgnoreEnabled(true);
    // rootConfigurationService.saveNodeChannel(c, true);
    //
    // // ignore
    //
    // clientPull();
    // batches =
    // rootOutgoingBatchService.getOutgoingBatches(TestConstants.TEST_CLIENT_NODE,
    // false);
    // assertEquals(batches.getBatches().size(), 0,
    // "There should be no outgoing batches, but there was one.");
    // assertEquals(clientJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "43" })
    // .size(), 0, "The order record was sync'd when it shouldn't have.");
    //
    // // Cleanup!
    // c =
    // rootConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setSuspendEnabled(false);
    // c.setIgnoreEnabled(false);
    // rootConfigurationService.saveNodeChannel(c, true);
    //
    // clientPull();
    // batches =
    // rootOutgoingBatchService.getOutgoingBatches(TestConstants.TEST_CLIENT_NODE,
    // false);
    // assertEquals(batches.getBatches().size(), 0,
    // "There should be no outgoing batches, but there was one.");
    // assertEquals(clientJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "43" })
    // .size(), 0, "The order record was sync'd when it shouldn't have.");
    //
    // }
    //
    // @Test(timeout = 120000)
    // public void testSuspendIgnorePullRemoteLocalComboBatches() throws
    // ParseException {
    //
    // // test suspend / ignore with remote database specifying the suspends
    // // and ignores
    //
    // logTestRunning();
    // // Should not sync when status = null
    // Date date = DateUtils.parseDate("2009-09-30", new String[] { "yyyy-MM-dd"
    // });
    // rootJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "442", 100,
    // "C", date },
    // new int[] { Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
    // clientPull();
    // IOutgoingBatchService rootOutgoingBatchService =
    // findOnRoot(Constants.OUTGOING_BATCH_SERVICE);
    // OutgoingBatches batches = rootOutgoingBatchService
    // .getOutgoingBatches(TestConstants.TEST_CLIENT_NODE, false);
    // assertEquals(clientJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "442" })
    // .size(), 1, "The order record wasn't sync'd when it should have.");
    // assertEquals(batches.getBatches().size(), 0,
    // "There should be no outgoing batches, yet I found some.");
    //
    // IConfigurationService rootConfigurationService =
    // findOnRoot(Constants.CONFIG_SERVICE);
    // IConfigurationService clientConfigurationService =
    // findOnClient(Constants.CONFIG_SERVICE);
    //
    // // suspend on remote
    //
    // NodeChannel c =
    // rootConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setSuspendEnabled(true);
    // rootConfigurationService.saveNodeChannel(c, true);
    //
    // // ignore on local
    //
    // c =
    // clientConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setIgnoreEnabled(true);
    // clientConfigurationService.saveNodeChannel(c, true);
    //
    // date = DateUtils.parseDate("2009-09-30", new String[] { "yyyy-MM-dd" });
    // rootJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "443", 100,
    // "C", date },
    // new int[] { Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
    // clientPull();
    //
    // batches =
    // rootOutgoingBatchService.getOutgoingBatches(TestConstants.TEST_CLIENT_NODE,
    // false);
    // assertEquals(batches.getBatches().size(), 0,
    // "There should be 0 outgoing batch.");
    // assertEquals(clientJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "443" })
    // .size(), 0, "The order record was sync'd when it shouldn't have.");
    //
    // // ignore on remote
    //
    // c =
    // rootConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setIgnoreEnabled(true);
    // c.setSuspendEnabled(false);
    // rootConfigurationService.saveNodeChannel(c, true);
    //
    // // suspend on local
    //
    // c =
    // clientConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setIgnoreEnabled(false);
    // c.setSuspendEnabled(true);
    // clientConfigurationService.saveNodeChannel(c, true);
    //
    // date = DateUtils.parseDate("2009-09-30", new String[] { "yyyy-MM-dd" });
    // rootJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "444", 100,
    // "C", date },
    // new int[] { Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
    // clientPull();
    //
    // batches =
    // rootOutgoingBatchService.getOutgoingBatches(TestConstants.TEST_CLIENT_NODE,
    // false);
    // assertEquals(batches.getBatches().size(), 0,
    // "There should be 0 outgoing batch.");
    // assertEquals(clientJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "444" })
    // .size(), 0, "The order record was sync'd when it shouldn't have.");
    //
    // // Cleanup!
    // c =
    // rootConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setSuspendEnabled(false);
    // c.setIgnoreEnabled(false);
    // rootConfigurationService.saveNodeChannel(c, true);
    //
    // c =
    // clientConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setSuspendEnabled(false);
    // c.setIgnoreEnabled(false);
    // clientConfigurationService.saveNodeChannel(c, true);
    //
    // clientPull();
    //
    // }
    //
    // @Test(timeout = 120000)
    // public void testUpdateDataWithNoChangesSyncToClient() throws Exception {
    // int clientIncomingBatchCount = clientJdbcTemplate
    // .queryForInt("select count(*) from sym_incoming_batch");
    // int rowsUpdated = rootJdbcTemplate
    // .update("update test_sync_column_level set string_value=string_value");
    // logger.info("Updated " + rowsUpdated + " test_sync_column_level rows");
    // clientPull();
    // Assert.assertTrue(rowsUpdated > 0);
    // Assert.assertTrue(clientIncomingBatchCount <= clientJdbcTemplate
    // .queryForInt("select count(*) from sym_incoming_batch"));
    // Assert.assertEquals(0, clientJdbcTemplate
    // .queryForInt("select count(*) from sym_incoming_batch where status != 'OK'"));
    // Assert.assertEquals(
    // 0,
    // rootJdbcTemplate
    // .queryForInt("select count(*) from sym_outgoing_batch where status not in ('OK','IG','SK')"));
    // Assert.assertEquals(
    // rootJdbcTemplate.queryForInt("select count(*) from test_sync_column_level"),
    // clientJdbcTemplate.queryForInt("select count(*) from test_sync_column_level"));
    // }
    //
    // @Test(timeout = 120000)
    // public void testSuspendIgnorePullLocalBatches() throws ParseException {
    //
    // // test suspend / ignore with local database specifying suspends and
    // // ignores
    //
    // logTestRunning();
    // // Should not sync when status = null
    // Date date = DateUtils.parseDate("2009-09-30", new String[] { "yyyy-MM-dd"
    // });
    // rootJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "44", 100,
    // "C", date },
    // new int[] { Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
    // clientPull();
    // IOutgoingBatchService rootOutgoingBatchService =
    // findOnRoot(Constants.OUTGOING_BATCH_SERVICE);
    // OutgoingBatches batches = rootOutgoingBatchService
    // .getOutgoingBatches(TestConstants.TEST_CLIENT_NODE, false);
    // assertEquals(batches.getBatches().size(), 0,
    // "There should be no outgoing batches, yet I found some.");
    // assertEquals(clientJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "44" })
    // .size(), 1, "The order record wasn't sync'd when it should have.");
    //
    // // Suspend the channel...
    //
    // IConfigurationService clientConfigurationService =
    // findOnClient(Constants.CONFIG_SERVICE);
    // NodeChannel c =
    // clientConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setSuspendEnabled(true);
    // clientConfigurationService.saveNodeChannel(c, true);
    //
    // date = DateUtils.parseDate("2009-09-30", new String[] { "yyyy-MM-dd" });
    // rootJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "45", 100,
    // "C", date },
    // new int[] { Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
    //
    // clientPull();
    // batches =
    // rootOutgoingBatchService.getOutgoingBatches(TestConstants.TEST_CLIENT_NODE,
    // false);
    // assertEquals(batches.getBatches().size(), 1,
    // "There should be 1 outgoing batch.");
    // assertEquals(clientJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "45" })
    // .size(), 0, "The order record was sync'd when it shouldn't have.");
    //
    // c =
    // clientConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setIgnoreEnabled(true);
    // clientConfigurationService.saveNodeChannel(c, true);
    //
    // // ignore
    //
    // clientPull();
    // batches =
    // rootOutgoingBatchService.getOutgoingBatches(TestConstants.TEST_CLIENT_NODE,
    // false);
    // assertEquals(batches.getBatches().size(), 0,
    // "There should be no outgoing batches, but there was one.");
    // assertEquals(clientJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "45" })
    // .size(), 0, "The order record was sync'd when it shouldn't have.");
    //
    // // Cleanup!
    //
    // c =
    // clientConfigurationService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
    // c.setSuspendEnabled(false);
    // c.setIgnoreEnabled(false);
    // clientConfigurationService.saveNodeChannel(c, true);
    //
    // clientPull();
    // batches =
    // rootOutgoingBatchService.getOutgoingBatches(TestConstants.TEST_CLIENT_NODE,
    // false);
    // assertEquals(batches.getBatches().size(), 0,
    // "There should be no outgoing batches, but there was one.");
    // assertEquals(clientJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "45" })
    // .size(), 0, "The order record was sync'd when it shouldn't have.");
    //
    // }
    //
    // @Test(timeout = 120000)
    // public void syncToRootAutoGeneratedPrimaryKey() {
    // logTestRunning();
    // final String NEW_VALUE = "unique new value one value";
    // IDbDialect clientDialect = getClientDbDialect();
    // insertIntoTestTriggerTable(clientJdbcTemplate, clientDialect, new
    // Object[] { 3,
    // "value one", "value \" two" });
    // clientPush();
    // clientJdbcTemplate.update(updateTestTriggerTableSql, new Object[] {
    // NEW_VALUE });
    // final String verifySql =
    // "select count(*) from test_triggers_table where string_one_value=?";
    // Assert.assertEquals(3,
    // clientJdbcTemplate.queryForInt(verifySql, new Object[] { NEW_VALUE }));
    // clientPush();
    // Assert.assertEquals(3, rootJdbcTemplate.queryForInt(verifySql, new
    // Object[] { NEW_VALUE }));
    // }
    //
    // @Test(timeout = 120000)
    // public void reopenRegistration() {
    // logTestRunning();
    // getRootEngine().reOpenRegistration(TestConstants.TEST_CLIENT_EXTERNAL_ID);
    // clientPull();
    // Assert.assertEquals(1, rootJdbcTemplate
    // .queryForInt(isRegistrationClosedSql,
    // new Object[] { TestConstants.TEST_CLIENT_EXTERNAL_ID },
    // new int[] { Types.VARCHAR }));
    // }
    //
    // private void assertEquals(Object actual, Object expected, String
    // failureMessage) {
    // Assert.assertEquals(failureMessage, expected, actual);
    // }
    //
    // private boolean turnOnNoKeysInUpdateParameter(boolean newValue) {
    // IParameterService clientParameterService = (IParameterService)
    // getClient()
    // .getApplicationContext().getBean(Constants.PARAMETER_SERVICE);
    // IParameterService rootParameterService = (IParameterService)
    // AppUtils.find(
    // Constants.PARAMETER_SERVICE, getRootEngine());
    // Assert.assertEquals(
    // clientParameterService.is(ParameterConstants.DATA_LOADER_NO_KEYS_IN_UPDATE),
    // rootParameterService.is(ParameterConstants.DATA_LOADER_NO_KEYS_IN_UPDATE));
    // boolean oldValue = clientParameterService
    // .is(ParameterConstants.DATA_LOADER_NO_KEYS_IN_UPDATE);
    // clientParameterService.saveParameter(ParameterConstants.DATA_LOADER_NO_KEYS_IN_UPDATE,
    // newValue);
    // rootParameterService.saveParameter(ParameterConstants.DATA_LOADER_NO_KEYS_IN_UPDATE,
    // newValue);
    // return oldValue;
    // }
    //
    // @Test(timeout = 120000)
    // public void syncToRoot() throws ParseException {
    // logTestRunning();
    // turnOnNoKeysInUpdateParameter(true);
    // Date date = DateUtils.parseDate("2007-01-03", new String[] { "yyyy-MM-dd"
    // });
    // clientJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "10", 100,
    // null, date },
    // new int[] { Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
    // clientJdbcTemplate.update(insertOrderDetailSql, new Object[] { "10", 1,
    // "STK", "110000065",
    // 3, 3.33 });
    // clientPush();
    // }
    //
    // @Test(timeout = 120000)
    // public void syncInsertCondition() throws ParseException {
    // logTestRunning();
    // // Should not sync when status = null
    // Date date = DateUtils.parseDate("2007-01-02", new String[] { "yyyy-MM-dd"
    // });
    // rootJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "11", 100,
    // null, date },
    // new int[] { Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
    // clientPull();
    //
    // IOutgoingBatchService outgoingBatchService =
    // findOnRoot(Constants.OUTGOING_BATCH_SERVICE);
    // OutgoingBatches batches = outgoingBatchService
    // .getOutgoingBatches(TestConstants.TEST_CLIENT_NODE, false);
    // assertEquals(batches.getBatches().size(), 0,
    // "There should be no outgoing batches, yet I found some.");
    //
    // assertEquals(clientJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "11" })
    // .size(), 0, "The order record was sync'd when it should not have been.");
    //
    // // Should sync when status = C
    // rootJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "12", 100,
    // "C", date },
    // new int[] { Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
    // clientPull();
    // assertEquals(clientJdbcTemplate.queryForList(selectOrderHeaderSql, new
    // Object[] { "12" })
    // .size(), 1, "The order record was not sync'd when it should have been.");
    // // TODO: make sure event did not fire
    // }
    //
    // @Test(timeout = 120000)
    // public void oneColumnTableWithPrimaryKeyUpdate() throws Exception {
    // logTestRunning();
    // boolean oldValue = turnOnNoKeysInUpdateParameter(true);
    // rootJdbcTemplate.update("insert into one_column_table values(1)");
    // Assert.assertTrue(clientJdbcTemplate
    // .queryForInt("select count(*) from one_column_table where my_one_column=1")
    // == 0);
    // clientPull();
    // Assert.assertTrue(clientJdbcTemplate
    // .queryForInt("select count(*) from one_column_table where my_one_column=1")
    // == 1);
    // rootJdbcTemplate
    // .update("update one_column_table set my_one_column=1 where my_one_column=1");
    // clientPull();
    // IOutgoingBatchService outgoingBatchService =
    // findOnRoot(Constants.OUTGOING_BATCH_SERVICE);
    // OutgoingBatches batches = outgoingBatchService
    // .getOutgoingBatches(TestConstants.TEST_CLIENT_NODE, false);
    // assertEquals(batches.getBatches().size(), 0,
    // "There should be no outgoing batches, yet I found some.");
    // turnOnNoKeysInUpdateParameter(oldValue);
    // }
    //
    // @Test(timeout = 120000)
    // public void syncUpdateCondition() {
    // logTestRunning();
    // rootJdbcTemplate.update(updateOrderHeaderStatusSql, new Object[] { "C",
    // "1" });
    // clientPull();
    // List<Map<String, Object>> list =
    // clientJdbcTemplate.queryForList(selectOrderHeaderSql,
    // new Object[] { "1" });
    // assertEquals(list.size(), 1, "The order record should exist.");
    // Map<String, Object> map = list.get(0);
    // assertEquals(map.get("status"), "C", "Status should be complete");
    // // TODO: make sure event did not fire
    // }
    //
    // @Test(timeout = 120000)
    // public void ignoreNodeChannel() {
    // logTestRunning();
    // INodeService rootNodeService = (INodeService)
    // AppUtils.find(Constants.NODE_SERVICE,
    // getRootEngine());
    // IConfigurationService rootConfigService = (IConfigurationService)
    // getRootEngine()
    // .getConfigurationService();
    // rootNodeService.ignoreNodeChannelForExternalId(true,
    // TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_ROOT_NODE_GROUP, TestConstants.TEST_ROOT_EXTERNAL_ID);
    // rootConfigService.reloadChannels();
    //
    // NodeChannel channel =
    // rootConfigService.getNodeChannel(TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_ROOT_EXTERNAL_ID, false);
    // Assert.assertNotNull(channel);
    // Assert.assertTrue(channel.isIgnoreEnabled());
    // Assert.assertFalse(channel.isSuspendEnabled());
    //
    // rootJdbcTemplate.update(insertCustomerSql, new Object[] { 201,
    // "Charlie Dude", "1",
    // "300 Grub Street", "New Yorl", "NY", 90009, new Date(), new Date(),
    // THIS_IS_A_TEST,
    // BINARY_DATA });
    // clientPull();
    // assertEquals(
    // clientJdbcTemplate
    // .queryForInt("select count(*) from test_customer where customer_id=201"),
    // 0, "The customer was sync'd to the client.");
    // rootNodeService.ignoreNodeChannelForExternalId(false,
    // TestConstants.TEST_CHANNEL_ID,
    // TestConstants.TEST_ROOT_NODE_GROUP, TestConstants.TEST_ROOT_EXTERNAL_ID);
    // rootConfigService.reloadChannels();
    //
    // clientPull();
    // getClient().getConfigurationService().reloadChannels();
    // }
    //
    // @Test(timeout = 120000)
    // public void syncUpdateWithEmptyKey() {
    // logTestRunning();
    // try {
    // if (getClientDbDialect().isEmptyStringNulled()) {
    // return;
    // }
    //
    // clientJdbcTemplate.update(insertStoreStatusSql, new Object[] { "00001",
    // "", 1 });
    // clientPush();
    //
    // clientJdbcTemplate.update(updateStoreStatusSql, new Object[] { 2,
    // "00001", "" });
    // clientPush();
    //
    // int status = rootJdbcTemplate.queryForInt(selectStoreStatusSql, new
    // Object[] { "00001",
    // "   " });
    // assertEquals(status, 2, "Wrong store status");
    // } finally {
    // logTestComplete();
    // }
    // }
    //
    // @Test(timeout = 120000)
    // public void testPurge() throws Exception {
    // logTestRunning();
    //
    // // do an extra push & pull to make sure we have events cleared out
    // clientPull();
    // clientPush();
    //
    // Thread.sleep(2000);
    //
    // IParameterService parameterService =
    // AppUtils.find(Constants.PARAMETER_SERVICE,
    // getRootEngine());
    // int purgeRetentionMinues = parameterService
    // .getInt(ParameterConstants.PURGE_RETENTION_MINUTES);
    // // set purge in the future just in case the database time is different
    // // than the current time
    // parameterService.saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES,
    // -60 * 24);
    //
    // int beforePurge =
    // rootJdbcTemplate.queryForInt("select count(*) from sym_data");
    // getRootEngine().purge();
    // int afterPurge =
    // rootJdbcTemplate.queryForInt("select count(*) from sym_data");
    // Timestamp maxCreateTime = (Timestamp) rootJdbcTemplate.queryForObject(
    // "select max(create_time) from sym_data", Timestamp.class);
    // Timestamp minCreateTime = (Timestamp) rootJdbcTemplate.queryForObject(
    // "select min(create_time) from sym_data", Timestamp.class);
    // Assert.assertTrue("Expected data rows to have been purged at the root.  There were "
    // + beforePurge + " row before anf " + afterPurge
    // + " rows after. The max create_time in sym_data was " + maxCreateTime
    // + " and the min create_time in sym_data was " + minCreateTime
    // + " and the current time of the server is " + new Date(),
    // (beforePurge - afterPurge) > 0);
    //
    // parameterService.saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES,
    // purgeRetentionMinues);
    //
    // parameterService = AppUtils.find(Constants.PARAMETER_SERVICE,
    // getClient());
    // purgeRetentionMinues =
    // parameterService.getInt(ParameterConstants.PURGE_RETENTION_MINUTES);
    // // set purge in the future just in case the database time is different
    // // than the current time
    // parameterService.saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES,
    // -60 * 24);
    //
    // beforePurge =
    // clientJdbcTemplate.queryForInt("select count(*) from sym_data");
    // getClient().purge();
    // afterPurge =
    // clientJdbcTemplate.queryForInt("select count(*) from sym_data");
    // maxCreateTime = (Timestamp) clientJdbcTemplate.queryForObject(
    // "select max(create_time) from sym_data", Timestamp.class);
    // minCreateTime = (Timestamp) clientJdbcTemplate.queryForObject(
    // "select min(create_time) from sym_data", Timestamp.class);
    // Assert.assertTrue("Expected data rows to have been purged at the client.  There were "
    // + beforePurge + " row before anf " + afterPurge
    // + " rows after. . The max create_time in sym_data was " + maxCreateTime
    // + " and the min create_time in sym_data was " + minCreateTime
    // + " and the current time of the server is " + new Date(),
    // (beforePurge - afterPurge) > 0);
    //
    // parameterService.saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES,
    // purgeRetentionMinues);
    // }
    //
    // @Test(timeout = 120000)
    // public void testHeartbeat() throws Exception {
    // logTestRunning();
    // final String checkHeartbeatSql =
    // "select heartbeat_time from sym_node where external_id='"
    // + TestConstants.TEST_CLIENT_EXTERNAL_ID + "'";
    // Date clientHeartbeatTimeBefore =
    // clientJdbcTemplate.queryForObject(checkHeartbeatSql,
    // Timestamp.class);
    // Thread.sleep(1000);
    // getClient().heartbeat(true);
    // Date clientHeartbeatTimeAfter =
    // clientJdbcTemplate.queryForObject(checkHeartbeatSql,
    // Timestamp.class);
    // Assert.assertNotSame("The heartbeat time was not updated at the client",
    // clientHeartbeatTimeAfter, clientHeartbeatTimeBefore);
    // Date rootHeartbeatTimeBefore =
    // rootJdbcTemplate.queryForObject(checkHeartbeatSql,
    // Timestamp.class);
    // Assert.assertNotSame(
    // "The root heartbeat time should not be the same as the updated client heartbeat time",
    // clientHeartbeatTimeAfter, rootHeartbeatTimeBefore);
    // while (clientPush()) {
    // // continue to push while there data to push
    // }
    // Date rootHeartbeatTimeAfter =
    // rootJdbcTemplate.queryForObject(checkHeartbeatSql,
    // Timestamp.class);
    // Assert.assertEquals(
    // "The client heartbeat time should have been the same as the root heartbeat time.",
    // clientHeartbeatTimeAfter, rootHeartbeatTimeAfter);
    // }
    //
    // @Test(timeout = 120000)
    // public void testVirtualTransactionId() {
    // logTestRunning();
    // rootJdbcTemplate.update("insert into test_very_long_table_name_1234 values('42')");
    // if (getRootDbDialect().isTransactionIdOverrideSupported()) {
    // assertEquals(
    // rootJdbcTemplate.queryForObject(
    // "select transaction_id from sym_data where data_id in (select max(data_id) from sym_data)",
    // String.class), "42", "The hardcoded transaction id was not found.");
    // Assert.assertEquals(rootJdbcTemplate
    // .update("delete from test_very_long_table_name_1234 where id='42'"), 1);
    // assertEquals(
    // rootJdbcTemplate.queryForObject(
    // "select transaction_id from sym_data where data_id in (select max(data_id) from sym_data)",
    // String.class), "42", "The hardcoded transaction id was not found.");
    // }
    // }
    //
    // @Test(timeout = 120000)
    // public void testCaseSensitiveTableNames() {
    // logTestRunning();
    // String rquote =
    // getRootDbDialect().getPlatform().isDelimitedIdentifierModeOn() ?
    // getRootDbDialect()
    // .getPlatform().getPlatformInfo().getDelimiterToken()
    // : "";
    // String cquote =
    // getClientDbDialect().getPlatform().isDelimitedIdentifierModeOn() ?
    // getClientDbDialect()
    // .getPlatform().getPlatformInfo().getDelimiterToken()
    // : "";
    // rootJdbcTemplate.update("insert into " + rquote + "TEST_ALL_CAPS" +
    // rquote
    // + " values(1, 'HELLO')");
    // clientPull();
    // assertEquals(
    // clientJdbcTemplate.queryForInt("select count(*) from " + cquote +
    // "TEST_ALL_CAPS"
    // + cquote + " where " + cquote + "ALL_CAPS_ID" + cquote + " = 1"), 1,
    // "Table name in all caps was not synced");
    // rootJdbcTemplate.update("insert into " + rquote + "Test_Mixed_Case" +
    // rquote
    // + " values(1, 'Hello')");
    // clientPull();
    // assertEquals(
    // clientJdbcTemplate.queryForInt("select count(*) from " + cquote +
    // "Test_Mixed_Case"
    // + cquote + " where " + cquote + "Mixed_Case_Id" + cquote + " = 1"), 1,
    // "Table name in mixed case was not synced");
    // }
    //
    // @Test(timeout = 120000)
    // public void testSyncShellCommand() throws Exception {
    // logTestRunning();
    // IDataService rootDataService = AppUtils.find(Constants.DATA_SERVICE,
    // getRootEngine());
    // IOutgoingBatchService rootOutgoingBatchService = AppUtils.find(
    // Constants.OUTGOING_BATCH_SERVICE, getRootEngine());
    // testFlag = false;
    // String scriptData =
    // "org.jumpmind.symmetric.test.SimpleIntegrationTest.testFlag=true;";
    // rootDataService.sendScript(TestConstants.TEST_CLIENT_EXTERNAL_ID,
    // scriptData, false);
    // clientPull();
    // OutgoingBatches batches = rootOutgoingBatchService
    // .getOutgoingBatches(TestConstants.TEST_CLIENT_NODE, false);
    // Assert.assertEquals(0, batches.countBatches(true));
    // Assert.assertTrue(testFlag);
    // }
    //
    // @Test(timeout = 120000)
    // public void testSyncShellCommandError() throws Exception {
    // logTestRunning();
    // Level old = setLoggingLevelForTest(Level.OFF);
    // IDataService rootDataService = AppUtils.find(Constants.DATA_SERVICE,
    // getRootEngine());
    // IOutgoingBatchService rootOutgoingBatchService = AppUtils.find(
    // Constants.OUTGOING_BATCH_SERVICE, getRootEngine());
    // testFlag = false;
    // String scriptData =
    // "org.jumpmind.symmetric.test.SimpleIntegrationTest.nonExistentFlag=true;";
    // rootDataService.sendScript(TestConstants.TEST_CLIENT_EXTERNAL_ID,
    // scriptData, false);
    // clientPull();
    // OutgoingBatches batches = rootOutgoingBatchService
    // .getOutgoingBatches(TestConstants.TEST_CLIENT_NODE, false);
    // Assert.assertEquals(1, batches.countBatches(true));
    // Assert.assertFalse(testFlag);
    // rootOutgoingBatchService.markAllAsSentForNode(TestConstants.TEST_CLIENT_NODE);
    // setLoggingLevelForTest(old);
    // }
    //
    // /**
    // * TODO test on MSSQL
    // */
    // @Test(timeout = 120000)
    // @ParameterExcluder("mssql")
    // public void testNoPrimaryKeySync() {
    // logTestRunning();
    // rootJdbcTemplate.update("insert into no_primary_key_table values(1, 2, 'HELLO')");
    // clientPull();
    // assertEquals(
    // clientJdbcTemplate
    // .queryForInt("select two_column from no_primary_key_table where one_column=1"),
    // 2, "Table was not synced");
    // rootJdbcTemplate.update("update no_primary_key_table set two_column=3 where one_column=1");
    // clientPull();
    // assertEquals(
    // clientJdbcTemplate
    // .queryForInt("select two_column from no_primary_key_table where one_column=1"),
    // 3, "Table was not updated");
    // rootJdbcTemplate.update("delete from no_primary_key_table");
    // clientPull();
    // assertEquals(clientJdbcTemplate.queryForInt("select count(*) from no_primary_key_table"),
    // 0, "Table was not deleted from");
    // }
    //
    // @Test(timeout = 120000)
    // public void testReservedColumnNames() {
    // logTestRunning();
    // if (getRootDbDialect() instanceof Db2DbDialect
    // || getClientDbDialect() instanceof Db2DbDialect
    // || getRootDbDialect() instanceof FirebirdDbDialect
    // || getClientDbDialect() instanceof FirebirdDbDialect
    // || getRootDbDialect() instanceof InterbaseDbDialect
    // || getRootDbDialect() instanceof InformixDbDialect) {
    // return;
    // }
    // // alter the table to have column names that are not usually allowed
    // String rquote = getRootDbDialect().getIdentifierQuoteString();
    // String cquote = getClientDbDialect().getIdentifierQuoteString();
    // rootJdbcTemplate.update(alterKeyWordSql.replaceAll("\"", rquote));
    // rootJdbcTemplate.update(alterKeyWordSql2.replaceAll("\"", rquote));
    // clientJdbcTemplate.update(alterKeyWordSql.replaceAll("\"", cquote));
    // clientJdbcTemplate.update(alterKeyWordSql2.replaceAll("\"", cquote));
    //
    // getClientDbDialect().resetCachedTableModel();
    // getRootDbDialect().resetCachedTableModel();
    //
    // // enable the trigger for the table and update the client with
    // // configuration
    // rootJdbcTemplate.update(enableKeyWordTriggerSql);
    // getRootEngine().syncTriggers();
    // getRootEngine().reOpenRegistration(TestConstants.TEST_CLIENT_EXTERNAL_ID);
    // clientPull();
    //
    // rootJdbcTemplate.update(insertKeyWordSql.replaceAll("\"", rquote), new
    // Object[] { 1, "x",
    // "a" });
    // clientPull();
    //
    // rootJdbcTemplate.update(updateKeyWordSql.replaceAll("\"", rquote), new
    // Object[] { "y", "b",
    // 1 });
    // clientPull();
    //
    // List<Map<String, Object>> rowList = clientJdbcTemplate.queryForList(
    // selectKeyWordSql.replaceAll("\"", cquote), new Object[] { 1 });
    // Assert.assertTrue(rowList.size() > 0);
    // Map<String, Object> columnMap = rowList.get(0);
    // assertEquals(columnMap.get("key word"), "y",
    // "Wrong key word value in table");
    // assertEquals(columnMap.get("case"), "b", "Wrong case value in table");
    // }
    //
    // @Test(timeout = 120000)
    // public void testSyncColumnLevel() throws ParseException {
    // logTestRunning();
    // int id = 1;
    // String[] columns = { "id", "string_value", "time_value", "date_value",
    // "bigint_value",
    // "decimal_value" };
    // Object[] values = new Object[] { id, "moredata",
    // getDate("2007-01-02 03:04:05"),
    // getDate("2007-02-01 05:03:04"), 600, new BigDecimal("34.10") };
    //
    // // Null out columns, change each column and sync one at a time
    // clientJdbcTemplate.update(nullSyncColumnLevelSql, new Object[] { id });
    //
    // for (int i = 1; i < columns.length; i++) {
    // rootJdbcTemplate.update(replace("column", columns[i],
    // updateSyncColumnLevelSql),
    // new Object[] { values[i], id });
    // clientPull();
    // assertEquals(
    // clientJdbcTemplate.queryForInt(
    // replace("column", columns[i], selectSyncColumnLevelSql), new Object[] {
    // id, values[i] }), 1, "Table was not updated for column "
    // + columns[i]);
    // }
    // }
    //
    // @Test(timeout = 120000)
    // public void testSyncColumnLevelTogether() throws ParseException {
    // logTestRunning();
    // int id = 1;
    // String[] columns = { "id", "string_value", "time_value", "date_value",
    // "bigint_value",
    // "decimal_value" };
    // Object[] values = new Object[] { id, "moredata",
    // getDate("2008-01-02 03:04:05"),
    // getDate("2008-02-01 05:03:04"), 600, new BigDecimal("34.10") };
    //
    // // Null out columns, change all columns, sync all together
    // rootJdbcTemplate.update(nullSyncColumnLevelSql, new Object[] { id });
    //
    // for (int i = 1; i < columns.length; i++) {
    // rootJdbcTemplate.update(replace("column", columns[i],
    // updateSyncColumnLevelSql),
    // new Object[] { values[i], id });
    // }
    // clientPull();
    // }
    //
    // @Test(timeout = 120000)
    // public void testSyncColumnLevelFallback() throws ParseException {
    // logTestRunning();
    // int id = 1;
    // String[] columns = { "id", "string_value", "time_value", "date_value",
    // "bigint_value",
    // "decimal_value" };
    // Object[] values = new Object[] { id, "fallback on insert",
    // getDate("2008-01-02 03:04:05"),
    // getDate("2008-02-01 05:03:04"), 600, new BigDecimal("34.10") };
    //
    // // Force a fallback of an update to insert the row
    // clientJdbcTemplate.update(deleteSyncColumnLevelSql, new Object[] { id });
    // rootJdbcTemplate.update(replace("column", "string_value",
    // updateSyncColumnLevelSql),
    // new Object[] { values[1], id });
    // clientPull();
    //
    // for (int i = 1; i < columns.length; i++) {
    // assertEquals(
    // clientJdbcTemplate.queryForInt(
    // replace("column", columns[i], selectSyncColumnLevelSql), new Object[] {
    // id, values[i] }), 1, "Table was not updated for column "
    // + columns[i]);
    // }
    // }
    //
    // @Test(timeout = 120000)
    // public void testSyncColumnLevelNoChange() throws ParseException {
    // logTestRunning();
    // int id = 1;
    //
    // // Change a column to the same value, which on some systems will be
    // // captured
    // rootJdbcTemplate.update(replace("column", "string_value",
    // updateSyncColumnLevelSql),
    // new Object[] { "same", id });
    // rootJdbcTemplate.update(replace("column", "string_value",
    // updateSyncColumnLevelSql),
    // new Object[] { "same", id });
    // clientJdbcTemplate.update(deleteSyncColumnLevelSql, new Object[] { id });
    // clientPull();
    // }
    //
    // @Test(timeout = 120000)
    // public void testTargetTableNameSync() throws Exception {
    // logTestRunning();
    // Assert.assertEquals(0,
    // clientJdbcTemplate.queryForInt("select count(*) from test_target_table_b"));
    // rootJdbcTemplate.update("insert into test_target_table_a values('1','2')");
    // clientPull();
    // Assert.assertEquals(1,
    // clientJdbcTemplate.queryForInt("select count(*) from test_target_table_b"));
    // Assert.assertEquals(0,
    // clientJdbcTemplate.queryForInt("select count(*) from test_target_table_a"));
    // }
    //
    // @Test(timeout = 120000)
    // public void testMaxRowsBeforeCommit() throws Exception {
    // logTestRunning();
    // IParameterService clientParameterService = (IParameterService)
    // getClient()
    // .getApplicationContext().getBean(Constants.PARAMETER_SERVICE);
    // long oldMaxRowsBeforeCommit = clientParameterService
    // .getLong(ParameterConstants.DATA_LOADER_MAX_ROWS_BEFORE_COMMIT);
    // clientParameterService.saveParameter(ParameterConstants.DATA_LOADER_MAX_ROWS_BEFORE_COMMIT,
    // 5);
    // int oldCount =
    // clientJdbcTemplate.queryForInt("select count(*) from one_column_table");
    // IStatisticManager statisticManager =
    // AppUtils.find(Constants.STATISTIC_MANAGER,
    // getClient());
    // statisticManager.flush();
    // rootJdbcTemplate.execute(new ConnectionCallback<Object>() {
    // public Object doInConnection(Connection con) throws SQLException,
    // DataAccessException {
    // con.setAutoCommit(false);
    // PreparedStatement stmt = con
    // .prepareStatement("insert into one_column_table values(?)");
    // for (int i = 400; i < 450; i++) {
    // stmt.setInt(1, i);
    // Assert.assertEquals(1, stmt.executeUpdate());
    // }
    // con.commit();
    // return null;
    // }
    // });
    // int count = 0;
    // do {
    // if (count > 0) {
    // logger.warn("If you see this message more than once the root database isn't respecting the fact that auto commit is set to false!");
    // }
    // count++;
    // } while (getClient().pull().wasDataProcessed());
    // int newCount =
    // clientJdbcTemplate.queryForInt("select count(*) from one_column_table");
    // Assert.assertEquals(50, newCount - oldCount);
    // clientParameterService.saveParameter(ParameterConstants.DATA_LOADER_MAX_ROWS_BEFORE_COMMIT,
    // oldMaxRowsBeforeCommit);
    // }
    //
    // @Test(timeout = 120000)
    // public void testLobSyncUsingStreaming() throws Exception {
    // String text =
    // "Another test.  Should not find this in text in sym_data, but it should be in the client database";
    // if (insertIntoTestUseStreamLob(200, "test_use_stream_lob", text)) {
    // String rowData = rootJdbcTemplate
    // .queryForObject(
    // "select row_data from sym_data where data_id = (select max(data_id) from sym_data)",
    // String.class);
    // Assert.assertTrue("Did not find the id in the row data",
    // rowData.contains("200"));
    // Assert.assertEquals("\"200\",,,,,,", rowData);
    // clientPull();
    // assertTestUseStreamBlobInClientDatabase(200, "test_use_stream_lob",
    // text);
    // }
    // }
    //
    // @Test(timeout = 120000)
    // public void testLobSyncUsingCapture() throws Exception {
    // String text =
    // "Another test.  Should not find this in text in sym_data, but it should be in the client database";
    // if (insertIntoTestUseStreamLob(200, "test_use_capture_lob", text)) {
    // String rowData = rootJdbcTemplate
    // .queryForObject(
    // "select row_data from sym_data where data_id = (select max(data_id) from sym_data)",
    // String.class);
    // Assert.assertTrue("Did not find the id in the row data",
    // rowData.contains("200"));
    // clientPull();
    // assertTestUseStreamBlobInClientDatabase(200, "test_use_capture_lob",
    // text);
    //
    // String updateText = "The text was updated";
    // updateTestUseStreamLob(200, "test_use_capture_lob", updateText);
    // clientPull();
    // assertTestUseStreamBlobInClientDatabase(200, "test_use_capture_lob",
    // updateText);
    // }
    // }
    //
    // @Test(timeout = 120000)
    // public void testSyncDisabled() {
    // clientPull();
    //
    // logTestRunning();
    //
    // Node clientIdentity = getClient().getNodeService().findIdentity();
    // Node clientNodeOnRoot = getRootEngine().getNodeService().findNode(
    // clientIdentity.getNodeId());
    //
    // Assert.assertEquals(true, clientNodeOnRoot.isSyncEnabled());
    //
    // // Update the heartbeat to be an old timestamp so the node will go
    // // offline
    // rootJdbcTemplate.update(makeHeartbeatOld, new Object[] {
    // clientIdentity.getNodeId() });
    // // Run the service to look for offline nodes
    // getRootEngine().getNodeService().checkForOfflineNodes();
    //
    // // Verify syncing was disabled for the node
    // clientNodeOnRoot =
    // getRootEngine().getNodeService().findNode(clientIdentity.getNodeId());
    // Assert.assertEquals(false, clientNodeOnRoot.isSyncEnabled());
    //
    // // Verify node security was deleted for the node
    // NodeSecurity clientNodeSecurity =
    // getRootEngine().getNodeService().findNodeSecurity(
    // clientIdentity.getNodeId());
    // Assert.assertNull(clientNodeSecurity);
    //
    // // A client pull will result in client getting a SyncDisabled return
    // // code which will cause the client's identity to be removed.
    // getClient().pull();
    // Node clientNodeAfterPull = getClient().getNodeService().findIdentity();
    // Assert.assertNull(clientNodeAfterPull);
    //
    // // Turn auto registration and reload on so the client will register and
    // // reload
    // getClient().getParameterService().saveParameter("auto.registration",
    // true);
    // getClient().getParameterService().saveParameter("auto.reload", true);
    // getRootEngine().getParameterService().saveParameter("auto.registration",
    // true);
    // getRootEngine().getParameterService().saveParameter("auto.reload", true);
    //
    // // A pull will cause the registration to occur and the node identify to
    // // be reestablished.
    // getClient().pull();
    // clientNodeAfterPull = getClient().getNodeService().findIdentity();
    // Assert.assertNotNull(clientNodeAfterPull);
    // }
    //
    // @Test(timeout = 120000)
    // public void testClientNodeNotRegistered() throws ParseException {
    // logTestRunning();
    //
    // Node clientIdentity = getClient().getNodeService().findIdentity();
    // Node clientNodeOnRoot = getRootEngine().getNodeService().findNode(
    // clientIdentity.getNodeId());
    // // Remove the client node from sym_node and sym_node_security
    // rootJdbcTemplate.update(deleteNode, new Object[] {
    // clientNodeOnRoot.getNodeId() });
    // getRootEngine().getNodeService().deleteNodeSecurity(clientNodeOnRoot.getNodeId());
    //
    // // Turn auto registration and reload on so the client will register and
    // // reload
    // getRootEngine().getParameterService().saveParameter("auto.registration",
    // true);
    // getRootEngine().getParameterService().saveParameter("auto.reload", true);
    //
    // turnOnNoKeysInUpdateParameter(true);
    // Date date = DateUtils.parseDate("2007-01-03", new String[] { "yyyy-MM-dd"
    // });
    // clientJdbcTemplate.update(insertOrderHeaderSql, new Object[] { "99", 100,
    // null, date },
    // new int[] { Types.VARCHAR, Types.INTEGER, Types.CHAR, Types.DATE });
    // clientJdbcTemplate.update(insertOrderDetailSql, new Object[] { "99", 1,
    // "STK", "110000099",
    // 3, 3.33 });
    //
    // // The push will cause a Registration Required return code which will
    // // cause the the client identify to be removed
    // // in order to force registration.
    // clientPush();
    //
    // // A pull will cause the registration to occur and the node identify to
    // // be
    // // reestablished.
    // clientPull();
    //
    // Node clientNodeAfterPull = getClient().getNodeService().findIdentity();
    // Assert.assertNotNull(clientNodeAfterPull);
    // }
    //
    // @Test(timeout = 120000)
    // public void flushStatistics() {
    // IStatisticManager statMgr = (IStatisticManager)
    // getClient().getApplicationContext()
    // .getBean(Constants.STATISTIC_MANAGER);
    // statMgr.flush();
    //
    // statMgr = (IStatisticManager)
    // getRootEngine().getApplicationContext().getBean(
    // Constants.STATISTIC_MANAGER);
    // statMgr.flush();
    // }
    //
    // @Test(timeout = 120000)
    // public void cleanupAfterTests() {
    // clientPull();
    // getClient().purge();
    // getRootEngine().purge();
    // }
    //
    // private String replace(String prop, String replaceWith, String
    // sourceString) {
    // return StringUtils.replace(sourceString, "$(" + prop + ")", replaceWith);
    // }
    //
    // private Date getDate(String dateString) throws ParseException {
    // if (!getClientDbDialect().isDateOverrideToTimestamp()
    // || !getRootDbDialect().isDateOverrideToTimestamp()) {
    // return DateUtils.parseDate(dateString.split(" ")[0], new String[] {
    // "yyyy-MM-dd" });
    // } else {
    // return DateUtils.parseDate(dateString, new String[] {
    // "yyyy-MM-dd HH:mm:ss" });
    // }
    // }
    //

}
