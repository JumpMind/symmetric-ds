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
package org.jumpmind.symmetric.service.impl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.derby.DerbyDbDialect;
import org.jumpmind.symmetric.db.mssql.MsSqlDbDialect;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.route.ChannelRouterContext;
import org.jumpmind.symmetric.route.DataRefRouteReader;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Test;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;

public class RouterServiceTest extends AbstractDatabaseTest {

    private static final String SELECT_COUNT_FROM_SYM_OUTGOING_BATCH_WHERE_NOT = "select count(*) from sym_outgoing_batch where status = 'NE' and node_id!=?";
    private static final String SELECT_COUNT_FROM_SYM_OUTGOING_BATCH = "select count(*) from sym_outgoing_batch where status = 'NE' and node_id=?";
    final static String TEST_TABLE_1 = "test_routing_data_1";
    final static String TEST_TABLE_2 = "test_routing_data_2";
    final static String TEST_SUBTABLE = "test_routing_data_subtable";

    final static Node NODE_GROUP_NODE_1 = new Node("00001", TestConstants.TEST_CLIENT_NODE_GROUP);
    final static Node NODE_GROUP_NODE_2 = new Node("00002", TestConstants.TEST_CLIENT_NODE_GROUP);
    final static Node NODE_GROUP_NODE_3 = new Node("00003", TestConstants.TEST_CLIENT_NODE_GROUP);
    final static Node NODE_UNROUTED = new Node(Constants.UNROUTED_NODE_ID, null);

    public RouterServiceTest() throws Exception {
    }

    @Test
    public void testMultiChannelRoutingToEveryone() {
        resetBatches();

        TriggerRouter trigger1 = getTestRoutingTableTrigger(TEST_TABLE_1);
        getTriggerRouterService().saveTriggerRouter(trigger1);
        TriggerRouter trigger2 = getTestRoutingTableTrigger(TEST_TABLE_2);
        getTriggerRouterService().saveTriggerRouter(trigger2);
        getTriggerRouterService().syncTriggers();
        NodeChannel testChannel = getConfigurationService().getNodeChannel(
                TestConstants.TEST_CHANNEL_ID, false);
        NodeChannel otherChannel = getConfigurationService().getNodeChannel(
                TestConstants.TEST_CHANNEL_ID_OTHER, false);
        Assert.assertEquals(50, testChannel.getMaxBatchSize());
        Assert.assertEquals(1, otherChannel.getMaxBatchSize());
        // should be 1 batch for table 1 on the testchannel w/ max batch size of
        // 50
        insert(TEST_TABLE_1, 5, false);
        // this should generate 15 batches because the max batch size is 1
        insert(TEST_TABLE_2, 15, false);
        insert(TEST_TABLE_1, 50, true);

        getRouterService().routeData();

        final int EXPECTED_BATCHES = getDbDialect().supportsTransactionId() ? 16 : 17;

        OutgoingBatches batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1);
        filterForChannels(batches, testChannel, otherChannel);
        Assert.assertEquals(EXPECTED_BATCHES, batches.getBatches().size());
        Assert.assertEquals(getDbDialect().supportsTransactionId() ? 1 : 2,
                countBatchesForChannel(batches, testChannel));
        Assert.assertEquals(15, countBatchesForChannel(batches, otherChannel));

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_2);
        filterForChannels(batches, testChannel, otherChannel);
        // Node 2 has sync disabled
        Assert.assertEquals(0, batches.getBatches().size());

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_3);
        filterForChannels(batches, testChannel, otherChannel);
        Assert.assertEquals(EXPECTED_BATCHES, batches.getBatches().size());

        resetBatches();

        // should be 2 batches for table 1 on the testchannel w/ max batch size
        // of 50
        insert(TEST_TABLE_1, 50, false);
        // this should generate 1 batches because the max batch size is 1, but
        // the batch is transactional
        insert(TEST_TABLE_2, 15, true);
        insert(TEST_TABLE_1, 50, false);
        getRouterService().routeData();

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1);
        filterForChannels(batches, testChannel, otherChannel);
        Assert.assertEquals(getDbDialect().supportsTransactionId() ? 3 : 17, batches.getBatches()
                .size());
        Assert.assertEquals(2, countBatchesForChannel(batches, testChannel));
        Assert.assertEquals(getDbDialect().supportsTransactionId() ? 1 : 15,
                countBatchesForChannel(batches, otherChannel));
    }

    @Test
    public void testLookupTableRouting() {

        getDbDialect().truncateTable("test_lookup_table");

        getJdbcTemplate().update("insert into test_lookup_table values ('A',?)",
                NODE_GROUP_NODE_1.getExternalId());
        getJdbcTemplate().update("insert into test_lookup_table values ('B',?)",
                NODE_GROUP_NODE_1.getExternalId());
        getJdbcTemplate().update("insert into test_lookup_table values ('C',?)",
                NODE_GROUP_NODE_3.getExternalId());
        getJdbcTemplate().update("insert into test_lookup_table values ('D',?)",
                NODE_GROUP_NODE_3.getExternalId());
        getJdbcTemplate().update("insert into test_lookup_table values ('D',?)",
                NODE_GROUP_NODE_1.getExternalId());

        TriggerRouter triggerRouter = getTestRoutingTableTrigger(TEST_TABLE_1);
        triggerRouter.getRouter().setRouterType("lookuptable");
        triggerRouter
                .getRouter()
                .setRouterExpression(
                        "LOOKUP_TABLE=test_lookup_table\nKEY_COLUMN=ROUTING_VARCHAR\nLOOKUP_KEY_COLUMN=COLUMN_ONE\nEXTERNAL_ID_COLUMN=COLUMN_TWO");
        getTriggerRouterService().saveTriggerRouter(triggerRouter);
        getTriggerRouterService().syncTriggers();

        getRouterService().routeData();

        resetBatches();

        insert(TEST_TABLE_1, 5, true, null, "A");

        int unroutedCount = countUnroutedBatches();

        getRouterService().routeData();

        Assert.assertEquals(
                1,
                getJdbcTemplate().queryForInt(SELECT_COUNT_FROM_SYM_OUTGOING_BATCH,
                        NODE_GROUP_NODE_1.getNodeId()));
        Assert.assertEquals(
                0,
                getJdbcTemplate().queryForInt(SELECT_COUNT_FROM_SYM_OUTGOING_BATCH_WHERE_NOT,
                        NODE_GROUP_NODE_1.getNodeId()));
        Assert.assertEquals(unroutedCount, countUnroutedBatches());

        resetBatches();

        insert(TEST_TABLE_1, 5, true, null, "B");

        getRouterService().routeData();

        Assert.assertEquals(
                1,
                getJdbcTemplate().queryForInt(SELECT_COUNT_FROM_SYM_OUTGOING_BATCH,
                        NODE_GROUP_NODE_1.getNodeId()));
        Assert.assertEquals(
                0,
                getJdbcTemplate().queryForInt(SELECT_COUNT_FROM_SYM_OUTGOING_BATCH_WHERE_NOT,
                        NODE_GROUP_NODE_1.getNodeId()));
        Assert.assertEquals(unroutedCount, countUnroutedBatches());

        resetBatches();

        insert(TEST_TABLE_1, 10, true, null, "C");

        getRouterService().routeData();

        Assert.assertEquals(
                1,
                getJdbcTemplate().queryForInt(SELECT_COUNT_FROM_SYM_OUTGOING_BATCH,
                        NODE_GROUP_NODE_3.getNodeId()));
        Assert.assertEquals(
                0,
                getJdbcTemplate().queryForInt(SELECT_COUNT_FROM_SYM_OUTGOING_BATCH_WHERE_NOT,
                        NODE_GROUP_NODE_3.getNodeId()));
        Assert.assertEquals(unroutedCount, countUnroutedBatches());

        resetBatches();

        insert(TEST_TABLE_1, 5, true, null, "D");

        getRouterService().routeData();

        Assert.assertEquals(
                1,
                getJdbcTemplate().queryForInt(SELECT_COUNT_FROM_SYM_OUTGOING_BATCH,
                        NODE_GROUP_NODE_1.getNodeId()));
        Assert.assertEquals(
                1,
                getJdbcTemplate().queryForInt(SELECT_COUNT_FROM_SYM_OUTGOING_BATCH,
                        NODE_GROUP_NODE_3.getNodeId()));
        Assert.assertEquals(
                0,
                getJdbcTemplate()
                        .queryForInt(
                                "select count(*) from sym_outgoing_batch where status = 'NE' and node_id not in (?,?)",
                                NODE_GROUP_NODE_1.getNodeId(), NODE_GROUP_NODE_3.getNodeId()));
        Assert.assertEquals(unroutedCount, countUnroutedBatches());

        resetBatches();

        insert(TEST_TABLE_1, 1, true, null, "F");

        getRouterService().routeData();

        Assert.assertEquals(
                0,
                getJdbcTemplate().queryForInt(SELECT_COUNT_FROM_SYM_OUTGOING_BATCH,
                        NODE_GROUP_NODE_1.getNodeId()));
        Assert.assertEquals(
                0,
                getJdbcTemplate().queryForInt(SELECT_COUNT_FROM_SYM_OUTGOING_BATCH,
                        NODE_GROUP_NODE_3.getNodeId()));
        Assert.assertEquals(1, countUnroutedBatches() - unroutedCount);

        resetBatches();

    }

    @Test
    public void testColumnMatchTransactionalOnlyRoutingToNode1() {
        resetBatches();

        TriggerRouter trigger1 = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger1.getRouter().setRouterType("column");
        trigger1.getRouter().setRouterExpression("ROUTING_VARCHAR=:NODE_ID");
        getTriggerRouterService().saveTriggerRouter(trigger1);
        getTriggerRouterService().syncTriggers();
        NodeChannel testChannel = getConfigurationService().getNodeChannel(
                TestConstants.TEST_CHANNEL_ID, false);
        testChannel.setMaxBatchToSend(100);
        testChannel.setBatchAlgorithm("transactional");
        getConfigurationService().saveChannel(testChannel, true);

        // should be 51 batches for table 1
        insert(TEST_TABLE_1, 500, true);
        insert(TEST_TABLE_1, 50, false);
        getRouterService().routeData();

        final int EXPECTED_BATCHES = getDbDialect().supportsTransactionId() ? 51 : 100;

        OutgoingBatches batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1);
        filterForChannels(batches, testChannel);
        Assert.assertEquals(EXPECTED_BATCHES, batches.getBatches().size());
        Assert.assertEquals(EXPECTED_BATCHES, countBatchesForChannel(batches, testChannel));

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_2);
        filterForChannels(batches, testChannel);
        // Node 2 has sync disabled
        Assert.assertEquals(0, batches.getBatches().size());

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_3);
        filterForChannels(batches, testChannel);
        // Batch was targeted only at node 1
        Assert.assertEquals(0, batches.getBatches().size());

        resetBatches();

        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));
        execute("delete from " + TEST_TABLE_1, true, null);
        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));
        getRouterService().routeData();
        Assert.assertEquals(
                getDbDialect().supportsTransactionId() ? 1 : 100,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));

        resetBatches();

    }

    @Test
    public void testSubSelectNonTransactionalRoutingToNode1() {
        resetBatches();

        TriggerRouter trigger1 = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger1.getRouter().setRouterType("subselect");
        trigger1.getRouter().setRouterExpression("c.node_id=:ROUTING_VARCHAR");
        getTriggerRouterService().saveTriggerRouter(trigger1);
        getTriggerRouterService().syncTriggers();
        NodeChannel testChannel = getConfigurationService().getNodeChannel(
                TestConstants.TEST_CHANNEL_ID, false);
        testChannel.setMaxBatchToSend(1000);
        testChannel.setMaxBatchSize(5);
        testChannel.setBatchAlgorithm("nontransactional");
        getConfigurationService().saveChannel(testChannel, true);

        // should be 100 batches for table 1, even though we committed the
        // changes as part of a transaction
        insert(TEST_TABLE_1, 500, true);
        getRouterService().routeData();

        final int EXPECTED_BATCHES = 100;

        OutgoingBatches batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1);
        filterForChannels(batches, testChannel);
        Assert.assertEquals(EXPECTED_BATCHES, batches.getBatches().size());
        Assert.assertEquals(EXPECTED_BATCHES, countBatchesForChannel(batches, testChannel));

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_2);
        filterForChannels(batches, testChannel);
        // Node 2 has sync disabled
        Assert.assertEquals(0, batches.getBatches().size());

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_3);
        filterForChannels(batches, testChannel);
        // Batch was targeted only at node 1
        Assert.assertEquals(0, batches.getBatches().size());

        resetBatches();
    }

    @Test
    public void testSyncIncomingBatch() throws Exception {
        resetBatches();

        TriggerRouter trigger1 = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger1.getTrigger().setSyncOnIncomingBatch(true);
        trigger1.getRouter().setRouterExpression(null);
        trigger1.getRouter().setRouterType(null);
        getTriggerRouterService().saveTriggerRouter(trigger1);

        NodeChannel testChannel = getConfigurationService().getNodeChannel(
                TestConstants.TEST_CHANNEL_ID, false);
        testChannel.setMaxBatchToSend(1000);
        testChannel.setMaxBatchSize(50);
        testChannel.setBatchAlgorithm("default");
        getConfigurationService().saveChannel(testChannel, true);

        getTriggerRouterService().syncTriggers();

        insert(TEST_TABLE_1, 10, true, NODE_GROUP_NODE_1.getNodeId());

        getRouterService().routeData();

        OutgoingBatches batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1);
        filterForChannels(batches, testChannel);
        Assert.assertEquals(
                "Should have been 0.  We did the insert as if the data had come from node 1.", 0,
                batches.getBatches().size());

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_3);
        filterForChannels(batches, testChannel);
        Assert.assertEquals(1, batches.getBatches().size());

        resetBatches();

    }

    @Test
    public void testUseOfDefaultChannelDuringCreate() throws Exception {
        testUseOfDefaultChannel(false);
    }

    @Test
    public void testUseOfDefaultChannelBecauseOfDelete() throws Exception {
        testUseOfDefaultChannel(true);
    }

    protected void testUseOfDefaultChannel(boolean deleteChannel) throws Exception {
        Channel unrecognizedChannel = new Channel("gobble-t-gook", 1);
        resetBatches();
        if (deleteChannel) {
            getConfigurationService().saveChannel(unrecognizedChannel, true);
        }
        
        NodeChannel otherChannel = getConfigurationService().getNodeChannel(
                TestConstants.TEST_CHANNEL_ID_OTHER, false);
        otherChannel.setBatchAlgorithm("nontransactional");
        otherChannel.setMaxBatchSize(100);
        getConfigurationService().saveChannel(otherChannel, true);       
        
        TriggerRouter trigger1 = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger1.getRouter().setRouterType("default");
        trigger1.getTrigger().setChannelId(unrecognizedChannel.getChannelId());
        getTriggerRouterService().saveTriggerRouter(trigger1);
        
        TriggerRouter trigger2 = getTestRoutingTableTrigger(TEST_TABLE_2);
        trigger2.getRouter().setRouterType("default");
        getTriggerRouterService().saveTriggerRouter(trigger2);
        
        getTriggerRouterService().syncTriggers();

        if (deleteChannel) {
            getConfigurationService().deleteChannel(unrecognizedChannel);
        }
        try {

            // get rid of config channel change
            getRouterService().routeData();
            resetBatches();

            insert(TEST_TABLE_1, 10, true, null);
            getRouterService().routeData();

            insert(TEST_TABLE_2, 10, true, null);
            getRouterService().routeData();
            
            insert(TEST_TABLE_2, 10, true, null);
            getRouterService().routeData();
            getRouterService().routeData();

            OutgoingBatches batches = getOutgoingBatchService().getOutgoingBatches(
                    NODE_GROUP_NODE_1);
            Assert.assertEquals(3, batches.getBatches().size());
            Assert.assertEquals(Constants.CHANNEL_DEFAULT, batches.getBatches().get(2)
                    .getChannelId());
        } finally {
            trigger1.getTrigger().setChannelId(TestConstants.TEST_CHANNEL_ID);
            getTriggerRouterService().saveTriggerRouter(trigger1);
            getTriggerRouterService().syncTriggers();
            resetBatches();
        }

    }

    // @Test
    public void testLargeNumberOfEventsToManyNodes() {
        resetBatches();

        TriggerRouter trigger1 = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger1.getRouter().setRouterType("column");
        // set up a constant to force the data to be routed through the column
        // data matcher, but to everyone
        trigger1.getRouter().setRouterExpression("ROUTING_VARCHAR=00001");
        getTriggerRouterService().saveTriggerRouter(trigger1);
        getTriggerRouterService().syncTriggers();

        NodeChannel testChannel = getConfigurationService().getNodeChannel(
                TestConstants.TEST_CHANNEL_ID, false);
        testChannel.setMaxBatchToSend(100);
        testChannel.setMaxBatchSize(10000);
        testChannel.setBatchAlgorithm("default");
        getConfigurationService().saveChannel(testChannel, true);
        final int ROWS_TO_INSERT = 1000;
        final int NODES_TO_INSERT = 10;
        logger.info(String.format("About to insert %s nodes", NODES_TO_INSERT));
        for (int i = 0; i < 1000; i++) {
            String nodeId = String.format("100%s", i);
            getRegistrationService().openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, nodeId);
            Node node = getNodeService().findNode(nodeId);
            node.setSyncEnabled(true);
            getNodeService().updateNode(node);
        }
        logger.info(String.format("Done inserting %s nodes", NODES_TO_INSERT));

        logger.info(String.format("About to insert %s rows", ROWS_TO_INSERT));
        insert(TEST_TABLE_1, ROWS_TO_INSERT, false);
        logger.info(String.format("Done inserting %s rows", ROWS_TO_INSERT));

        logger.info("About to route data");
        getRouterService().routeData();
        logger.info("Done routing data");
    }

    @Test
    public void testBshTransactionalRoutingOnUpdate() {
        resetBatches();

        TriggerRouter trigger1 = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger1.getRouter().setRouterType("bsh");
        trigger1.getRouter().setRouterExpression(
                "targetNodes.add(ROUTING_VARCHAR); targetNodes.add(OLD_ROUTING_VARCHAR);");

        getTriggerRouterService().saveTriggerRouter(trigger1);
        getTriggerRouterService().syncTriggers();

        NodeChannel testChannel = getConfigurationService().getNodeChannel(
                TestConstants.TEST_CHANNEL_ID, false);
        testChannel.setMaxBatchToSend(1000);
        testChannel.setMaxBatchSize(5);
        testChannel.setBatchAlgorithm("transactional");
        getConfigurationService().saveChannel(testChannel, true);

        long ts = System.currentTimeMillis();
        TransactionCallback<Integer> callback = new TransactionCallback<Integer>() {
            public Integer doInTransaction(TransactionStatus status) {
                SimpleJdbcTemplate t = new SimpleJdbcTemplate(getJdbcTemplate());
                return t.update(String.format("update %s set ROUTING_VARCHAR=?", TEST_TABLE_1),
                        NODE_GROUP_NODE_3.getNodeId());
            }
        };
        int count = (Integer) getTransactionTemplate().execute(callback);
        logger.info("Just recorded a change to " + count + " rows in " + TEST_TABLE_1 + " in "
                + (System.currentTimeMillis() - ts) + "ms");
        ts = System.currentTimeMillis();
        getRouterService().routeData();
        logger.info("Just routed " + count + " rows in " + TEST_TABLE_1 + " in "
                + (System.currentTimeMillis() - ts) + "ms");

        OutgoingBatches batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1);
        filterForChannels(batches, testChannel);
        Assert.assertEquals(getDbDialect().supportsTransactionId() ? 1 : 530, batches.getBatches()
                .size());
        Assert.assertEquals(getDbDialect().supportsTransactionId() ? count : 1, (int) batches
                .getBatches().get(0).getDataEventCount());

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_2);
        filterForChannels(batches, testChannel);
        // Node 2 has sync disabled
        Assert.assertEquals(0, batches.getBatches().size());

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_3);
        filterForChannels(batches, testChannel);
        Assert.assertEquals(getDbDialect().supportsTransactionId() ? 1 : 530, batches.getBatches()
                .size());
        Assert.assertEquals(getDbDialect().supportsTransactionId() ? count : 1, (int) batches
                .getBatches().get(0).getDataEventCount());

        resetBatches();
    }

    @Test
    public void testBshRoutingDeletesToNode3() {
        resetBatches();

        TriggerRouter trigger1 = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger1.getRouter().setRouterType("bsh");
        trigger1.getRouter().setRouterExpression(
                "targetNodes.add(ROUTING_VARCHAR); targetNodes.add(OLD_ROUTING_VARCHAR);");
        getTriggerRouterService().saveTriggerRouter(trigger1);
        getTriggerRouterService().syncTriggers();
        NodeChannel testChannel = getConfigurationService().getNodeChannel(
                TestConstants.TEST_CHANNEL_ID, false);
        testChannel.setMaxBatchToSend(100);
        final int MAX_BATCH_SIZE = 100;
        testChannel.setMaxBatchSize(MAX_BATCH_SIZE);
        testChannel.setBatchAlgorithm("nontransactional");
        getConfigurationService().saveChannel(testChannel, true);

        TransactionCallback<Integer> callback = new TransactionCallback<Integer>() {
            public Integer doInTransaction(TransactionStatus status) {
                SimpleJdbcTemplate t = new SimpleJdbcTemplate(getJdbcTemplate());
                return t.update(String.format("delete from %s", TEST_TABLE_1));
            }
        };
        int count = (Integer) getTransactionTemplate().execute(callback);
        getRouterService().routeData();

        OutgoingBatches batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_3);
        filterForChannels(batches, testChannel);
        Assert.assertEquals(count / MAX_BATCH_SIZE + (count % MAX_BATCH_SIZE > 0 ? 1 : 0), batches
                .getBatches().size());

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_2);
        // Node 2 has sync disabled
        Assert.assertEquals(0, batches.getBatches().size());

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1);
        filterForChannels(batches, testChannel);
        // Batch was targeted only at node 3
        Assert.assertEquals(0, batches.getBatches().size());

        resetBatches();
    }

    @Test
    public void testColumnMatchSubtableRoutingToNode1() {
        NodeChannel testChannel = getConfigurationService().getNodeChannel(
                TestConstants.TEST_CHANNEL_ID, false);
        testChannel.setMaxBatchToSend(100);
        testChannel.setBatchAlgorithm("transactional");
        getConfigurationService().saveChannel(testChannel, true);

        TriggerRouter trigger2 = getTestRoutingTableTrigger(TEST_SUBTABLE);
        trigger2.getRouter().setRouterType("column");
        trigger2.getRouter().setRouterExpression("EXTERNAL_DATA=:NODE_ID");
        if (getDbDialect() instanceof DerbyDbDialect || getDbDialect() instanceof MsSqlDbDialect) {
            // TODO could not get subselect to work in trigger text for derby or
            // mssql. probably need to work on derby's support of
            // external_select a bit more
            trigger2.getTrigger().setExternalSelect("'" + NODE_GROUP_NODE_1.getNodeId() + "'");
        } else {
            trigger2.getTrigger().setExternalSelect(
                    "select ROUTING_VARCHAR from " + TEST_TABLE_1
                            + " where PK=$(curTriggerValue).$(curColumnPrefix)FK");
        }
        getTriggerRouterService().saveTriggerRouter(trigger2);

        getTriggerRouterService().syncTriggers();

        insert(TEST_TABLE_1, 1, true);
        getRouterService().routeData();
        resetBatches();

        int pk = ((Number) getJdbcTemplate()
                .queryForList(
                        "select * from " + TEST_TABLE_1 + " where ROUTING_VARCHAR='"
                                + NODE_GROUP_NODE_1.getNodeId() + "'").get(0).get("PK")).intValue();
        getJdbcTemplate().update("insert into " + TEST_SUBTABLE + " (FK) values(?)",
                new Object[] { pk });

        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));

        getRouterService().routeData();

        Assert.assertEquals(
                1,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));
        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_2),
                        testChannel));
        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_3),
                        testChannel));

        resetBatches();

        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));

        execute("delete from " + TEST_SUBTABLE, true, null);

        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));
        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_2),
                        testChannel));
        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_3),
                        testChannel));

        getRouterService().routeData();
        Assert.assertEquals(
                1,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));
        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_2),
                        testChannel));
        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_3),
                        testChannel));

        resetBatches();

    }

    @Test
    public void testColumnMatchOnNull() {
        NodeChannel testChannel = getConfigurationService().getNodeChannel(
                TestConstants.TEST_CHANNEL_ID, false);

        TriggerRouter trigger = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger.getRouter().setRouterType("column");
        trigger.getRouter().setRouterExpression("ROUTING_VARCHAR=NULL");
        getTriggerRouterService().saveTriggerRouter(trigger);

        getTriggerRouterService().syncTriggers();

        resetBatches();

        update(TEST_TABLE_1, "Not Routed");

        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));

        getRouterService().routeData();

        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));

        resetBatches();

        update(TEST_TABLE_1, null);

        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));

        getRouterService().routeData();

        Assert.assertEquals(
                1,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));

    }

    @Test
    public void testColumnMatchOnNotNull() {
        NodeChannel testChannel = getConfigurationService().getNodeChannel(
                TestConstants.TEST_CHANNEL_ID, false);

        TriggerRouter trigger = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger.getRouter().setRouterType("column");
        trigger.getRouter().setRouterExpression("ROUTING_VARCHAR!=NULL");
        getTriggerRouterService().saveTriggerRouter(trigger);

        getTriggerRouterService().syncTriggers();

        resetBatches();

        update(TEST_TABLE_1, "Not Routed");

        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));

        getRouterService().routeData();

        Assert.assertEquals(
                1,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));

        resetBatches();

        update(TEST_TABLE_1, null);

        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));

        getRouterService().routeData();

        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));

    }

    @Test
    public void testSyncOnColumnChange() {
        NodeChannel testChannel = getConfigurationService().getNodeChannel(
                TestConstants.TEST_CHANNEL_ID, false);
        testChannel.setMaxBatchToSend(100);
        testChannel.setBatchAlgorithm("transactional");
        getConfigurationService().saveChannel(testChannel, true);

        TriggerRouter trigger1 = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger1.getRouter().setRouterType("bsh");
        trigger1.getRouter().setRouterExpression(
                "ROUTING_INT != null && !ROUTING_INT.equals(OLD_ROUTING_INT)");
        getTriggerRouterService().saveTriggerRouter(trigger1);

        getTriggerRouterService().syncTriggers();

        resetBatches();

        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));

        insert(TEST_TABLE_1, 1, true);
        getRouterService().routeData();

        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));

        resetBatches();

        int pk = ((Number) getJdbcTemplate()
                .queryForList(
                        "select * from " + TEST_TABLE_1 + " where ROUTING_VARCHAR='"
                                + NODE_GROUP_NODE_1.getNodeId() + "'").get(0).get("PK")).intValue();
        getJdbcTemplate().update("update " + TEST_TABLE_1 + " set ROUTING_INT=1 where PK=?",
                new Object[] { pk });

        getRouterService().routeData();

        Assert.assertEquals(
                1,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));

        resetBatches();
        getJdbcTemplate().update("update " + TEST_TABLE_1 + " set ROUTING_INT=1 where PK=?",
                new Object[] { pk });

        getRouterService().routeData();

        Assert.assertEquals(
                0,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));

        resetBatches();
        getJdbcTemplate().update("update " + TEST_TABLE_1 + " set ROUTING_INT=10 where PK=?",
                new Object[] { pk });

        getRouterService().routeData();

        Assert.assertEquals(
                1,
                countBatchesForChannel(
                        getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1),
                        testChannel));

    }

    @Test
    public void testSyncIncomingBatchWhenUnrouted() throws Exception {
        resetBatches();

        TriggerRouter triggerRouter = getTestRoutingTableTrigger(TEST_TABLE_1);
        triggerRouter.getTrigger().setSyncOnIncomingBatch(true);
        triggerRouter.getRouter().setRouterType("bsh");
        triggerRouter.getRouter().setRouterExpression("return " + NODE_GROUP_NODE_1.getNodeId());
        getTriggerRouterService().saveTriggerRouter(triggerRouter);

        NodeChannel testChannel = getConfigurationService().getNodeChannel(
                TestConstants.TEST_CHANNEL_ID, false);
        testChannel.setMaxBatchToSend(1000);
        testChannel.setMaxBatchSize(50);
        testChannel.setBatchAlgorithm("default");
        getConfigurationService().saveChannel(testChannel, true);

        getTriggerRouterService().syncTriggers();

        insert(TEST_TABLE_1, 10, true, NODE_GROUP_NODE_1.getNodeId());

        int unroutedCount = countUnroutedBatches();

        getRouterService().routeData();

        OutgoingBatches batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1);
        filterForChannels(batches, testChannel);
        Assert.assertEquals(
                "Should have been 0.  We did the insert as if the data had come from node 1.", 0,
                batches.getBatches().size());

        Assert.assertTrue(countUnroutedBatches() > unroutedCount);

        resetBatches();

    }

    @Test
    public void testDefaultRouteToTargetNodeGroupOnly() throws Exception {

        setUpDefaultTriggerRouterForTable1();

        resetBatches();

        insert(TEST_TABLE_1, 1, true);

        getRouterService().routeData();

        Assert.assertEquals(1, getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1)
                .getBatches().size());

        Node node2 = getNodeService().findNode("00030");

        Assert.assertNotNull(node2);

        Assert.assertEquals(0, getOutgoingBatchService().getOutgoingBatches(node2).getBatches()
                .size());

        resetBatches();

    }

    @Test
    public void testUnroutedDataCreatedBatch() {
        // TODO
    }

    @Test
    public void testGapRouting() {
        if (getDbDialect().canGapsOccurInCapturedDataIds()) {
            setUpDefaultTriggerRouterForTable1();

            resetBatches();

            Assert.assertEquals(1, getDataService().findDataGaps().size());

            // route again to make sure we still only have one gap
            getRouterService().routeData();

            Assert.assertEquals(1, getDataService().findDataGaps().size());

            insertGaps(2, 1, 2);

            getRouterService().routeData();

            // route again to calculate gaps
            getRouterService().routeData();

            Assert.assertEquals(1, getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1)
                    .getBatches().size());

            List<DataGap> gaps = getDataService().findDataGaps();

            Assert.assertEquals(2, gaps.size());
            DataGap gap = gaps.get(0);
            Assert.assertEquals(0, gap.getEndId() - gap.getStartId());

            // route again to make sure the gaps don't disappear
            getRouterService().routeData();
            getRouterService().routeData();

            gaps = getDataService().findDataGaps();
            Assert.assertEquals(2, gaps.size());
            gap = gaps.get(0);
            Assert.assertEquals(0, gap.getEndId() - gap.getStartId());
        }
    }

    @Test
    public void testGapWithGapAtBegining() {
        if (getDbDialect().canGapsOccurInCapturedDataIds()) {

            setUpDefaultTriggerRouterForTable1();

            resetBatches();

            List<DataGap> gaps = getDataService().findDataGaps();
            Assert.assertEquals(1, gaps.size());

            // evidently, derby only leaves a gap of one, no matter how many
            // rows you insert
            int gapsize = getDbDialect() instanceof DerbyDbDialect ? 1 : 10;

            insert(TEST_TABLE_1, gapsize, true, null, NODE_GROUP_NODE_1.getNodeId(), true);
            insert(TEST_TABLE_1, 10, true, null, NODE_GROUP_NODE_1.getNodeId(), false);

            routeAndCreateGaps();

            Assert.assertEquals(1, getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1)
                    .getBatches().size());

            gaps = getDataService().findDataGaps();

            Assert.assertEquals(2, gaps.size());
            DataGap gap = gaps.get(0);
            Assert.assertEquals(
                    "The gap's start id was " + gap.getStartId() + " end id was " + gap.getEndId(),
                    gapsize - 1, gap.getEndId() - gap.getStartId());

            routeAndCreateGaps();

            gaps = getDataService().findDataGaps();
            Assert.assertEquals(2, gaps.size());
            gap = gaps.get(0);
            Assert.assertEquals(gapsize - 1, gap.getEndId() - gap.getStartId());
        }
    }

    @Test
    public void testGapWithGapAtEnd() {
        if (getDbDialect().canGapsOccurInCapturedDataIds()) {

            setUpDefaultTriggerRouterForTable1();

            resetBatches();

            Assert.assertEquals(1, getDataService().findDataGaps().size());

            long startId = getJdbcTemplate().queryForLong("select max(start_id) from sym_data_gap");

            getJdbcTemplate().update("update sym_data_gap set status='OK'");
            getDataService().insertDataGap(new DataGap(startId, startId + 10));
            getDataService().insertDataGap(new DataGap(startId + 11, startId + 11 + getParameterService().getLong(ParameterConstants.ROUTING_LARGEST_GAP_SIZE)));

            insertGaps(8, 0, 1);

            routeAndCreateGaps();

            Assert.assertEquals(1, getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1)
                    .getBatches().size());

            List<DataGap> gaps = getDataService().findDataGaps();

            Assert.assertEquals(2, gaps.size());
            DataGap gap = gaps.get(0);
            Assert.assertEquals(startId + 8, gap.getStartId());

            routeAndCreateGaps();
        }
    }

    private void routeAndCreateGaps() {
        // one to route unrouted data
        getRouterService().routeData();
        // one to create gaps
        getRouterService().routeData();
    }

    @Test
    public void testDataGapExpired() {
        if (getDbDialect().canGapsOccurInCapturedDataIds()) {
            resetGaps();

            testGapRouting();

            List<DataGap> gaps = getDataService().findDataGaps();

            Assert.assertEquals(2, gaps.size());
            DataGap gap = gaps.get(0);
            Assert.assertEquals(0, gap.getEndId() - gap.getStartId());

            Calendar time = Calendar.getInstance();
            time.add(Calendar.DATE, -10);
            getJdbcTemplate().update("update sym_data set create_time=?", time.getTime());

            routeAndCreateGaps();
            ;

            gaps = getDataService().findDataGaps();
            Assert.assertEquals("Gap should have expired", 1, gaps.size());
        }
    }

    @Test
    public void testLotsOfGaps() {
        if (getDbDialect().canGapsOccurInCapturedDataIds()) {
            setUpDefaultTriggerRouterForTable1();

            resetGaps();

            resetBatches();

            insertGaps(5, 3, 100);

            routeAndCreateGaps();

            Assert.assertEquals(10, getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1)
                    .getBatches().size());

            List<DataGap> gaps = getDataService().findDataGaps();

            Assert.assertEquals(100, gaps.size());

            insertGaps(5, 3, 100);

            routeAndCreateGaps();

            gaps = getDataService().findDataGaps();

            Assert.assertEquals(200, gaps.size());

            resetGaps();
        }
    }

    @Test
    public void testNoResend() {
        resetBatches();

        Assert.assertEquals(0, getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1)
                .getBatches().size());

        getJdbcTemplate().update("delete from sym_data_gap");

        routeAndCreateGaps();

        Assert.assertEquals(0, getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1)
                .getBatches().size());

    }

    @Test
    public void testDontSelectOldDataDuringRouting() throws Exception {
        NodeChannel testChannel = getConfigurationService().getNodeChannel(
                TestConstants.TEST_CHANNEL_ID, false);
        testChannel.setUseOldDataToRoute(false);
        testChannel.setMaxBatchSize(50);
        testChannel.setBatchAlgorithm("nontransactional");
        getConfigurationService().saveChannel(testChannel, true);

        TriggerRouter trigger = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger.getRouter().setRouterType("column");
        trigger.getRouter().setRouterExpression("ROUTING_VARCHAR=:NODE_ID");
        getTriggerRouterService().saveTriggerRouter(trigger);

        // clean setup
        deleteAll(TEST_TABLE_1);
        insert(TEST_TABLE_1, 100, true);
        getRouterService().routeData();
        resetBatches();

        // delete
        deleteAll(TEST_TABLE_1);

        ChannelRouterContext context = new ChannelRouterContext(
                TestConstants.TEST_ROOT_EXTERNAL_ID, testChannel, getDataSource());
        DataRefRouteReader reader = new DataRefRouteReader(logger, getRouterService(), context, getDataService());

        reader.run();

        List<Data> list = new ArrayList<Data>();
        do {
            Data data = reader.take();
            if (data != null) {
                list.add(data);
            } else {
                break;
            }
        } while (true);

        Assert.assertEquals(100, list.size());
        for (Data data : list) {
            Assert.assertNull(data.toParsedOldData());
            Assert.assertNotNull(data.toParsedPkData());
        }

    }

    @Test
    public void testMaxNumberOfDataToRoute() {
        // TODO
    }

    protected void setUpDefaultTriggerRouterForTable1() {
        TriggerRouter triggerRouter = getTestRoutingTableTrigger(TEST_TABLE_1);
        triggerRouter.getRouter().setRouterType("default");
        triggerRouter.getRouter().setRouterExpression(null);
        getTriggerRouterService().saveTriggerRouter(triggerRouter);

        NodeChannel testChannel = getConfigurationService().getNodeChannel(
                TestConstants.TEST_CHANNEL_ID, false);
        testChannel.setMaxBatchToSend(1000);
        testChannel.setMaxBatchSize(50);
        testChannel.setBatchAlgorithm("default");
        getConfigurationService().saveChannel(testChannel, true);

        getTriggerRouterService().syncTriggers();
    }

    protected int countUnroutedBatches() {
        return getJdbcTemplate().queryForInt(
                "select count(*) from sym_outgoing_batch where node_id=?",
                Constants.UNROUTED_NODE_ID);
    }

    protected TriggerRouter getTestRoutingTableTrigger(String tableName) {
        TriggerRouter trigger = null;
        Set<TriggerRouter> triggerRouters = getTriggerRouterService()
                .getTriggerRouterForTableForCurrentNode(null, null, tableName, true);
        if (triggerRouters == null || triggerRouters.size() == 0) {
            trigger = new TriggerRouter();
            trigger.getTrigger().setSourceTableName(tableName);
            trigger.getRouter().getNodeGroupLink()
                    .setSourceNodeGroupId(TestConstants.TEST_ROOT_NODE_GROUP);
            trigger.getRouter().getNodeGroupLink()
                    .setTargetNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
            if (tableName.equals(TEST_TABLE_2)) {
                trigger.getTrigger().setChannelId(TestConstants.TEST_CHANNEL_ID_OTHER);
            } else {
                trigger.getTrigger().setChannelId(TestConstants.TEST_CHANNEL_ID);
            }
        } else {
            trigger = triggerRouters.iterator().next();
        }
        return trigger;
    }

    protected void filterForChannels(OutgoingBatches batches, NodeChannel... channels) {
        for (Iterator<OutgoingBatch> iterator = batches.getBatches().iterator(); iterator.hasNext();) {
            OutgoingBatch outgoingBatch = iterator.next();
            boolean foundChannel = false;
            for (NodeChannel nodeChannel : channels) {
                if (outgoingBatch.getChannelId().equals(nodeChannel.getChannelId())) {
                    foundChannel = true;
                }
            }

            if (!foundChannel) {
                iterator.remove();
            }
        }
    }

    protected void resetGaps() {
        getJdbcTemplate().update("delete from sym_data_gap");
    }

    protected void resetBatches() {
        routeAndCreateGaps();
        getJdbcTemplate().update("update sym_outgoing_batch set status='OK' where status != 'OK'");
        long startId = getJdbcTemplate().queryForLong("select max(start_id) from sym_data_gap");
        getJdbcTemplate()
                .update("update sym_data_gap set status='OK' where start_id != ?", startId);
    }

    protected int countBatchesForChannel(OutgoingBatches batches, NodeChannel channel) {
        int count = 0;
        for (Iterator<OutgoingBatch> iterator = batches.getBatches().iterator(); iterator.hasNext();) {
            OutgoingBatch outgoingBatch = iterator.next();
            count += outgoingBatch.getChannelId().equals(channel.getChannelId()) ? 1 : 0;
        }
        return count;
    }

    protected void insertGaps(int insertedCount, int rollbackCount, int repeatCount) {
        for (int i = 0; i < repeatCount; i++) {
            insert(TEST_TABLE_1, insertedCount, true, null, NODE_GROUP_NODE_1.getNodeId(), false);
            insert(TEST_TABLE_1, rollbackCount, true, null, NODE_GROUP_NODE_1.getNodeId(), true);
        }
    }

    protected void deleteAll(final String tableName) {
        getJdbcTemplate().update("delete from " + tableName);
    }

    protected void insert(final String tableName, final int count, boolean transactional) {
        insert(tableName, count, transactional, null);
    }

    protected void insert(final String tableName, final int count, boolean transactional,
            final String node2disable) {
        insert(tableName, count, transactional, node2disable, NODE_GROUP_NODE_1.getNodeId());
    }

    protected void insert(final String tableName, final int count, boolean transactional,
            final String node2disable, final String routingVarcharFieldValue) {
        insert(tableName, count, transactional, node2disable, routingVarcharFieldValue, false);
    }

    protected void insert(final String tableName, final int count, boolean transactional,
            final String node2disable, final String routingVarcharFieldValue, final boolean rollback) {
        TransactionCallbackWithoutResult callback = new TransactionCallbackWithoutResult() {
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                try {
                    if (node2disable != null) {
                        getDbDialect().disableSyncTriggers(getJdbcTemplate(), node2disable);
                    }
                    for (int i = 0; i < count; i++) {
                        update(tableName, routingVarcharFieldValue);
                    }

                    if (status != null && rollback) {
                        status.setRollbackOnly();
                    }
                } finally {
                    if (node2disable != null) {
                        getDbDialect().enableSyncTriggers(getJdbcTemplate());
                    }
                }

            }
        };

        if (transactional) {
            getTransactionTemplate().execute(callback);
        } else {
            callback.doInTransaction(null);
        }
    }

    protected void update(String tableName, String value) {
        getJdbcTemplate().update(
                String.format("insert into %s (ROUTING_VARCHAR) values(?)", tableName), value);
    }

    protected void execute(final String sql, boolean transactional, final String node2disable) {
        TransactionCallbackWithoutResult callback = new TransactionCallbackWithoutResult() {
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                SimpleJdbcTemplate t = new SimpleJdbcTemplate(getJdbcTemplate());
                try {
                    if (node2disable != null) {
                        getDbDialect().disableSyncTriggers(getJdbcTemplate(), node2disable);
                    }
                    t.update(sql);
                } finally {
                    if (node2disable != null) {
                        getDbDialect().enableSyncTriggers(getJdbcTemplate());
                    }
                }

            }
        };

        if (transactional) {
            getTransactionTemplate().execute(callback);
        } else {
            callback.doInTransaction(null);
        }
    }

}