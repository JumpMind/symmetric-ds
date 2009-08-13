package org.jumpmind.symmetric.service.impl;

import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;

public class RouterServiceTest extends AbstractDatabaseTest {

    final static String TEST_TABLE_1 = "TEST_ROUTING_DATA_1";
    final static String TEST_TABLE_2 = "TEST_ROUTING_DATA_2";

    final static String NODE_GROUP_NODE_1 = "00001";
    final static String NODE_GROUP_NODE_2 = "00002";
    final static String NODE_GROUP_NODE_3 = "00003";

    public RouterServiceTest(String dbName) {
        super(dbName);
    }

    public RouterServiceTest() throws Exception {
    }

    @Test
    public void testMultiChannelRoutingToEveryone() {
        resetBatches();

        Trigger trigger1 = getTestRoutingTableTrigger(TEST_TABLE_1);
        getTriggerService().saveTrigger(trigger1);
        Trigger trigger2 = getTestRoutingTableTrigger(TEST_TABLE_2);
        getTriggerService().saveTrigger(trigger2);
        getTriggerService().syncTriggers();
        NodeChannel testChannel = getConfigurationService().getChannel(TestConstants.TEST_CHANNEL_ID);
        NodeChannel otherChannel = getConfigurationService().getChannel(TestConstants.TEST_CHANNEL_ID_OTHER);
        Assert.assertEquals(50, testChannel.getMaxBatchSize());
        Assert.assertEquals(1, otherChannel.getMaxBatchSize());
        // should be 1 batch for table 1 on the testchannel w/ max batch size of 50
        insert(TEST_TABLE_1, 5, false);
        // this should generate 15 batches because the max batch size is 1
        insert(TEST_TABLE_2, 15, false);
        insert(TEST_TABLE_1, 50, true);
        getRoutingService().routeData();

        final int EXPECTED_BATCHES = 16;

        List<OutgoingBatch> batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1);
        filterForChannels(batches, testChannel, otherChannel);
        Assert.assertEquals(EXPECTED_BATCHES, batches.size());
        Assert.assertEquals(1, countBatchesForChannel(batches, testChannel));
        Assert.assertEquals(15, countBatchesForChannel(batches, otherChannel));

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_2);
        filterForChannels(batches, testChannel, otherChannel);
        // Node 2 has sync disabled
        Assert.assertEquals(0, batches.size());

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_3);
        filterForChannels(batches, testChannel, otherChannel);
        Assert.assertEquals(EXPECTED_BATCHES, batches.size());

        resetBatches();

        // should be 2 batches for table 1 on the testchannel w/ max batch size of 50
        insert(TEST_TABLE_1, 50, false);
        // this should generate 1 batches because the max batch size is 1, but the batch is transactional
        insert(TEST_TABLE_2, 15, true);
        insert(TEST_TABLE_1, 50, false);
        getRoutingService().routeData();

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1);
        filterForChannels(batches, testChannel, otherChannel);
        Assert.assertEquals(3, batches.size());
        Assert.assertEquals(2, countBatchesForChannel(batches, testChannel));
        Assert.assertEquals(1, countBatchesForChannel(batches, otherChannel));
    }

    @Test
    public void testColumnMatchTransactionalOnlyRoutingToNode1() {
        resetBatches();

        Trigger trigger1 = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger1.setRouterName("column");
        trigger1.setRouterExpression("ROUTING_VARCHAR=:NODE_ID");
        getTriggerService().saveTrigger(trigger1);
        getTriggerService().syncTriggers();
        NodeChannel testChannel = getConfigurationService().getChannel(TestConstants.TEST_CHANNEL_ID);
        testChannel.setMaxBatchToSend(100);
        testChannel.setBatchAlgorithm("transactional");
        getConfigurationService().saveChannel(testChannel);

        // should be 51 batches for table 1
        insert(TEST_TABLE_1, 500, true);
        insert(TEST_TABLE_1, 50, false);
        getRoutingService().routeData();

        final int EXPECTED_BATCHES = 51;

        List<OutgoingBatch> batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1);
        filterForChannels(batches, testChannel);
        Assert.assertEquals(EXPECTED_BATCHES, batches.size());
        Assert.assertEquals(EXPECTED_BATCHES, countBatchesForChannel(batches, testChannel));

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_2);
        filterForChannels(batches, testChannel);
        // Node 2 has sync disabled
        Assert.assertEquals(0, batches.size());

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_3);
        filterForChannels(batches, testChannel);
        // Batch was targeted only at node 1
        Assert.assertEquals(0, batches.size());

        resetBatches();
    }

    @Test
    public void testSubSelectNonTransactionalRoutingToNode1() {
        resetBatches();

        Trigger trigger1 = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger1.setRouterName("subselect");
        trigger1.setRouterExpression("c.node_id=:ROUTING_VARCHAR");
        getTriggerService().saveTrigger(trigger1);
        getTriggerService().syncTriggers();
        NodeChannel testChannel = getConfigurationService().getChannel(TestConstants.TEST_CHANNEL_ID);
        testChannel.setMaxBatchToSend(1000);
        testChannel.setMaxBatchSize(5);
        testChannel.setBatchAlgorithm("nontransactional");
        getConfigurationService().saveChannel(testChannel);

        // should be 100 batches for table 1, even though we committed the changes as part of a transaction
        insert(TEST_TABLE_1, 500, true);
        getRoutingService().routeData();

        final int EXPECTED_BATCHES = 100;

        List<OutgoingBatch> batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1);
        filterForChannels(batches, testChannel);
        Assert.assertEquals(EXPECTED_BATCHES, batches.size());
        Assert.assertEquals(EXPECTED_BATCHES, countBatchesForChannel(batches, testChannel));

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_2);
        filterForChannels(batches, testChannel);
        // Node 2 has sync disabled
        Assert.assertEquals(0, batches.size());

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_3);
        filterForChannels(batches, testChannel);
        // Batch was targeted only at node 1
        Assert.assertEquals(0, batches.size());

        resetBatches();
    }

    @Test
    public void testSyncIncomingBatch() throws Exception {
        resetBatches();

        Trigger trigger1 = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger1.setSyncOnIncomingBatch(true);
        trigger1.setRouterExpression(null);
        trigger1.setRouterName(null);
        getTriggerService().saveTrigger(trigger1);

        NodeChannel testChannel = getConfigurationService().getChannel(TestConstants.TEST_CHANNEL_ID);
        testChannel.setMaxBatchToSend(1000);
        testChannel.setMaxBatchSize(50);
        testChannel.setBatchAlgorithm("default");
        getConfigurationService().saveChannel(testChannel);

        getTriggerService().syncTriggers();

        insert(TEST_TABLE_1, 10, true, NODE_GROUP_NODE_1);

        getRoutingService().routeData();

        List<OutgoingBatch> batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1);
        filterForChannels(batches, testChannel);
        Assert.assertEquals("Should have been 0.  We did the insert as if the data had come from node 1.", 0, batches
                .size());

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_3);
        filterForChannels(batches, testChannel);
        Assert.assertEquals(1, batches.size());

        resetBatches();

    }

    @Ignore
    @Test
    public void testLargeNumberOfEventsToManyNodes() {
        resetBatches();

        Trigger trigger1 = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger1.setRouterName("column");
        // set up a constant to force the data to be routed through the column data matcher, but to everyone
        trigger1.setRouterExpression("ROUTING_VARCHAR=00001");
        getTriggerService().saveTrigger(trigger1);
        getTriggerService().syncTriggers();

        NodeChannel testChannel = getConfigurationService().getChannel(TestConstants.TEST_CHANNEL_ID);
        testChannel.setMaxBatchToSend(100);
        testChannel.setMaxBatchSize(10000);
        testChannel.setBatchAlgorithm("default");
        getConfigurationService().saveChannel(testChannel);
        final int ROWS_TO_INSERT = 100000;
        final int NODES_TO_INSERT = 1000;
        logger.info(String.format("About to insert %s nodes",NODES_TO_INSERT));
        for (int i = 0; i < 1000; i++) {
            String nodeId = String.format("100%s", i);
            getRegistrationService().openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, nodeId);
            Node node = getNodeService().findNode(nodeId);
            node.setSyncEnabled(true);
            getNodeService().updateNode(node);
        }
        logger.info(String.format("Done inserting %s nodes",NODES_TO_INSERT));

        logger.info(String.format("About to insert %s rows", ROWS_TO_INSERT));
        insert(TEST_TABLE_1, ROWS_TO_INSERT, false);
        logger.info(String.format("Done inserting %s rows", ROWS_TO_INSERT));
        
        logger.info("About to route data");
        getRoutingService().routeData();
        logger.info("Done routing data");
    }
    
    @Test
    public void testBshTransactionalRoutingOnUpdate() {
        resetBatches();

        Trigger trigger1 = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger1.setRouterName("bsh");
        trigger1.setRouterExpression("targetNodes.add(ROUTING_VARCHAR); targetNodes.add(OLD_ROUTING_VARCHAR);");
        
        getTriggerService().saveTrigger(trigger1);
        getTriggerService().syncTriggers();
        
        NodeChannel testChannel = getConfigurationService().getChannel(TestConstants.TEST_CHANNEL_ID);
        testChannel.setMaxBatchToSend(1000);
        testChannel.setMaxBatchSize(5);
        testChannel.setBatchAlgorithm("transactional");
        getConfigurationService().saveChannel(testChannel);

        long ts = System.currentTimeMillis();
        SimpleJdbcTemplate t = new SimpleJdbcTemplate(getJdbcTemplate());
        int count = t.update(String.format("update %s set ROUTING_VARCHAR=?", TEST_TABLE_1),
                        NODE_GROUP_NODE_3);
        logger.info("Just recorded a change to " + count + " rows in " + TEST_TABLE_1 + " in " + (System.currentTimeMillis()-ts) + "ms");
        ts = System.currentTimeMillis();
        getRoutingService().routeData();
        logger.info("Just routed " + count + " rows in " + TEST_TABLE_1 + " in " + (System.currentTimeMillis()-ts) + "ms");

        List<OutgoingBatch> batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1);
        filterForChannels(batches, testChannel);        
        Assert.assertEquals(1, batches.size());
        Assert.assertEquals(count, (int)batches.get(0).getDataEventCount());

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_2);
        filterForChannels(batches, testChannel);
        // Node 2 has sync disabled
        Assert.assertEquals(0, batches.size());

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_3);
        filterForChannels(batches, testChannel);
        Assert.assertEquals(1, batches.size());
        Assert.assertEquals(count, (int)batches.get(0).getDataEventCount());
        
        resetBatches();
    }    
    
    @Test
    public void testBshTransactionalRoutingInsert() {
        resetBatches();

        Trigger trigger1 = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger1.setRouterName("bsh");
        trigger1.setRouterExpression("targetNodes.add(ROUTING_VARCHAR); targetNodes.add(OLD_ROUTING_VARCHAR);");
        
        getTriggerService().saveTrigger(trigger1);
        getTriggerService().syncTriggers();
        
        NodeChannel testChannel = getConfigurationService().getChannel(TestConstants.TEST_CHANNEL_ID);
        testChannel.setMaxBatchToSend(1000);
        testChannel.setMaxBatchSize(5);
        testChannel.setBatchAlgorithm("transactional");
        getConfigurationService().saveChannel(testChannel);

        insert(TEST_TABLE_1, 5, true);
        getRoutingService().routeData();

        List<OutgoingBatch> batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_1);
        filterForChannels(batches, testChannel);        
        Assert.assertEquals(1, batches.size());
        Assert.assertEquals(5, (int)batches.get(0).getDataEventCount());

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_2);
        filterForChannels(batches, testChannel);
        // Node 2 has sync disabled
        Assert.assertEquals(0, batches.size());

        batches = getOutgoingBatchService().getOutgoingBatches(NODE_GROUP_NODE_3);
        filterForChannels(batches, testChannel);
        // Batch was targeted only at node 1
        Assert.assertEquals(0, batches.size());
        
        resetBatches();
    }        

    protected Trigger getTestRoutingTableTrigger(String tableName) {
        Trigger trigger = getTriggerService().getTriggerFor(tableName, TestConstants.TEST_ROOT_NODE_GROUP);
        if (trigger == null) {
            trigger = new Trigger(tableName);
            trigger.setSourceGroupId(TestConstants.TEST_ROOT_NODE_GROUP);
            trigger.setTargetGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
            if (tableName.equals(TEST_TABLE_2)) {
                trigger.setChannelId(TestConstants.TEST_CHANNEL_ID_OTHER);
            } else {
                trigger.setChannelId(TestConstants.TEST_CHANNEL_ID);
            }
        }
        return trigger;
    }

    protected void filterForChannels(List<OutgoingBatch> batches, NodeChannel... channels) {
        for (Iterator<OutgoingBatch> iterator = batches.iterator(); iterator.hasNext();) {
            OutgoingBatch outgoingBatch = iterator.next();
            boolean foundChannel = false;
            for (NodeChannel nodeChannel : channels) {
                if (outgoingBatch.getChannelId().equals(nodeChannel.getId())) {
                    foundChannel = true;
                }
            }

            if (!foundChannel) {
                iterator.remove();
            }
        }
    }

    protected void resetBatches() {
        getOutgoingBatchService().markAllAsSentForNode(NODE_GROUP_NODE_1);
        getOutgoingBatchService().markAllAsSentForNode(NODE_GROUP_NODE_2);
        getOutgoingBatchService().markAllAsSentForNode(NODE_GROUP_NODE_3);
    }

    protected int countBatchesForChannel(List<OutgoingBatch> batches, NodeChannel channel) {
        int count = 0;
        for (Iterator<OutgoingBatch> iterator = batches.iterator(); iterator.hasNext();) {
            OutgoingBatch outgoingBatch = iterator.next();
            count += outgoingBatch.getChannelId().equals(channel.getId()) ? 1 : 0;
        }
        return count;
    }

    protected void insert(final String tableName, final int count, boolean transactional) {
        insert(tableName, count, transactional, null);
    }

    protected void insert(final String tableName, final int count, boolean transactional, final String node2disable) {
        TransactionCallbackWithoutResult callback = new TransactionCallbackWithoutResult() {
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                SimpleJdbcTemplate t = new SimpleJdbcTemplate(getJdbcTemplate());
                try {
                    if (node2disable != null) {
                        getDbDialect().disableSyncTriggers(node2disable);
                    }
                    for (int i = 0; i < count; i++) {
                        t.update(String.format("insert into %s (ROUTING_VARCHAR) values(?)", tableName),
                                NODE_GROUP_NODE_1);
                    }
                } finally {
                    if (node2disable != null) {
                        getDbDialect().enableSyncTriggers();
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
