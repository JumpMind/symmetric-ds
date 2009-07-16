package org.jumpmind.symmetric.service.impl;

import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.jumpmind.symmetric.test.ParameterizedSuite.ParameterExcluder;
import org.junit.Test;

public class RoutingServiceTest extends AbstractDatabaseTest {

    final static String TEST_TABLE_1 = "TEST_ROUTING_DATA_1";
    final static String TEST_TABLE_2 = "TEST_ROUTING_DATA_2";
    
    public RoutingServiceTest(String dbName) {
        super(dbName);
    }

    public RoutingServiceTest() throws Exception {
    }
    
    protected Trigger getTestRoutingTableTrigger(String tableName) {
        Trigger trigger = getConfigurationService().getTriggerFor(tableName, TestConstants.TEST_ROOT_NODE_GROUP);
        if (trigger == null) {
            trigger = new Trigger(tableName);
            trigger.setSourceGroupId(TestConstants.TEST_ROOT_NODE_GROUP);
            trigger.setTargetGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        }
        return trigger;
    }
    
    @Test
    public void testMultiChannelRoutingToEveryone() {
        Trigger trigger = getTestRoutingTableTrigger(TEST_TABLE_1);
        trigger.setChannelId(TestConstants.TEST_CHANNEL_ID);
        getConfigurationService().insert(trigger);
    }
    
    @Test
    public void syncIncomingBatchTest() throws Exception {
        
    }
    
    @Test
    public void testSyncBackToNode() {
        
    }
    
    @Test
    @ParameterExcluder("postgres")
    public void validateTransactionFunctionailty() throws Exception {
    }

}
