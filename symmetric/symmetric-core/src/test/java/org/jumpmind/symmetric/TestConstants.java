package org.jumpmind.symmetric;

import org.jumpmind.symmetric.model.Node;

abstract public class TestConstants {
    
    public final static String TEST_CLIENT_EXTERNAL_ID = "00001";
    public static final String TEST_CLIENT_NODE_GROUP = "test-node-group";
    public static final String TEST_CLIENT_NODE_GROUP_2 = "test-node-group2";
    public final static Node TEST_CLIENT_NODE = new Node(TestConstants.TEST_CLIENT_EXTERNAL_ID, TestConstants.TEST_CLIENT_NODE_GROUP);
    
    public final static String TEST_ROOT_EXTERNAL_ID = "00000";
    public static final String TEST_ROOT_NODE_GROUP = "test-root-group";
    public final static Node TEST_ROOT_NODE = new Node(TestConstants.TEST_ROOT_EXTERNAL_ID, TestConstants.TEST_ROOT_NODE_GROUP);

    public static final String TEST_DROP_ALL_SCRIPT = "/test-data-drop-all.sql";
    public static final String TEST_DROP_SEQ_SCRIPT = "/test-data-drop-";
    public static final String TEST_ROOT_DOMAIN_SETUP_SCRIPT = "-integration-root-setup.sql";
    public static final String TEST_CONTINUOUS_SETUP_SCRIPT = "-database-setup.sql";
    public static final String TEST_CONTINUOUS_NODE_GROUP = TEST_ROOT_NODE_GROUP;
    public static final String TEST_CHANNEL_ID = "testchannel";
    public static final String TEST_CHANNEL_ID_OTHER = "other";
    public static final int TEST_TRIGGER_HISTORY_ID = 1;

    public static final String MYSQL = "mysql";
}