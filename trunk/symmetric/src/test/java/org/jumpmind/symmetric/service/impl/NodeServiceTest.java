package org.jumpmind.symmetric.service.impl;

import java.util.List;

import org.jumpmind.symmetric.AbstractTest;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class NodeServiceTest extends AbstractTest {

    protected INodeService nodeService;
    
    @BeforeTest(groups = "continuous")
    protected void setUp() {
        nodeService = (INodeService) getBeanFactory().getBean(Constants.NODE_SERVICE);
    }

    @Test(groups = "continuous")
    public void testFindNode() throws Exception {
        Node node = nodeService.findNode("00001");
        Assert.assertEquals(node.getNodeId(), "00001", "Wrong nodeId");
        Assert.assertEquals(node.getNodeGroupId(), "STORE", "Wrong node group id");
        Assert.assertEquals(node.getExternalId(), "00001", "Wrong external id");
        Assert.assertEquals(node.getSyncURL().toString(), "http://localhost:8080/sync", "Wrong syncUrl");
        Assert.assertEquals(node.getSchemaVersion(), "1", "Wrong schemaVersion");
        Assert.assertEquals(node.getDatabaseType(), "MySQL", "Wrong databaseType");
        Assert.assertEquals(node.getDatabaseVersion(), "5.0", "Wrong databaseVersion");
    }

    @Test(groups = "continuous")
    public void testFindNodeFail() throws Exception {
        Node node = nodeService.findNode("00004");
        Assert.assertNull(node, "Should not find node");
    }

    @Test(groups = "continuous")
    public void testFindNodeSecurity() throws Exception {
        NodeSecurity node = nodeService.findNodeSecurity("00001");
        Assert.assertEquals(node.getNodeId(), "00001", "Wrong nodeId");
        Assert.assertEquals(node.getPassword(), "secret", "Wrong password");
        Assert.assertEquals(node.isRegistrationEnabled(), false, "Wrong isRegistrationEnabled");
        Assert.assertEquals(node.getRegistrationTime().toString(), "2007-01-01 01:01:01.0",
                "Wrong registrationTime");
    }

    @Test(groups = "continuous")
    public void testFindNodeSecurityFail() throws Exception {
        NodeSecurity node = nodeService.findNodeSecurity("00004");
        Assert.assertNull(node, "Should not find node");
    }

    @Test(groups = "continuous")
    public void testIsNodeAuthorized() throws Exception {
        Assert.assertTrue(nodeService.isNodeAuthorized("00001", "secret"),
                "Node should be authorized");

        Assert.assertFalse(nodeService.isNodeAuthorized("00001", "wrongPassword"),
                "Node should NOT be authorized");

        Assert.assertFalse(nodeService.isNodeAuthorized("wrongNodeId", "secret"),
                "Node should NOT be authorized");
    }

    @Test(groups = "continuous")
    public void testFindIdentity() throws Exception {
        Node node = nodeService.findIdentity();
        Assert.assertEquals(node.getNodeId(), "00001", "Wrong nodeId");
        Assert.assertEquals(node.getNodeGroupId(), "STORE", "Wrong node group id");
        Assert.assertEquals(node.getExternalId(), "00001", "Wrong external id");
        Assert.assertEquals(node.getSyncURL().toString(), "http://localhost:8080/sync", "Wrong syncUrl");
        Assert.assertEquals(node.getSchemaVersion(), "1", "Wrong schemaVersion");
        Assert.assertEquals(node.getDatabaseType(), "MySQL", "Wrong databaseType");
        Assert.assertEquals(node.getDatabaseVersion(), "5.0", "Wrong databaseVersion");
    }

    @Test(groups = "continuous")
    public void testFindPullNodes() throws Exception {
        List<Node> list = nodeService.findNodesToPull();
        Assert.assertEquals(list.size(), 1, "Wrong number of pull nodes");
        Node node = list.get(0);
        Assert.assertEquals(node.getNodeId(), "00000", "Wrong nodeId");
        Assert.assertEquals(node.getNodeGroupId(), "CORP", "Wrong node group id");
        Assert.assertEquals(node.getExternalId(), "00000", "Wrong external id");
        Assert.assertEquals(node.getSyncURL().toString(), "http://centraloffice:8080/sync", "Wrong syncUrl");
        Assert.assertEquals(node.getSchemaVersion(), "1", "Wrong schemaVersion");
        Assert.assertEquals(node.getDatabaseType(), "Oracle", "Wrong databaseType");
        Assert.assertEquals(node.getDatabaseVersion(), "9", "Wrong databaseVersion");
    }

    @Test(groups = "continuous")
    public void testFindPushNodes() throws Exception {
        List<Node> list = nodeService.findNodesToPushTo();
        Assert.assertEquals(list.size(), 1, "Wrong number of push nodes");
        Node node = list.get(0);
        Assert.assertEquals(node.getNodeId(), "00000", "Wrong nodeId");
        Assert.assertEquals(node.getNodeGroupId(), "CORP", "Wrong node group id");
        Assert.assertEquals(node.getExternalId(), "00000", "Wrong external id");
        Assert.assertEquals(node.getSyncURL().toString(), "http://centraloffice:8080/sync", "Wrong syncUrl");
        Assert.assertEquals(node.getSchemaVersion(), "1", "Wrong schemaVersion");
        Assert.assertEquals(node.getDatabaseType(), "Oracle", "Wrong databaseType");
        Assert.assertEquals(node.getDatabaseVersion(), "9", "Wrong databaseVersion");
    }

}
