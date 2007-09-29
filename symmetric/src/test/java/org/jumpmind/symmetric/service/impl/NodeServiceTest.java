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
    public void testFindClient() throws Exception {
        Node client = nodeService.findNode("00001");
        Assert.assertEquals(client.getNodeId(), "00001", "Wrong clientId");
        Assert.assertEquals(client.getGroupId(), "STORE", "Wrong domainName");
        Assert.assertEquals(client.getExternalId(), "00001", "Wrong domainId");
        Assert.assertEquals(client.getSyncURL().toString(), "http://localhost:8080/sync", "Wrong syncUrl");
        Assert.assertEquals(client.getSchemaVersion(), "1", "Wrong schemaVersion");
        Assert.assertEquals(client.getDatabaseType(), "MySQL", "Wrong databaseType");
        Assert.assertEquals(client.getDatabaseVersion(), "5", "Wrong databaseVersion");
    }

    @Test(groups = "continuous")
    public void testFindClientFail() throws Exception {
        Node client = nodeService.findNode("00004");
        Assert.assertNull(client, "Should not find client");
    }

    @Test(groups = "continuous")
    public void testFindClientSecurity() throws Exception {
        NodeSecurity client = nodeService.findNodeSecurity("00001");
        Assert.assertEquals(client.getNodeId(), "00001", "Wrong clientId");
        Assert.assertEquals(client.getPassword(), "secret", "Wrong password");
        Assert.assertEquals(client.isRegistrationEnabled(), false, "Wrong isRegistrationEnabled");
        Assert.assertEquals(client.getRegistrationTime().toString(), "2007-01-01 01:01:01.0",
                "Wrong registrationTime");
    }

    @Test(groups = "continuous")
    public void testFindClientSecurityFail() throws Exception {
        NodeSecurity client = nodeService.findNodeSecurity("00004");
        Assert.assertNull(client, "Should not find client");
    }

    @Test(groups = "continuous")
    public void testIsClientAuthorized() throws Exception {
        Assert.assertTrue(nodeService.isNodeAuthorized("00001", "secret"),
                "Client should be authorized");

        Assert.assertFalse(nodeService.isNodeAuthorized("00001", "wrongPassword"),
                "Client should NOT be authorized");

        Assert.assertFalse(nodeService.isNodeAuthorized("wrongClientId", "secret"),
                "Client should NOT be authorized");
    }

    @Test(groups = "continuous")
    public void testFindIdentity() throws Exception {
        Node client = nodeService.findIdentity();
        Assert.assertEquals(client.getNodeId(), "00001", "Wrong clientId");
        Assert.assertEquals(client.getGroupId(), "STORE", "Wrong domainName");
        Assert.assertEquals(client.getExternalId(), "00001", "Wrong domainId");
        Assert.assertEquals(client.getSyncURL().toString(), "http://localhost:8080/sync", "Wrong syncUrl");
        Assert.assertEquals(client.getSchemaVersion(), "1", "Wrong schemaVersion");
        Assert.assertEquals(client.getDatabaseType(), "MySQL", "Wrong databaseType");
        Assert.assertEquals(client.getDatabaseVersion(), "5", "Wrong databaseVersion");
    }

    @Test(groups = "continuous")
    public void testFindPullClients() throws Exception {
        List<Node> list = nodeService.findNodesToPull();
        Assert.assertEquals(list.size(), 1, "Wrong number of pull clients");
        Node client = list.get(0);
        Assert.assertEquals(client.getNodeId(), "00000", "Wrong clientId");
        Assert.assertEquals(client.getGroupId(), "CORP", "Wrong domainName");
        Assert.assertEquals(client.getExternalId(), "00000", "Wrong domainId");
        Assert.assertEquals(client.getSyncURL().toString(), "http://centraloffice:8080/sync", "Wrong syncUrl");
        Assert.assertEquals(client.getSchemaVersion(), "1", "Wrong schemaVersion");
        Assert.assertEquals(client.getDatabaseType(), "Oracle", "Wrong databaseType");
        Assert.assertEquals(client.getDatabaseVersion(), "9", "Wrong databaseVersion");
    }

    @Test(groups = "continuous")
    public void testFindPushClients() throws Exception {
        List<Node> list = nodeService.findNodesToPushTo();
        Assert.assertEquals(list.size(), 1, "Wrong number of push clients");
        Node client = list.get(0);
        Assert.assertEquals(client.getNodeId(), "00000", "Wrong clientId");
        Assert.assertEquals(client.getGroupId(), "CORP", "Wrong domainName");
        Assert.assertEquals(client.getExternalId(), "00000", "Wrong domainId");
        Assert.assertEquals(client.getSyncURL().toString(), "http://centraloffice:8080/sync", "Wrong syncUrl");
        Assert.assertEquals(client.getSchemaVersion(), "1", "Wrong schemaVersion");
        Assert.assertEquals(client.getDatabaseType(), "Oracle", "Wrong databaseType");
        Assert.assertEquals(client.getDatabaseVersion(), "9", "Wrong databaseVersion");
    }

}
