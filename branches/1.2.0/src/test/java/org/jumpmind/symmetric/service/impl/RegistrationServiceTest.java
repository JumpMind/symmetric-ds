/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
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

package org.jumpmind.symmetric.service.impl;

import java.io.ByteArrayOutputStream;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class RegistrationServiceTest extends AbstractDatabaseTest {

    protected INodeService nodeService;
    
    protected IRegistrationService registrationService;

    @BeforeTest(groups = "continuous")
    protected void setUp() {
        nodeService = (INodeService) getBeanFactory().getBean(Constants.NODE_SERVICE);
        registrationService = (IRegistrationService) getBeanFactory().getBean(Constants.REGISTRATION_SERVICE);
    }

    @Test(groups = "continuous")
    public void testRegisterNodeFail() throws Exception {
        Node node = new Node();
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        // Existing domain name and ID that is not open to register
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00001");
        Assert.assertFalse(registrationService.registerNode(node, out),
                "Node should NOT be allowed to register");

        // Existing domain name but wrong ID
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("wrongId");
        Assert.assertFalse(registrationService.registerNode(node, out),
                "Node should NOT be allowed to register");

        // Wrong domain name and wrong ID
        node.setNodeGroupId("wrongDomain");
        node.setExternalId("wrongId");
        Assert.assertFalse(registrationService.registerNode(node, out),
                "Node should NOT be allowed to register");
    }

    @Test(groups = "continuous")
    public void testRegisterNode() throws Exception {
        Node node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00002");
        node.setSyncURL("http://localhost:8080/sync");
        node.setSchemaVersion("1");
        node.setDatabaseType("MySQL");
        node.setDatabaseVersion("5");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Assert.assertTrue(registrationService.registerNode(node, out),
                "Node should be allowed to register");

        node = nodeService.findNode("00002");
        Assert.assertEquals(node.getNodeId(), "00002", "Wrong nodeId");
        Assert.assertEquals(node.getNodeGroupId(), TestConstants.TEST_CLIENT_NODE_GROUP, "Wrong domainName");
        Assert.assertEquals(node.getExternalId(), "00002", "Wrong domainId");
        Assert.assertEquals(node.getSyncURL().toString(), "http://localhost:8080/sync", "Wrong syncUrl");
        Assert.assertEquals(node.getSchemaVersion(), "1", "Wrong schemaVersion");
        Assert.assertEquals(node.getDatabaseType(), "MySQL", "Wrong databaseType");
        Assert.assertEquals(node.getDatabaseVersion(), "5", "Wrong databaseVersion");

        NodeSecurity security = nodeService.findNodeSecurity("00002");
        Assert.assertEquals(security.getNodeId(), "00002", "Wrong nodeId");
        Assert.assertNotSame(security.getPassword(), null, "Wrong password");
        Assert.assertEquals(security.isRegistrationEnabled(), false, "Wrong isRegistrationEnabled");
        Assert.assertNotSame(security.getRegistrationTime(), null, "Wrong registrationTime");
    }

    @Test(groups = "continuous")
    public void testRegisterNodeAutomatic() throws Exception {
        try {
            ((RegistrationService) registrationService).setAutoRegistration(true);
            doTestRegisterNodeAutomatic();
        }
        finally {
            ((RegistrationService) registrationService).setAutoRegistration(false);            
        }
    }
    
    private void doTestRegisterNodeAutomatic() throws Exception{
        Node node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00008");
        node.setSyncURL("http://localhost:8080/sync");
        node.setSchemaVersion("1");
        node.setDatabaseType("MySQL");
        node.setDatabaseVersion("5");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Assert.assertTrue(registrationService.registerNode(node, out),
                "Node should be allowed to register");

        node = nodeService.findNode("00008");
        Assert.assertEquals(node.getNodeId(), "00008", "Wrong nodeId");
        Assert.assertEquals(node.getNodeGroupId(), TestConstants.TEST_CLIENT_NODE_GROUP, "Wrong domainName");
        Assert.assertEquals(node.getExternalId(), "00008", "Wrong domainId");
        Assert.assertEquals(node.getSyncURL().toString(), "http://localhost:8080/sync", "Wrong syncUrl");
        Assert.assertEquals(node.getSchemaVersion(), "1", "Wrong schemaVersion");
        Assert.assertEquals(node.getDatabaseType(), "MySQL", "Wrong databaseType");
        Assert.assertEquals(node.getDatabaseVersion(), "5", "Wrong databaseVersion");

        NodeSecurity security = nodeService.findNodeSecurity("00008");
        Assert.assertEquals(security.getNodeId(), "00008", "Wrong nodeId");
        Assert.assertNotSame(security.getPassword(), null, "Wrong password");
        Assert.assertEquals(security.isRegistrationEnabled(), false, "Wrong isRegistrationEnabled");
        Assert.assertNotSame(security.getRegistrationTime(), null, "Wrong registrationTime");
        
        // Make sure opening registration still works with auto-registration
        registrationService.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, "00009");
        node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00009");
        node.setSyncURL("http://localhost:8080/sync");
        node.setSchemaVersion("1");
        node.setDatabaseType("MySQL");
        node.setDatabaseVersion("5");
        out = new ByteArrayOutputStream();
        Assert.assertTrue(registrationService.registerNode(node, out), "Node should be allowed to register");        
    }

    @Test(groups = "continuous")
    public void testRegisterNodeWithResponse() throws Exception {
        registrationService.openRegistration("test-root-group", "09999");

        Node node = new Node();
        node.setNodeGroupId("test-root-group");
        node.setExternalId("09999");
        node.setSyncURL("http://localhost:8080/sync");
        node.setSchemaVersion("1");
        node.setDatabaseType("MySQL");
        node.setDatabaseVersion("5");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Assert.assertTrue(registrationService.registerNode(node, out),
                "Node should be allowed to register");
        out.close();
        String response = out.toString();
        Assert.assertTrue(response.indexOf("batch,") != -1, "Expected batch");
        Assert.assertTrue(response.indexOf("table,") != -1, "Expected table");
        Assert.assertTrue(response.indexOf("keys,") != -1, "Expected table");
        Assert.assertTrue(response.indexOf("columns,") != -1, "Expected table");
        Assert.assertTrue(response.indexOf("insert,") != -1, "Expected insert");
        Assert.assertTrue(response.indexOf("commit,") != -1, "Expected commit");
    }

    @Test(groups = "continuous")
    public void testReOpenRegistration() throws Exception {
        registrationService.reOpenRegistration("00003");
        NodeSecurity security = nodeService.findNodeSecurity("00003");
        Assert.assertEquals(security.getNodeId(), "00003", "Wrong nodeId");
        Assert.assertNotSame(security.getPassword(), "notsecret", "Wrong password");
        Assert.assertEquals(security.isRegistrationEnabled(), true, "Wrong isRegistrationEnabled");
        Assert.assertEquals(security.getRegistrationTime(), null, "Wrong registrationTime");

        Node node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00003");
        node.setSyncURL("http://0:8080/sync");
        node.setSchemaVersion("1");
        node.setDatabaseType("hqsql");
        node.setDatabaseVersion("1");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Assert.assertTrue(registrationService.registerNode(node, out),
                "Node should be allowed to register");

        node = nodeService.findNode("00003");
        Assert.assertEquals(node.getNodeId(), "00003", "Wrong nodeId");
        Assert.assertEquals(node.getNodeGroupId(), TestConstants.TEST_CLIENT_NODE_GROUP, "Wrong domainName");
        Assert.assertEquals(node.getExternalId(), "00003", "Wrong domainId");
        Assert.assertEquals(node.getSyncURL().toString(), "http://0:8080/sync", "Wrong syncUrl");
        Assert.assertEquals(node.getSchemaVersion(), "1", "Wrong schemaVersion");
        Assert.assertEquals(node.getDatabaseType(), "hqsql", "Wrong databaseType");
        Assert.assertEquals(node.getDatabaseVersion(), "1", "Wrong databaseVersion");

        security = nodeService.findNodeSecurity("00003");
        Assert.assertEquals(security.getNodeId(), "00003", "Wrong nodeId");
        Assert.assertNotSame(security.getPassword(), "notsecret", "Wrong password");
        Assert.assertEquals(security.isRegistrationEnabled(), false, "Wrong isRegistrationEnabled");
        Assert.assertNotSame(security.getRegistrationTime(), null, "Wrong registrationTime");
    }

    @Test(groups = "continuous")
    public void testReOpenRegistrationFail() throws Exception {
        registrationService.reOpenRegistration("00004");
        NodeSecurity security = nodeService.findNodeSecurity("00004");
        Assert.assertNull(security, "Should not be able to re-open registration");
    }

    @Test(groups = "continuous")
    public void testOpenRegistration() throws Exception {        
        registrationService.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, "00005");

        Node node = nodeService.findNode("00005");
        Assert.assertEquals(node.getNodeId(), "00005", "Wrong nodeId");
        Assert.assertEquals(node.getNodeGroupId(), TestConstants.TEST_CLIENT_NODE_GROUP, "Wrong domainName");
        Assert.assertEquals(node.getExternalId(), "00005", "Wrong domainId");
        Assert.assertEquals(node.getSyncURL(), null, "Wrong syncUrl");
        Assert.assertEquals(node.getSchemaVersion(), null, "Wrong schemaVersion");
        Assert.assertEquals(node.getDatabaseType(), null, "Wrong databaseType");
        Assert.assertEquals(node.getDatabaseVersion(), null, "Wrong databaseVersion");

        NodeSecurity security = nodeService.findNodeSecurity("00005");
        Assert.assertEquals(security.getNodeId(), "00005", "Wrong nodeId");
        Assert.assertNotSame(security.getPassword(), null, "Wrong password");
        Assert.assertEquals(security.isRegistrationEnabled(), true, "Wrong isRegistrationEnabled");
        Assert.assertEquals(security.getRegistrationTime(), null, "Wrong registrationTime");
    }

    @Test(groups = "continuous")
    public void testOpenRegistrationOnceAndRegisterTwice() throws Exception {
        registrationService.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, "00006");
        Node node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00006");
        node.setSyncURL("http://127.0.0.1");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Assert.assertTrue(registrationService.registerNode(node, out),
                "Node should be able to register");
        Assert.assertFalse(registrationService.registerNode(node, out),
                "Node should NOT be able to register");
    }

    @Test(groups = "continuous")
    public void testOpenRegistrationTwiceAndRegisterThrice() throws Exception {
        registrationService.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, "00007");
        registrationService.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, "00007");
        Node node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00007");
        node.setSyncURL("http://0");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Assert.assertTrue(registrationService.registerNode(node, out),
                "Node should be able to register");
        Assert.assertTrue(registrationService.registerNode(node, out),
                "Node should be able to register");
        Assert.assertFalse(registrationService.registerNode(node, out),
                "Node should NOT be able to register");
    }

}
