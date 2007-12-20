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

import org.jumpmind.symmetric.AbstractTest;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class RegistrationServiceTest extends AbstractTest {

    protected INodeService nodeService;
    
    protected IRegistrationService registrationService;

    @BeforeTest(groups = "continuous")
    protected void setUp() {
        nodeService = (INodeService) getBeanFactory().getBean(Constants.NODE_SERVICE);
        registrationService = (IRegistrationService) getBeanFactory().getBean(Constants.REGISTRATION_SERVICE);
    }

    @Test(groups = "continuous")
    public void testRegisterClientFail() throws Exception {
        Node client = new Node();
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        // Existing domain name and ID that is not open to register
        client.setNodeGroupId("STORE");
        client.setExternalId("00001");
        Assert.assertFalse(registrationService.registerNode(client, out),
                "Client should NOT be allowed to register");

        // Existing domain name but wrong ID
        client.setNodeGroupId("STORE");
        client.setExternalId("wrongId");
        Assert.assertFalse(registrationService.registerNode(client, out),
                "Client should NOT be allowed to register");

        // Wrong domain name and wrong ID
        client.setNodeGroupId("wrongDomain");
        client.setExternalId("wrongId");
        Assert.assertFalse(registrationService.registerNode(client, out),
                "Client should NOT be allowed to register");
    }

    @Test(groups = "continuous")
    public void testRegisterClient() throws Exception {
        Node client = new Node();
        client.setNodeGroupId("STORE");
        client.setExternalId("00002");
        client.setSyncURL("http://localhost:8080/sync");
        client.setSchemaVersion("1");
        client.setDatabaseType("MySQL");
        client.setDatabaseVersion("5");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Assert.assertTrue(registrationService.registerNode(client, out),
                "Client should be allowed to register");

        client = nodeService.findNode("00002");
        Assert.assertEquals(client.getNodeId(), "00002", "Wrong clientId");
        Assert.assertEquals(client.getNodeGroupId(), "STORE", "Wrong domainName");
        Assert.assertEquals(client.getExternalId(), "00002", "Wrong domainId");
        Assert.assertEquals(client.getSyncURL().toString(), "http://localhost:8080/sync", "Wrong syncUrl");
        Assert.assertEquals(client.getSchemaVersion(), "1", "Wrong schemaVersion");
        Assert.assertEquals(client.getDatabaseType(), "MySQL", "Wrong databaseType");
        Assert.assertEquals(client.getDatabaseVersion(), "5", "Wrong databaseVersion");

        NodeSecurity security = nodeService.findNodeSecurity("00002");
        Assert.assertEquals(security.getNodeId(), "00002", "Wrong clientId");
        Assert.assertNotSame(security.getPassword(), null, "Wrong password");
        Assert.assertEquals(security.isRegistrationEnabled(), false, "Wrong isRegistrationEnabled");
        Assert.assertNotSame(security.getRegistrationTime(), null, "Wrong registrationTime");
    }

    @Test(groups = "continuous")
    public void testRegisterClientWithResponse() throws Exception {
        registrationService.openRegistration("test-root-group", "09999");

        Node client = new Node();
        client.setNodeGroupId("test-root-group");
        client.setExternalId("09999");
        client.setSyncURL("http://localhost:8080/sync");
        client.setSchemaVersion("1");
        client.setDatabaseType("MySQL");
        client.setDatabaseVersion("5");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Assert.assertTrue(registrationService.registerNode(client, out),
                "Client should be allowed to register");
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
        Assert.assertEquals(security.getNodeId(), "00003", "Wrong clientId");
        Assert.assertNotSame(security.getPassword(), "notsecret", "Wrong password");
        Assert.assertEquals(security.isRegistrationEnabled(), true, "Wrong isRegistrationEnabled");
        Assert.assertEquals(security.getRegistrationTime(), null, "Wrong registrationTime");

        Node client = new Node();
        client.setNodeGroupId("STORE");
        client.setExternalId("00003");
        client.setSyncURL("http://0:8080/sync");
        client.setSchemaVersion("1");
        client.setDatabaseType("hqsql");
        client.setDatabaseVersion("1");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Assert.assertTrue(registrationService.registerNode(client, out),
                "Client should be allowed to register");

        client = nodeService.findNode("00003");
        Assert.assertEquals(client.getNodeId(), "00003", "Wrong clientId");
        Assert.assertEquals(client.getNodeGroupId(), "STORE", "Wrong domainName");
        Assert.assertEquals(client.getExternalId(), "00003", "Wrong domainId");
        Assert.assertEquals(client.getSyncURL().toString(), "http://0:8080/sync", "Wrong syncUrl");
        Assert.assertEquals(client.getSchemaVersion(), "1", "Wrong schemaVersion");
        Assert.assertEquals(client.getDatabaseType(), "hqsql", "Wrong databaseType");
        Assert.assertEquals(client.getDatabaseVersion(), "1", "Wrong databaseVersion");

        security = nodeService.findNodeSecurity("00003");
        Assert.assertEquals(security.getNodeId(), "00003", "Wrong clientId");
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
        registrationService.openRegistration("STORE", "00005");

        Node client = nodeService.findNode("00005");
        Assert.assertEquals(client.getNodeId(), "00005", "Wrong clientId");
        Assert.assertEquals(client.getNodeGroupId(), "STORE", "Wrong domainName");
        Assert.assertEquals(client.getExternalId(), "00005", "Wrong domainId");
        Assert.assertEquals(client.getSyncURL(), null, "Wrong syncUrl");
        Assert.assertEquals(client.getSchemaVersion(), null, "Wrong schemaVersion");
        Assert.assertEquals(client.getDatabaseType(), null, "Wrong databaseType");
        Assert.assertEquals(client.getDatabaseVersion(), null, "Wrong databaseVersion");

        NodeSecurity security = nodeService.findNodeSecurity("00005");
        Assert.assertEquals(security.getNodeId(), "00005", "Wrong clientId");
        Assert.assertNotSame(security.getPassword(), null, "Wrong password");
        Assert.assertEquals(security.isRegistrationEnabled(), true, "Wrong isRegistrationEnabled");
        Assert.assertEquals(security.getRegistrationTime(), null, "Wrong registrationTime");
    }

    @Test(groups = "continuous")
    public void testOpenRegistrationOnceAndRegisterTwice() throws Exception {
        registrationService.openRegistration("STORE", "00006");
        Node client = new Node();
        client.setNodeGroupId("STORE");
        client.setExternalId("00006");
        client.setSyncURL("http://127.0.0.1");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Assert.assertTrue(registrationService.registerNode(client, out),
                "Client should be able to register");
        Assert.assertFalse(registrationService.registerNode(client, out),
                "Client should NOT be able to register");
    }

    @Test(groups = "continuous")
    public void testOpenRegistrationTwiceAndRegisterThrice() throws Exception {
        registrationService.openRegistration("STORE", "00007");
        registrationService.openRegistration("STORE", "00007");
        Node client = new Node();
        client.setNodeGroupId("STORE");
        client.setExternalId("00007");
        client.setSyncURL("http://0");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Assert.assertTrue(registrationService.registerNode(client, out),
                "Client should be able to register");
        Assert.assertTrue(registrationService.registerNode(client, out),
                "Client should be able to register");
        Assert.assertFalse(registrationService.registerNode(client, out),
                "Client should NOT be able to register");
    }

}
