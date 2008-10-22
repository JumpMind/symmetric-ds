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

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Before;
import org.junit.Test;

public class RegistrationServiceTest extends AbstractDatabaseTest {

    protected INodeService nodeService;

    protected IRegistrationService registrationService;

    public RegistrationServiceTest() throws Exception {
        super();
    }

    public RegistrationServiceTest(String dbName) {
        super(dbName);
    }

    @Before
    public void setUp() {
        nodeService = (INodeService) find(Constants.NODE_SERVICE);
        registrationService = (IRegistrationService) find(Constants.REGISTRATION_SERVICE);
    }

    @Test
    public void testRegisterNodeFail() throws Exception {
        Node node = new Node();
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        // Existing domain name and ID that is not open to register
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00001");
        assertFalse(registrationService.registerNode(node, out), "Node should NOT be allowed to register");

        // Existing domain name but wrong ID
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("wrongId");
        assertFalse(registrationService.registerNode(node, out), "Node should NOT be allowed to register");

        // Wrong domain name and wrong ID
        node.setNodeGroupId("wrongDomain");
        node.setExternalId("wrongId");
        assertFalse(registrationService.registerNode(node, out), "Node should NOT be allowed to register");
    }

    @Test
    public void testRegisterNode() throws Exception {
        Node node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00002");
        node.setSyncURL("http://localhost:8080/sync");
        node.setSchemaVersion("1");
        node.setDatabaseType("MySQL");
        node.setDatabaseVersion("5");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertTrue(registrationService.registerNode(node, out), "Node should be allowed to register");

        node = nodeService.findNode("00002");
        assertEquals(node.getNodeId(), "00002", "Wrong nodeId");
        assertEquals(node.getNodeGroupId(), TestConstants.TEST_CLIENT_NODE_GROUP, "Wrong domainName");
        assertEquals(node.getExternalId(), "00002", "Wrong domainId");
        assertEquals(node.getSyncURL().toString(), "http://localhost:8080/sync", "Wrong syncUrl");
        assertEquals(node.getSchemaVersion(), "1", "Wrong schemaVersion");
        assertEquals(node.getDatabaseType(), "MySQL", "Wrong databaseType");
        assertEquals(node.getDatabaseVersion(), "5", "Wrong databaseVersion");

        NodeSecurity security = nodeService.findNodeSecurity("00002");
        assertEquals(security.getNodeId(), "00002", "Wrong nodeId");
        assertNotSame(security.getPassword(), null, "Wrong password");
        assertEquals(security.isRegistrationEnabled(), false, "Wrong isRegistrationEnabled");
        assertNotSame(security.getRegistrationTime(), null, "Wrong registrationTime");
    }

    @Test
    public void testRegisterNodeAutomatic() throws Exception {
        try {

            getParameterService().saveParameter(ParameterConstants.AUTO_REGISTER_ENABLED, true);
            doTestRegisterNodeAutomatic();
        } finally {
            getParameterService().saveParameter(ParameterConstants.AUTO_REGISTER_ENABLED, false);
        }
    }

    private void doTestRegisterNodeAutomatic() throws Exception {
        Node node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00008");
        node.setSyncURL("http://localhost:8080/sync");
        node.setSchemaVersion("1");
        node.setDatabaseType("MySQL");
        node.setDatabaseVersion("5");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertTrue(registrationService.registerNode(node, out), "Node should be allowed to register");

        node = nodeService.findNode("00008");
        assertEquals(node.getNodeId(), "00008", "Wrong nodeId");
        assertEquals(node.getNodeGroupId(), TestConstants.TEST_CLIENT_NODE_GROUP, "Wrong domainName");
        assertEquals(node.getExternalId(), "00008", "Wrong domainId");
        assertEquals(node.getSyncURL().toString(), "http://localhost:8080/sync", "Wrong syncUrl");
        assertEquals(node.getSchemaVersion(), "1", "Wrong schemaVersion");
        assertEquals(node.getDatabaseType(), "MySQL", "Wrong databaseType");
        assertEquals(node.getDatabaseVersion(), "5", "Wrong databaseVersion");

        NodeSecurity security = nodeService.findNodeSecurity("00008");
        assertEquals(security.getNodeId(), "00008", "Wrong nodeId");
        assertNotSame(security.getPassword(), null, "Wrong password");
        assertEquals(security.isRegistrationEnabled(), false, "Wrong isRegistrationEnabled");
        assertNotSame(security.getRegistrationTime(), null, "Wrong registrationTime");

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
        assertTrue(registrationService.registerNode(node, out), "Node should be allowed to register");
    }

    @Test
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
        assertTrue(registrationService.registerNode(node, out), "Node should be allowed to register");
        out.close();
        String response = out.toString();
        assertTrue(response.indexOf("batch,") != -1, "Expected batch");
        assertTrue(response.indexOf("table,") != -1, "Expected table");
        assertTrue(response.indexOf("keys,") != -1, "Expected table");
        assertTrue(response.indexOf("columns,") != -1, "Expected table");
        assertTrue(response.indexOf("insert,") != -1, "Expected insert");
        assertTrue(response.indexOf("commit,") != -1, "Expected commit");
    }

    @Test
    public void testReOpenRegistration() throws Exception {
        registrationService.reOpenRegistration("00003");
        NodeSecurity security = nodeService.findNodeSecurity("00003");
        assertEquals(security.getNodeId(), "00003", "Wrong nodeId");
        assertNotSame(security.getPassword(), "notsecret", "Wrong password");
        assertEquals(security.isRegistrationEnabled(), true, "Wrong isRegistrationEnabled");
        assertEquals(security.getRegistrationTime(), null, "Wrong registrationTime");

        Node node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00003");
        node.setSyncURL("http://0:8080/sync");
        node.setSchemaVersion("1");
        node.setDatabaseType("hqsql");
        node.setDatabaseVersion("1");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertTrue(registrationService.registerNode(node, out), "Node should be allowed to register");

        node = nodeService.findNode("00003");
        assertEquals(node.getNodeId(), "00003", "Wrong nodeId");
        assertEquals(node.getNodeGroupId(), TestConstants.TEST_CLIENT_NODE_GROUP, "Wrong domainName");
        assertEquals(node.getExternalId(), "00003", "Wrong domainId");
        assertEquals(node.getSyncURL().toString(), "http://0:8080/sync", "Wrong syncUrl");
        assertEquals(node.getSchemaVersion(), "1", "Wrong schemaVersion");
        assertEquals(node.getDatabaseType(), "hqsql", "Wrong databaseType");
        assertEquals(node.getDatabaseVersion(), "1", "Wrong databaseVersion");

        security = nodeService.findNodeSecurity("00003");
        assertEquals(security.getNodeId(), "00003", "Wrong nodeId");
        assertNotSame(security.getPassword(), "notsecret", "Wrong password");
        assertEquals(security.isRegistrationEnabled(), false, "Wrong isRegistrationEnabled");
        assertNotSame(security.getRegistrationTime(), null, "Wrong registrationTime");
    }

    @Test
    public void testReOpenRegistrationFail() throws Exception {
        registrationService.reOpenRegistration("00004");
        NodeSecurity security = nodeService.findNodeSecurity("00004");
        assertNull(security, "Should not be able to re-open registration");
    }

    @Test
    public void testOpenRegistration() throws Exception {
        registrationService.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, "00005");

        Node node = nodeService.findNode("00005");
        assertEquals(node.getNodeId(), "00005", "Wrong nodeId");
        assertEquals(node.getNodeGroupId(), TestConstants.TEST_CLIENT_NODE_GROUP, "Wrong domainName");
        assertEquals(node.getExternalId(), "00005", "Wrong domainId");
        assertEquals(node.getSyncURL(), null, "Wrong syncUrl");
        assertEquals(node.getSchemaVersion(), null, "Wrong schemaVersion");
        assertEquals(node.getDatabaseType(), null, "Wrong databaseType");
        assertEquals(node.getDatabaseVersion(), null, "Wrong databaseVersion");

        NodeSecurity security = nodeService.findNodeSecurity("00005");
        assertEquals(security.getNodeId(), "00005", "Wrong nodeId");
        assertNotSame(security.getPassword(), null, "Wrong password");
        assertEquals(security.isRegistrationEnabled(), true, "Wrong isRegistrationEnabled");
        assertEquals(security.getRegistrationTime(), null, "Wrong registrationTime");
    }

    @Test
    public void testOpenRegistrationOnceAndRegisterTwice() throws Exception {
        registrationService.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, "00006");
        Node node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00006");
        node.setSyncURL("http://127.0.0.1");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertTrue(registrationService.registerNode(node, out), "Node should be able to register");
        assertFalse(registrationService.registerNode(node, out), "Node should NOT be able to register");
    }

    @Test
    public void testOpenRegistrationTwiceAndRegisterThrice() throws Exception {
        registrationService.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, "00007");
        registrationService.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, "00007");
        Node node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00007");
        node.setSyncURL("http://0");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertTrue(registrationService.registerNode(node, out), "Node should be able to register");
        assertTrue(registrationService.registerNode(node, out), "Node should be able to register");
        assertFalse(registrationService.registerNode(node, out), "Node should NOT be able to register");
    }

}
