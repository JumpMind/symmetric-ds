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
 * under the License.  */


package org.jumpmind.symmetric.service.impl;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import junit.framework.Assert;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Before;
import org.junit.Test;

public class RegistrationServiceTest extends AbstractDatabaseTest {

    protected INodeService nodeService;

    protected IRegistrationService registrationService;

    protected IParameterService parameterService;

    public RegistrationServiceTest() throws Exception {
        super();
    }

    @Before
    public void setUp() {
        nodeService = find(Constants.NODE_SERVICE);
        registrationService = find(Constants.REGISTRATION_SERVICE);
        parameterService = find(Constants.PARAMETER_SERVICE);
    }

    @Test
    public void testRegisterNodeFail() throws Exception {
        Node node = new Node();
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        // Existing domain name and ID that is not open to register
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00001");
        assertFalse(registrationService.registerNode(node, out, false),
                "Node should NOT be allowed to register");

        // Existing domain name but wrong ID
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("wrongId");
        assertFalse(registrationService.registerNode(node, out, false),
                "Node should NOT be allowed to register");

        // Wrong domain name and wrong ID
        node.setNodeGroupId("wrongDomain");
        node.setExternalId("wrongId");
        assertFalse(registrationService.registerNode(node, out, false),
                "Node should NOT be allowed to register");
    }

    @Test
    public void testRegisterNode() throws Exception {
        Node node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00002");
        node.setSyncUrl("http://localhost:8080/sync");
        node.setSchemaVersion("1");
        node.setDatabaseType("MySQL");
        node.setDatabaseVersion("5");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertTrue(registrationService.registerNode(node, out, false),
                "Node should be allowed to register");

        registrationService.markNodeAsRegistered("00002");

        node = nodeService.findNode("00002");
        assertEquals(node.getNodeId(), "00002", "Wrong nodeId");
        assertEquals(node.getNodeGroupId(), TestConstants.TEST_CLIENT_NODE_GROUP,
                "Wrong domainName");
        assertEquals(node.getExternalId(), "00002", "Wrong domainId");
        assertEquals(node.getSyncUrl().toString(), "http://localhost:8080/sync", "Wrong syncUrl");
        assertEquals(node.getSchemaVersion(), "1", "Wrong schemaVersion");
        assertEquals(node.getDatabaseType(), "MySQL", "Wrong databaseType");
        assertEquals(node.getDatabaseVersion(), "5", "Wrong databaseVersion");

        NodeSecurity security = nodeService.findNodeSecurity("00002");
        assertEquals(security.getNodeId(), "00002", "Wrong nodeId");
        assertNotSame(security.getNodePassword(), null, "Wrong password");
        assertEquals(security.isRegistrationEnabled(), false, "Wrong isRegistrationEnabled");
        assertNotSame(security.getRegistrationTime(), null, "Wrong registrationTime");
    }

    @Test
    public void testGetRegistrationRedirectMap() {
        Map<String, String> redirectMap = registrationService.getRegistrationRedirectMap();
        Assert.assertEquals(0, redirectMap.size());

        getJdbcTemplate()
                .update(
                        "insert into sym_registration_redirect (registrant_external_id,registration_node_id) values ('test', '00011')");
        getJdbcTemplate()
                .update(
                        "insert into sym_registration_redirect (registrant_external_id,registration_node_id) values ('00010', '00011')");
        getJdbcTemplate()
                .update(
                        "insert into sym_registration_redirect (registrant_external_id,registration_node_id) values ('test2', 'test3')");
        redirectMap = registrationService.getRegistrationRedirectMap();

        Assert.assertEquals(3, redirectMap.size());
        Assert.assertEquals("00011", redirectMap.get("test"));
        Assert.assertEquals("00011", redirectMap.get("00010"));
        Assert.assertEquals("test3", redirectMap.get("test2"));
    }

    @Test
    public void testRegisterWithBlankExternalId() throws Exception {
        Node node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId(null);
        node.setSyncUrl("http://localhost:8080/sync");
        node.setSchemaVersion("1");
        node.setDatabaseType("MySQL");
        node.setDatabaseVersion("5");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertFalse(registrationService.registerNode(node, out, true),
                "Node should not be allowed to register");
        registrationService.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, "0");
        node.setExternalId(null);
        node.setNodeId(null);
        assertTrue(registrationService.registerNode(node, out, true),
                "Node should be allowed to register");
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
        node.setSyncUrl("http://localhost:8080/sync");
        node.setSchemaVersion("1");
        node.setDatabaseType("MySQL");
        node.setDatabaseVersion("5");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertTrue(registrationService.registerNode(node, out, false),
                "Node should be allowed to register");

        registrationService.markNodeAsRegistered("00008");

        node = nodeService.findNode("00008");
        assertEquals(node.getNodeId(), "00008", "Wrong nodeId");
        assertEquals(node.getNodeGroupId(), TestConstants.TEST_CLIENT_NODE_GROUP,
                "Wrong domainName");
        assertEquals(node.getExternalId(), "00008", "Wrong domainId");
        assertEquals(node.getSyncUrl().toString(), "http://localhost:8080/sync", "Wrong syncUrl");
        assertEquals(node.getSchemaVersion(), "1", "Wrong schemaVersion");
        assertEquals(node.getDatabaseType(), "MySQL", "Wrong databaseType");
        assertEquals(node.getDatabaseVersion(), "5", "Wrong databaseVersion");

        NodeSecurity security = nodeService.findNodeSecurity("00008");
        assertEquals(security.getNodeId(), "00008", "Wrong nodeId");
        assertNotSame(security.getNodePassword(), null, "Wrong password");
        assertEquals(security.isRegistrationEnabled(), false, "Wrong isRegistrationEnabled");
        assertNotSame(security.getRegistrationTime(), null, "Wrong registrationTime");

        // Make sure opening registration still works with auto-registration
        registrationService.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, "00009");
        node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00009");
        node.setSyncUrl("http://localhost:8080/sync");
        node.setSchemaVersion("1");
        node.setDatabaseType("MySQL");
        node.setDatabaseVersion("5");
        out = new ByteArrayOutputStream();
        assertTrue(registrationService.registerNode(node, out, false),
                "Node should be allowed to register");
    }

    @Test
    public void testRegisterNodeWithResponse() throws Exception {
        registrationService.openRegistration("test-root-group", "09999");

        Node node = new Node();
        node.setNodeGroupId("test-root-group");
        node.setExternalId("09999");
        node.setSyncUrl("http://localhost:8080/sync");
        node.setSchemaVersion("1");
        node.setDatabaseType("MySQL");
        node.setDatabaseVersion("5");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertTrue(registrationService.registerNode(node, out, false),
                "Node should be allowed to register");
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
        assertNotSame(security.getNodePassword(), "notsecret", "Wrong password");
        assertEquals(security.isRegistrationEnabled(), true, "Wrong isRegistrationEnabled");
        assertEquals(security.getRegistrationTime(), null, "Wrong registrationTime");

        Node node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00003");
        node.setSyncUrl("http://0:8080/sync");
        node.setSchemaVersion("1");
        node.setDatabaseType("hqsql");
        node.setDatabaseVersion("1");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertTrue(registrationService.registerNode(node, out, false),
                "Node should be allowed to register");

        registrationService.markNodeAsRegistered("00003");

        node = nodeService.findNode("00003");
        assertEquals(node.getNodeId(), "00003", "Wrong nodeId");
        assertEquals(node.getNodeGroupId(), TestConstants.TEST_CLIENT_NODE_GROUP,
                "Wrong domainName");
        assertEquals(node.getExternalId(), "00003", "Wrong domainId");
        assertEquals(node.getSyncUrl().toString(), "http://0:8080/sync", "Wrong syncUrl");
        assertEquals(node.getSchemaVersion(), "1", "Wrong schemaVersion");
        assertEquals(node.getDatabaseType(), "hqsql", "Wrong databaseType");
        assertEquals(node.getDatabaseVersion(), "1", "Wrong databaseVersion");

        security = nodeService.findNodeSecurity("00003");
        assertEquals(security.getNodeId(), "00003", "Wrong nodeId");
        assertNotSame(security.getNodePassword(), "notsecret", "Wrong password");
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
    public void testReOpenRegistrationMissing() throws Exception {
        registrationService.reOpenRegistration("00003");
        NodeSecurity security = nodeService.findNodeSecurity("00003");
        assertEquals(security.getNodeId(), "00003", "Wrong nodeId");
        assertNotSame(security.getNodePassword(), "notsecret", "Wrong password");
        assertEquals(security.isRegistrationEnabled(), true, "Wrong isRegistrationEnabled");
        assertEquals(security.getRegistrationTime(), null, "Wrong registrationTime");

        getJdbcTemplate().update("delete from sym_node_security where node_id = '00003'");

        registrationService.reOpenRegistration("00003");
        security = nodeService.findNodeSecurity("00003");
        assertEquals(security.getNodeId(), "00003", "Wrong nodeId");
        assertNotSame(security.getNodePassword(), "notsecret", "Wrong password");
        assertEquals(security.isRegistrationEnabled(), true, "Wrong isRegistrationEnabled");
        assertEquals(security.getRegistrationTime(), null, "Wrong registrationTime");
    }

    @Test
    public void testOpenRegistration() throws Exception {
        registrationService.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, "00005");

        Node node = nodeService.findNode("00005");
        assertEquals(node.getNodeId(), "00005", "Wrong nodeId");
        assertEquals(node.getNodeGroupId(), TestConstants.TEST_CLIENT_NODE_GROUP,
                "Wrong domainName");
        assertEquals(node.getExternalId(), "00005", "Wrong domainId");
        assertEquals(node.getSyncUrl(), null, "Wrong syncUrl");
        assertEquals(node.getSchemaVersion(), null, "Wrong schemaVersion");
        assertEquals(node.getDatabaseType(), null, "Wrong databaseType");
        assertEquals(node.getDatabaseVersion(), null, "Wrong databaseVersion");

        NodeSecurity security = nodeService.findNodeSecurity("00005");
        assertEquals(security.getNodeId(), "00005", "Wrong nodeId");
        assertNotSame(security.getNodePassword(), null, "Wrong password");
        assertEquals(security.isRegistrationEnabled(), true, "Wrong isRegistrationEnabled");
        assertEquals(security.getRegistrationTime(), null, "Wrong registrationTime");
    }

    @Test
    public void testOpenRegistrationOnceAndRegisterTwice() throws Exception {
        registrationService.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, "00006");
        Node node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00006");
        node.setSyncUrl("http://127.0.0.1");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertTrue(registrationService.registerNode(node, out, false),
                "Node should be able to register");
        registrationService.markNodeAsRegistered("00006");
        assertFalse(registrationService.registerNode(node, out, false),
                "Node should NOT be able to register");
    }

    @Test
    public void testOpenRegistrationTwiceAndRegisterThrice() throws Exception {
        registrationService.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, "00007");
        registrationService.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, "00007");
        Node node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00007");
        node.setSyncUrl("http://0");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertTrue(registrationService.registerNode(node, out, false),
                "Node should be able to register");
        registrationService.markNodeAsRegistered("00007");
        node.setNodeId(null);
        assertTrue(registrationService.registerNode(node, out, false),
                "Node should be able to register");
        registrationService.markNodeAsRegistered("00007-0");
        assertFalse(registrationService.registerNode(node, out, false),
                "Node should NOT be able to register");
    }

    @Test
    public void testOpenRegistrationOfOlderVersionClient() throws Exception {
        registrationService.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, "00012");
        Node node = new Node();
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        node.setExternalId("00012");
        node.setSymmetricVersion("1.5.0");
        node.setSyncUrl("http://127.0.0.1");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertTrue(registrationService.registerNode(node, out, false),
                "Node should be able to register");
        // older versions of software to not ack. let's simulate this by not
        // marking the node as registered
        // registrationService.markNodeAsRegistered("00006");
        assertFalse(registrationService.registerNode(node, out, false),
                "Node should NOT be able to register");
    }

    @Test
    public void testGetRedirectionUrlFor() throws Exception {
        final String EXPECTED_REDIRECT_URL = "http://snoopdog.com";
        registrationService.saveRegistrationRedirect("4444", "55555");
        String url = registrationService.getRedirectionUrlFor("4444");
        Assert.assertEquals(EXPECTED_REDIRECT_URL, url);
        url = registrationService.getRedirectionUrlFor("44445");
        Assert.assertNull(url);
    }
    
    @Test
    public void testGetRegistrationRequests() {
        Assert.assertNotNull(registrationService.getRegistrationRequests(false));
    }

}