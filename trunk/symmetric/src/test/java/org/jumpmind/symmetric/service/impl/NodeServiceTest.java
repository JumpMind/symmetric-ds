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

import java.util.List;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Before;
import org.junit.Test;

public class NodeServiceTest extends AbstractDatabaseTest {

    protected INodeService nodeService;

    public NodeServiceTest() throws Exception {
        super();
    }

    public NodeServiceTest(String dbName) {
        super(dbName);
    }

    @Before
    public void setUp() {
        nodeService = (INodeService) find(Constants.NODE_SERVICE);
    }

    @Test
    public void testFindNode() throws Exception {
        Node node = nodeService.findNode("00001");
        assertEquals(node.getNodeId(), "00001", "Wrong nodeId");
        assertEquals(node.getNodeGroupId(), TestConstants.TEST_CLIENT_NODE_GROUP, "Wrong node group id");
        assertEquals(node.getExternalId(), "00001", "Wrong external id");
        assertEquals(node.getSyncURL().toString(), "http://localhost:8080/sync", "Wrong syncUrl");
        assertEquals(node.getSchemaVersion(), "1", "Wrong schemaVersion");
        assertEquals(node.getDatabaseType(), "MySQL", "Wrong databaseType");
        assertEquals(node.getDatabaseVersion(), "5.0", "Wrong databaseVersion");
    }

    @Test
    public void testFindNodeFail() throws Exception {
        Node node = nodeService.findNode("00004");
        assertNull(node, "Should not find node");
    }

    @Test
    public void testFindNodeSecurity() throws Exception {
        NodeSecurity node = nodeService.findNodeSecurity("00001");
        assertEquals(node.getNodeId(), "00001", "Wrong nodeId");
        assertEquals(node.getPassword(), "secret", "Wrong password");
        assertEquals(node.isRegistrationEnabled(), false, "Wrong isRegistrationEnabled");
        assertEquals(node.getRegistrationTime().toString(), "2007-01-01 01:01:01.0", "Wrong registrationTime");
    }

    @Test
    public void testFindNodeSecurityFail() throws Exception {
        NodeSecurity node = nodeService.findNodeSecurity("00004");
        assertNull(node, "Should not find node");
    }

    @Test
    public void testIsNodeAuthorized() throws Exception {
        assertTrue(nodeService.isNodeAuthorized("00001", "secret"), "Node should be authorized");

        assertFalse(nodeService.isNodeAuthorized("00001", "wrongPassword"), "Node should NOT be authorized");

        assertFalse(nodeService.isNodeAuthorized("wrongNodeId", "secret"), "Node should NOT be authorized");
    }

    @Test
    public void testFindIdentity() throws Exception {
        Node node = nodeService.findIdentity();
        assertEquals(node.getNodeId(), "00000", "Wrong nodeId");
        assertEquals(node.getNodeGroupId(), TestConstants.TEST_ROOT_NODE_GROUP, "Wrong node group id");
        assertEquals(node.getExternalId(), TestConstants.TEST_ROOT_EXTERNAL_ID, "Wrong external id");
        assertEquals(node.getSyncURL().toString(), "http://localhost:8888/sync", "Wrong syncUrl");
        assertEquals(node.getSchemaVersion(), "?", "Wrong schemaVersion");
        assertEquals(node.getDatabaseType(), getDbDialect().getName(), "Wrong databaseType");
        assertEquals(node.getDatabaseVersion(), getDbDialect().getVersion(), "Wrong databaseVersion");
    }

    @Test
    public void testFindPullNodes() throws Exception {
        List<Node> list = nodeService.findNodesToPull();
        assertEquals(list.size(), 4, "Wrong number of pull nodes");
    }

    @Test
    public void testFindPushNodes() throws Exception {
        List<Node> list = nodeService.findNodesToPushTo();
        assertEquals(list.size(), 1, "Wrong number of push nodes");
    }

}
