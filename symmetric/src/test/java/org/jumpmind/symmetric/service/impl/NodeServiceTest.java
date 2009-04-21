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

import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Assert;
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
        assertEquals(node.getDatabaseType(), "H2", "Wrong databaseType");
        assertEquals(node.getDatabaseVersion(), "5.0", "Wrong databaseVersion");
    }

    @Test
    public void testUpdateNode() throws Exception {
        Node node = nodeService.findNode("00001");
        nodeService.updateNode(node);
        node = nodeService.findNode("00001");
        assertEquals(node.getNodeId(), "00001", "Wrong nodeId");
        assertEquals(node.getNodeGroupId(), TestConstants.TEST_CLIENT_NODE_GROUP, "Wrong node group id");
        assertEquals(node.getExternalId(), "00001", "Wrong external id");
        assertEquals(node.getSyncURL().toString(), "http://localhost:8080/sync", "Wrong syncUrl");
        assertEquals(node.getSchemaVersion(), "1", "Wrong schemaVersion");
        assertEquals(node.getDatabaseType(), "H2", "Wrong databaseType");
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
        assertEquals(node.getSyncURL().toString(), "internal://root",
                "Wrong syncUrl");
        assertEquals(node.getSchemaVersion(), "1", "Wrong schemaVersion");
        assertEquals(node.getDatabaseType(), "H2", "Wrong databaseType");
        assertEquals(node.getDatabaseVersion(), "1.1", "Wrong databaseVersion");
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

    @Test
    public void testFindNodesThatOriginatedHere() throws Exception {
        Set<Node> nodes = nodeService.findNodesThatOriginatedFromNodeId("00011");
        Assert.assertEquals(4, nodes.size());
        Set<String> expectedIds = new HashSet<String>();
        expectedIds.add("44001");
        expectedIds.add("44003");
        expectedIds.add("44005");
        expectedIds.add("44006");
        for (Node n : nodes) {
            Assert.assertTrue(expectedIds.contains(n.getNodeId()));
        }

        nodes = nodeService.findNodesThatOriginatedFromNodeId("00001");
        Assert.assertEquals(0, nodes.size());

    }

    @Test
    public void testIsDataLoadStartedOrCompleted() throws Exception {
        Node node = nodeService.findIdentity();
        NodeSecurity originalNodeSecurity = nodeService.findNodeSecurity(node.getNodeId());

        try {
            NodeSecurity nodeSecurity = nodeService.findNodeSecurity(node.getNodeId());
            nodeSecurity.setNodeId(node.getNodeId());

            // Fresh node

            nodeSecurity.setInitialLoadEnabled(false);
            nodeSecurity.setInitialLoadTime(null);
            nodeService.updateNodeSecurity(nodeSecurity);
            Assert.assertFalse(nodeService.isDataLoadStarted());
            Assert.assertFalse(nodeService.isDataLoadCompleted());

            // Initial load started but not completed.

            nodeSecurity.setInitialLoadEnabled(true);
            nodeSecurity.setInitialLoadTime(null);
            nodeService.updateNodeSecurity(nodeSecurity);
            Assert.assertTrue(nodeService.isDataLoadStarted());
            Assert.assertFalse(nodeService.isDataLoadCompleted());

            // Initial load completed.

            nodeSecurity.setInitialLoadEnabled(false);
            nodeSecurity.setInitialLoadTime(Calendar.getInstance().getTime());
            nodeService.updateNodeSecurity(nodeSecurity);
            Assert.assertFalse(nodeService.isDataLoadStarted());
            Assert.assertTrue(nodeService.isDataLoadCompleted());

            // Erroneous configuration - if load is complete, time should be set
            // and enabled should be 0
            // Expected behavior is to report it as data load started but not
            // completed.

            nodeSecurity.setInitialLoadEnabled(true);
            nodeSecurity.setInitialLoadTime(Calendar.getInstance().getTime());
            nodeService.updateNodeSecurity(nodeSecurity);
            Assert.assertTrue(nodeService.isDataLoadStarted());
            Assert.assertFalse(nodeService.isDataLoadCompleted());

        } finally {
            nodeService.updateNodeSecurity(originalNodeSecurity);
        }
    }
}