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

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class NodeServiceTest extends AbstractDatabaseTest {

    protected INodeService nodeService;
    
    @BeforeTest(groups = "continuous")
    protected void setUp() {
        nodeService = (INodeService) getBeanFactory().getBean(Constants.NODE_SERVICE);
    }

    @Test(groups = "continuous")
    public void testFindNode() throws Exception {
        Node node = nodeService.findNode("00001");
        Assert.assertEquals(node.getNodeId(), "00001", "Wrong nodeId");
        Assert.assertEquals(node.getNodeGroupId(), TestConstants.TEST_CLIENT_NODE_GROUP, "Wrong node group id");
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
        Assert.assertEquals(node.getNodeId(), "00000", "Wrong nodeId");
        Assert.assertEquals(node.getNodeGroupId(), TestConstants.TEST_ROOT_NODE_GROUP, "Wrong node group id");
        Assert.assertEquals(node.getExternalId(), TestConstants.TEST_ROOT_EXTERNAL_ID, "Wrong external id");
        Assert.assertEquals(node.getSyncURL().toString(), "internal://root", "Wrong syncUrl");
        Assert.assertEquals(node.getSchemaVersion(), "?", "Wrong schemaVersion");
        Assert.assertEquals(node.getDatabaseType(), getDbDialect().getName(), "Wrong databaseType");
        Assert.assertEquals(node.getDatabaseVersion(), getDbDialect().getVersion(), "Wrong databaseVersion");
    }

    @Test(groups = "continuous")
    public void testFindPullNodes() throws Exception {
        List<Node> list = nodeService.findNodesToPull();
        Assert.assertEquals(list.size(), 3, "Wrong number of pull nodes");
    }

    @Test(groups = "continuous")
    public void testFindPushNodes() throws Exception {
        List<Node> list = nodeService.findNodesToPushTo();
        Assert.assertEquals(list.size(), 1, "Wrong number of push nodes");
    }

}
