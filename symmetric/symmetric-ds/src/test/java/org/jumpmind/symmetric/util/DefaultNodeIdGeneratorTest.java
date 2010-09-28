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

package org.jumpmind.symmetric.util;

import junit.framework.Assert;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.mock.MockNodeService;
import org.junit.Test;

/**
 * 
 */
public class DefaultNodeIdGeneratorTest {

    @Test
    public void testSelectNodeId() throws Exception {
        final String EXPECTED_NODE_ID = "100-2";
        DefaultNodeIdGenerator generator = new DefaultNodeIdGenerator();
        Node node = new Node();
        node.setExternalId("100");
        String selectedNodeId = generator.selectNodeId(new MockNodeService() {
            @Override
            public NodeSecurity findNodeSecurity(String nodeId) {
                if (nodeId.equals(EXPECTED_NODE_ID)) {
                    NodeSecurity security = new NodeSecurity();
                    security.setNodeId(EXPECTED_NODE_ID);
                    security.setRegistrationEnabled(true);
                    return security;
                } else {
                    return null;
                }
            }
        }, node);
        Assert.assertEquals(EXPECTED_NODE_ID, selectedNodeId);
    }

    @Test
    public void testSelectNodeIdWithNodeIdSet() throws Exception {
        DefaultNodeIdGenerator generator = new DefaultNodeIdGenerator();
        Node node = new Node();
        final String EXPECTED_NODE_ID = "10001";
        node.setExternalId(EXPECTED_NODE_ID);
        node.setNodeId(EXPECTED_NODE_ID);
        String selectedNodeId = generator.selectNodeId(new MockNodeService(), node);
        Assert.assertEquals(EXPECTED_NODE_ID, selectedNodeId);
    }

    @Test
    public void testGenerateNodeIdNoExisting() throws Exception {
        final String EXPECTED_NODE_ID = "100";
        DefaultNodeIdGenerator generator = new DefaultNodeIdGenerator();
        Node node = new Node();
        node.setExternalId("100");
        String selectedNodeId = generator.generateNodeId(new MockNodeService(), node);
        Assert.assertEquals(EXPECTED_NODE_ID, selectedNodeId);
    }

    @Test
    public void testGenerateNodeIdExisting() throws Exception {
        final String EXPECTED_NODE_ID = "100-0";
        DefaultNodeIdGenerator generator = new DefaultNodeIdGenerator();
        Node node = new Node();
        node.setExternalId("100");
        String selectedNodeId = generator.generateNodeId(new MockNodeService() {
            @Override
            public Node findNode(String nodeId) {
                if (nodeId.equals("100")) {
                    Node node = new Node();
                    node.setNodeId("100");
                    return node;
                } else {
                    return null;
                }
            }
        }, node);
        Assert.assertEquals(EXPECTED_NODE_ID, selectedNodeId);
    }

    @Test
    public void testGenerateNodeIdExistingAll() throws Exception {
        DefaultNodeIdGenerator generator = new DefaultNodeIdGenerator();
        Node node = new Node();
        node.setExternalId("100");
        try {
            generator.generateNodeId(new MockNodeService() {
                @Override
                public Node findNode(String nodeId) {
                    Node node = new Node();
                    node.setNodeId(nodeId);
                    return node;
                }
            }, node);
            Assert.fail("Should have received exception");
        } catch (Exception e) {
            // Expected
        }
    }

    @Test
    public void testGenerateNodeIdWithNodeIdSet() throws Exception {
        DefaultNodeIdGenerator generator = new DefaultNodeIdGenerator();
        Node node = new Node();
        final String EXPECTED_NODE_ID = "10001";
        node.setExternalId(EXPECTED_NODE_ID);
        node.setNodeId(EXPECTED_NODE_ID);
        String selectedNodeId = generator.generateNodeId(new MockNodeService(), node);
        Assert.assertEquals(EXPECTED_NODE_ID, selectedNodeId);
    }

}