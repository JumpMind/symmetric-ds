package org.jumpmind.symmetric.core.service;

import junit.framework.Assert;

import org.jumpmind.symmetric.core.model.Node;
import org.junit.Test;

abstract public class AbstractNodeServiceTest extends AbstractServiceTest {

    @Test
    public void testSaveReadDeleteNode() {
       NodeService nodeService = client.getNodeService();
       Node lookupNode = nodeService.findNode("2000");
       Assert.assertNull(lookupNode);
       Node newNode = new Node("2000", "testgroup");
       newNode.setCreatedAtNodeId("0000");
       nodeService.saveNode(newNode);
       lookupNode = nodeService.findNode("2000");
       Assert.assertNotNull(lookupNode);
       Assert.assertEquals(newNode.getNodeId(), lookupNode.getNodeId());
       Assert.assertEquals(newNode.getNodeGroupId(), lookupNode.getNodeGroupId());
       
       nodeService.deleteNode(lookupNode);
       lookupNode = nodeService.findNode("2000");
       Assert.assertNull(lookupNode);
       
       
    }
}
