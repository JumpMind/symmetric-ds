package org.jumpmind.symmetric.route;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.NetworkedNode;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.junit.Test;

public class ConfigurationChangedDataRouterTest {

    private static List<NodeGroupLink> THREE_TIER_LINKS;

    private static NetworkedNode THREE_TIER_NETWORKED_ROOT;
    
    private static List<NodeGroupLink> MULTIPLE_GROUPS_PLUS_REG_SVR_LINKS;

    private static NetworkedNode MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT;

    static {
        THREE_TIER_LINKS = new ArrayList<NodeGroupLink>();
        THREE_TIER_LINKS.add(new NodeGroupLink("corp", "region"));
        THREE_TIER_LINKS.add(new NodeGroupLink("region", "corp"));
        THREE_TIER_LINKS.add(new NodeGroupLink("region", "laptop"));
        THREE_TIER_LINKS.add(new NodeGroupLink("laptop", "region"));

        THREE_TIER_NETWORKED_ROOT = new NetworkedNode(new Node("corp", "corp"));

        Node rgn1 = new Node("rgn1", "region");
        rgn1.setCreatedAtNodeId("corp");
        THREE_TIER_NETWORKED_ROOT.addChild(new NetworkedNode(rgn1));

        Node rgn2 = new Node("rgn2", "region");
        rgn2.setCreatedAtNodeId("corp");
        THREE_TIER_NETWORKED_ROOT.addChild(new NetworkedNode(rgn2));

        Node laptop1 = new Node("laptop1", "laptop");
        laptop1.setCreatedAtNodeId("rgn1");
        THREE_TIER_NETWORKED_ROOT.findNetworkedNode(laptop1.getCreatedAtNodeId()).addChild(
                new NetworkedNode(laptop1));

        Node laptop2 = new Node("laptop2", "laptop");
        laptop2.setCreatedAtNodeId("rgn2");
        THREE_TIER_NETWORKED_ROOT.findNetworkedNode(laptop2.getCreatedAtNodeId()).addChild(
                new NetworkedNode(laptop2));
        
        MULTIPLE_GROUPS_PLUS_REG_SVR_LINKS = new ArrayList<NodeGroupLink>();
        MULTIPLE_GROUPS_PLUS_REG_SVR_LINKS.add(new NodeGroupLink("regsvr", "s1"));
        MULTIPLE_GROUPS_PLUS_REG_SVR_LINKS.add(new NodeGroupLink("regsvr", "s2"));
        MULTIPLE_GROUPS_PLUS_REG_SVR_LINKS.add(new NodeGroupLink("regsvr", "dw"));
        MULTIPLE_GROUPS_PLUS_REG_SVR_LINKS.add(new NodeGroupLink("s1", "regsvr"));
        MULTIPLE_GROUPS_PLUS_REG_SVR_LINKS.add(new NodeGroupLink("s2", "regsvr"));
        MULTIPLE_GROUPS_PLUS_REG_SVR_LINKS.add(new NodeGroupLink("dw", "regsvr"));
        MULTIPLE_GROUPS_PLUS_REG_SVR_LINKS.add(new NodeGroupLink("s1", "dw"));
        MULTIPLE_GROUPS_PLUS_REG_SVR_LINKS.add(new NodeGroupLink("s2", "dw"));
        
        MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT = new NetworkedNode(new Node("regsvr", "regsvr"));
        
        Node node = null;
        
        node = new Node("s1", "s1");
        node.setCreatedAtNodeId("regsvr");
        MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT.addChild(new NetworkedNode(node));
        
        node = new Node("s2", "s2");
        node.setCreatedAtNodeId("regsvr");
        MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT.addChild(new NetworkedNode(node));

        node = new Node("dw", "dw");
        node.setCreatedAtNodeId("regsvr");
        MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT.addChild(new NetworkedNode(node));

        
    }

    @Test
    public void testRouteHeartbeatToParent() {
        IDataRouter router = buildTestableRouter(
                THREE_TIER_NETWORKED_ROOT.findNetworkedNode("laptop1").getNode(), THREE_TIER_LINKS,
                THREE_TIER_NETWORKED_ROOT);
        Set<Node> nodes = new HashSet<Node>();
        nodes.add(THREE_TIER_NETWORKED_ROOT.findNetworkedNode("rgn1").getNode());
        Collection<String> nodeIds = router.routeToNodes(new SimpleRouterContext(), buildDataMetaData("SYM_NODE", "laptop1"), nodes, false);
        Assert.assertNotNull(nodeIds);
        Assert.assertEquals(1, nodeIds.size());
        Assert.assertEquals("rgn1", nodeIds.iterator().next());
    }
    
    @Test
    public void testRouteLaptop1FromCorp() {
        IDataRouter router = buildTestableRouter(
                THREE_TIER_NETWORKED_ROOT.findNetworkedNode("corp").getNode(), THREE_TIER_LINKS,
                THREE_TIER_NETWORKED_ROOT);
        Set<Node> nodes = new HashSet<Node>();
        nodes.add(THREE_TIER_NETWORKED_ROOT.findNetworkedNode("rgn1").getNode());
        nodes.add(THREE_TIER_NETWORKED_ROOT.findNetworkedNode("rgn2").getNode());
        Collection<String> nodeIds = router.routeToNodes(new SimpleRouterContext(), buildDataMetaData("SYM_NODE", "laptop1"), nodes, false);
        Assert.assertNotNull(nodeIds);
        Assert.assertEquals(1, nodeIds.size());
        Assert.assertEquals("rgn1", nodeIds.iterator().next());
    }
    
    @Test
    public void testRouteS1ToDWFromRegsvr() {
        IDataRouter router = buildTestableRouter(
                MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT.findNetworkedNode("regsvr").getNode(), MULTIPLE_GROUPS_PLUS_REG_SVR_LINKS,
                MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT);
        Set<Node> nodes = new HashSet<Node>();
        nodes.add(MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT.findNetworkedNode("s1").getNode());
        nodes.add(MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT.findNetworkedNode("s2").getNode());
        nodes.add(MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT.findNetworkedNode("dw").getNode());
        Collection<String> nodeIds = router.routeToNodes(new SimpleRouterContext(), buildDataMetaData("SYM_NODE", "s1"), nodes, false);
        Assert.assertNotNull(nodeIds);
        Assert.assertEquals(2, nodeIds.size());
        Assert.assertTrue(nodeIds.contains("s1"));
        Assert.assertTrue(nodeIds.contains("dw"));
    }
    
    @Test
    public void testRouteDWToS1andS2FromRegsvr() {
        IDataRouter router = buildTestableRouter(
                MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT.findNetworkedNode("regsvr").getNode(), MULTIPLE_GROUPS_PLUS_REG_SVR_LINKS,
                MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT);
        Set<Node> nodes = new HashSet<Node>();
        nodes.add(MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT.findNetworkedNode("s1").getNode());
        nodes.add(MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT.findNetworkedNode("s2").getNode());
        nodes.add(MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT.findNetworkedNode("dw").getNode());
        Collection<String> nodeIds = router.routeToNodes(new SimpleRouterContext(), buildDataMetaData("SYM_NODE", "dw"), nodes, false);
        Assert.assertNotNull(nodeIds);
        Assert.assertEquals(3, nodeIds.size());
        Assert.assertTrue(nodeIds.contains("s1"));
        Assert.assertTrue(nodeIds.contains("s2"));
        Assert.assertTrue(nodeIds.contains("dw"));
    }
    
    @Test
    public void testRouteS1toRegsvrFromS1() {
        IDataRouter router = buildTestableRouter(
                MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT.findNetworkedNode("s1").getNode(), MULTIPLE_GROUPS_PLUS_REG_SVR_LINKS,
                MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT);
        Set<Node> nodes = new HashSet<Node>();
        nodes.add(MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT.findNetworkedNode("s1").getNode());
        nodes.add(MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT.findNetworkedNode("dw").getNode());
        nodes.add(MULTIPLE_GROUPS_PLUS_REG_SVR_NETWORKED_ROOT.findNetworkedNode("regsvr").getNode());
        Collection<String> nodeIds = router.routeToNodes(new SimpleRouterContext(), buildDataMetaData("SYM_NODE", "s1"), nodes, false);
        Assert.assertNotNull(nodeIds);
        Assert.assertEquals(1, nodeIds.size());
        Assert.assertTrue(nodeIds.contains("regsvr"));
    }
    
    protected DataMetaData buildDataMetaData(String tableName, String nodeId) {
        Data data = new Data();
        data.setTableName(tableName);
        data.setDataEventType(DataEventType.UPDATE);
        data.setTriggerHistory(new TriggerHistory(tableName, "NODE_ID", "NODE_ID"));
        data.setPkData(nodeId);
        data.setRowData(nodeId);
        return new DataMetaData(data, new Table(tableName), null, null);
    }

    protected IDataRouter buildTestableRouter(final Node nodeThatIsRouting,
            final List<NodeGroupLink> links, final NetworkedNode root) {
        ConfigurationChangedDataRouter router = new ConfigurationChangedDataRouter() {
            @Override
            protected Node findIdentity() {
                return nodeThatIsRouting;
            }

            @Override
            protected List<NodeGroupLink> getNodeGroupLinksFromContext(SimpleRouterContext routingContext) {
                return links;
            }

            @Override
            protected NetworkedNode getRootNetworkNodeFromContext(SimpleRouterContext routingContext) {
                return root;
            }
        };
        router.setTablePrefix("sym");
        return router;
    }
}
