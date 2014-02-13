/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.route;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.route.ColumnMatchDataRouter.Expression;
import org.junit.Test;

public class ColumnMatchDataRouterTest {

    @Test
    public void testExpressionUsingLineFeedsParsing() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        List<Expression> expressions = router.parse("one=two\ntwo=three\rthree!=:EXTERNAL_ID");
        assertEquals(3, expressions.size());
        assertEquals("two",expressions.get(0).tokens[1]);
        assertEquals("three",expressions.get(2).tokens[0]);
        assertEquals(false,expressions.get(2).hasEquals);
    }
    
    @Test
    public void testExpressionOrParsing() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        List<Expression> expressions = router.parse("one=door OR two=three or three!=:EXTERNAL_ID");
        assertEquals(3, expressions.size());
        assertEquals("door",expressions.get(0).tokens[1]);
        assertEquals("three",expressions.get(2).tokens[0]);
        assertEquals(false,expressions.get(2).hasEquals);
    }
    
    @Test
    public void testExpressionTickParsing() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        List<Expression> expressions = router.parse("one='two three' OR four='five'\r\nor six=isn't \r\n seven='can''t'" +
                                                    " or eight='yall \n nine=' ten  ' or eleven  =  'twelve'  ");
        assertEquals(7, expressions.size());

        assertEquals("one",expressions.get(0).tokens[0]);
        assertEquals("two three",expressions.get(0).tokens[1]);

        assertEquals("four",expressions.get(1).tokens[0]);
        assertEquals("five",expressions.get(1).tokens[1]);

        assertEquals("six",expressions.get(2).tokens[0]);
        assertEquals("isn't",expressions.get(2).tokens[1]);

        assertEquals("seven",expressions.get(3).tokens[0]);
        assertEquals("can't",expressions.get(3).tokens[1]);

        assertEquals("eight",expressions.get(4).tokens[0]);
        assertEquals("'yall",expressions.get(4).tokens[1]);

        assertEquals("nine",expressions.get(5).tokens[0]);
        assertEquals(" ten  ",expressions.get(5).tokens[1]);

        assertEquals("eleven",expressions.get(6).tokens[0]);
        assertEquals("twelve",expressions.get(6).tokens[1]);


    }
    
    @Test
    public void testExpressionOrAndLineFeedsParsing() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        List<Expression> expressions = router.parse("one=two OR three=four\r\nor   five!=:EXTERNAL_ID");
        assertEquals(3, expressions.size());
        assertEquals("two",expressions.get(0).tokens[1]);
        assertEquals("three",expressions.get(1).tokens[0]);
        assertEquals("five",expressions.get(2).tokens[0]);
        assertEquals(false,expressions.get(2).hasEquals);
    }
    
    @Test
    public void testExpressionWithOrInColumnName() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        List<Expression> expressions = router.parse("ORDER_ID=:EXTERNAL_ID");
        assertEquals(1, expressions.size());
        assertEquals("ORDER_ID",expressions.get(0).tokens[0]);
        assertEquals(":EXTERNAL_ID",expressions.get(0).tokens[1]);
    }

    @Test
    public void testExpressionEqualsNodeId() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        SimpleRouterContext routingContext = new SimpleRouterContext();
        HashSet<Node> nodes = new HashSet<Node>();
        nodes.add(new Node("100", "client"));
        nodes.add(new Node("200", "client"));
        nodes.add(new Node("300", "client"));

        TriggerHistory triggerHist = new TriggerHistory("mytable","ID","ID,NODE_ID,COLUMN2");
        Data data = new Data();
        data.setDataId(1);
        data.setDataEventType(DataEventType.INSERT);
        data.setRowData("1,100,Super Dooper");
        data.setTriggerHistory(triggerHist);
        Table table = new Table();
        NodeChannel nodeChannel = new NodeChannel();
        Router route = new Router();
        route.setRouterExpression("NODE_ID = :NODE_ID");
        route.setRouterId("route1");
        DataMetaData dataMetaData = new DataMetaData(data, table, route, nodeChannel);
        
        Set<String> result = router.routeToNodes(routingContext, dataMetaData, nodes, false, false, null);
        assertEquals(1, result.size());
        assertEquals(true, result.contains("100"));
    }

    @Test
    public void testExpressionNotEqualsNodeId() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        SimpleRouterContext routingContext = new SimpleRouterContext();
        HashSet<Node> nodes = new HashSet<Node>();
        nodes.add(new Node("100", "client"));
        nodes.add(new Node("200", "client"));
        nodes.add(new Node("300", "client"));

        TriggerHistory triggerHist = new TriggerHistory("mytable","ID","ID,NODE_ID,COLUMN2");
        Data data = new Data();
        data.setDataId(1);
        data.setDataEventType(DataEventType.INSERT);
        data.setRowData("1,100,Super Dooper");
        data.setTriggerHistory(triggerHist);
        Table table = new Table();
        NodeChannel nodeChannel = new NodeChannel();
        Router route = new Router();
        route.setRouterExpression("NODE_ID != :NODE_ID");
        route.setRouterId("route1");
        DataMetaData dataMetaData = new DataMetaData(data, table, route, nodeChannel);
        
        Set<String> result = router.routeToNodes(routingContext, dataMetaData, nodes, false, false, null);
        assertEquals(2, result.size());
        assertEquals(true, result.contains("200"));
        assertEquals(true, result.contains("300"));
    }

    @Test
    public void testExpressionEqualsExternalId() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        SimpleRouterContext routingContext = new SimpleRouterContext();
        HashSet<Node> nodes = new HashSet<Node>();
        nodes.add(new Node("1000", "client"));
        nodes.add(new Node("100", "client"));
        nodes.add(new Node("10", "client"));

        TriggerHistory triggerHist = new TriggerHistory("mytable","ID","ID,STORE_ID,COLUMN2");
        Data data = new Data();
        data.setDataId(1);
        data.setDataEventType(DataEventType.INSERT);
        data.setRowData("1,100,Super Dooper");
        data.setTriggerHistory(triggerHist);
        Table table = new Table();
        NodeChannel nodeChannel = new NodeChannel();
        Router route = new Router();
        route.setRouterExpression("STORE_ID = :EXTERNAL_ID");
        route.setRouterId("route1");
        DataMetaData dataMetaData = new DataMetaData(data, table, route, nodeChannel);
        
        Set<String> result = router.routeToNodes(routingContext, dataMetaData, nodes, false, false, null);
        assertEquals(1, result.size());
        assertEquals(true, result.contains("100"));
    }

    @Test
    public void testExpressionEqualsGroupId() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        SimpleRouterContext routingContext = new SimpleRouterContext();
        HashSet<Node> nodes = new HashSet<Node>();
        nodes.add(new Node("10", "server"));
        nodes.add(new Node("20", "server"));
        nodes.add(new Node("3", "client"));
        nodes.add(new Node("2", "client"));
        nodes.add(new Node("1", "client"));

        TriggerHistory triggerHist = new TriggerHistory("mytable","ID","ID,GROUP_ID,COLUMN2");
        Data data = new Data();
        data.setDataId(1);
        data.setDataEventType(DataEventType.INSERT);
        data.setRowData("1,client,Super Dooper");
        data.setTriggerHistory(triggerHist);
        Table table = new Table();
        NodeChannel nodeChannel = new NodeChannel();
        Router route = new Router();
        route.setRouterExpression("GROUP_ID = :NODE_GROUP_ID");
        route.setRouterId("route1");
        DataMetaData dataMetaData = new DataMetaData(data, table, route, nodeChannel);
        
        Set<String> result = router.routeToNodes(routingContext, dataMetaData, nodes, false, false, null);
        assertEquals(3, result.size());
        assertEquals(true, result.contains("1"));
        assertEquals(true, result.contains("2"));
        assertEquals(true, result.contains("3"));
    }

    @Test
    public void testExpressionEqualsNull() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        SimpleRouterContext routingContext = new SimpleRouterContext();
        HashSet<Node> nodes = new HashSet<Node>();
        nodes.add(new Node("100", "client"));
        nodes.add(new Node("200", "client"));

        TriggerHistory triggerHist = new TriggerHistory("mytable","ID","ID,NODE_ID,COLUMN2");
        Data data = new Data();
        data.setDataId(1);
        data.setDataEventType(DataEventType.INSERT);
        data.setRowData("1,100,");
        data.setTriggerHistory(triggerHist);
        Table table = new Table();
        NodeChannel nodeChannel = new NodeChannel();
        Router route = new Router();
        route.setRouterExpression("COLUMN2 = NULL");
        route.setRouterId("route1");
        DataMetaData dataMetaData = new DataMetaData(data, table, route, nodeChannel);
        
        Set<String> result = router.routeToNodes(routingContext, dataMetaData, nodes, false, false, null);
        assertEquals(2, result.size());
        assertEquals(true, result.contains("100"));
        assertEquals(true, result.contains("200"));
    }

    @Test
    public void testExpressionNotEqualsNull() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        SimpleRouterContext routingContext = new SimpleRouterContext();
        HashSet<Node> nodes = new HashSet<Node>();
        nodes.add(new Node("100", "client"));
        nodes.add(new Node("200", "client"));

        TriggerHistory triggerHist = new TriggerHistory("mytable","ID","ID,NODE_ID,COLUMN2");
        Data data = new Data();
        data.setDataId(1);
        data.setDataEventType(DataEventType.INSERT);
        data.setRowData("1,100,");
        data.setTriggerHistory(triggerHist);
        Table table = new Table();
        NodeChannel nodeChannel = new NodeChannel();
        Router route = new Router();
        route.setRouterExpression("COLUMN2 != NULL");
        route.setRouterId("route1");
        DataMetaData dataMetaData = new DataMetaData(data, table, route, nodeChannel);
        
        Set<String> result = router.routeToNodes(routingContext, dataMetaData, nodes, false, false, null);
        assertEquals(0, result.size());
    }

    @Test
    public void testExpressionExternalData() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        SimpleRouterContext routingContext = new SimpleRouterContext();
        HashSet<Node> nodes = new HashSet<Node>();
        nodes.add(new Node("100", "client"));
        nodes.add(new Node("200", "client"));
        nodes.add(new Node("300", "client"));

        TriggerHistory triggerHist = new TriggerHistory("mytable","ID","ID,NODE_ID,COLUMN2");
        Data data = new Data();
        data.setDataId(1);
        data.setDataEventType(DataEventType.INSERT);
        data.setRowData("1,100,Super Dooper");
        data.setExternalData("100");
        data.setTriggerHistory(triggerHist);
        Table table = new Table();
        NodeChannel nodeChannel = new NodeChannel();
        Router route = new Router();
        route.setRouterExpression("EXTERNAL_DATA = :NODE_ID");
        route.setRouterId("route1");
        DataMetaData dataMetaData = new DataMetaData(data, table, route, nodeChannel);
        
        Set<String> result = router.routeToNodes(routingContext, dataMetaData, nodes, false, false, null);
        assertEquals(1, result.size());
        assertEquals(true, result.contains("100"));
    }

    @Test
    public void testExpressionContains() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        SimpleRouterContext routingContext = new SimpleRouterContext();
        HashSet<Node> nodes = new HashSet<Node>();
        nodes.add(new Node("100", "client"));
        nodes.add(new Node("200", "client"));
        nodes.add(new Node("300", "client"));
        nodes.add(new Node("1000", "client"));

        TriggerHistory triggerHist = new TriggerHistory("mytable","ID","ID,NODE_ID,COLUMN2");
        Data data = new Data();
        data.setDataId(1);
        data.setDataEventType(DataEventType.INSERT);
        data.setRowData("1,100,Super Dooper");
        data.setExternalData("1000,200");
        data.setTriggerHistory(triggerHist);
        Table table = new Table();
        NodeChannel nodeChannel = new NodeChannel();
        Router route = new Router();
        route.setRouterExpression("EXTERNAL_DATA contains :NODE_ID");
        route.setRouterId("route1");
        DataMetaData dataMetaData = new DataMetaData(data, table, route, nodeChannel);
        
        Set<String> result = router.routeToNodes(routingContext, dataMetaData, nodes, false, false, null);
        assertEquals(2, result.size());
        assertEquals(true, result.contains("1000"));
        assertEquals(true, result.contains("200"));        
    }

    @Test
    public void testExpressionNotContains() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        SimpleRouterContext routingContext = new SimpleRouterContext();
        HashSet<Node> nodes = new HashSet<Node>();
        nodes.add(new Node("100", "client"));
        nodes.add(new Node("200", "client"));
        nodes.add(new Node("300", "client"));
        nodes.add(new Node("1000", "client"));

        TriggerHistory triggerHist = new TriggerHistory("mytable","ID","ID,NODE_ID,COLUMN2");
        Data data = new Data();
        data.setDataId(1);
        data.setDataEventType(DataEventType.INSERT);
        data.setRowData("1,100,Super Dooper");
        data.setExternalData("1000,200");
        data.setTriggerHistory(triggerHist);
        Table table = new Table();
        NodeChannel nodeChannel = new NodeChannel();
        Router route = new Router();
        route.setRouterExpression("EXTERNAL_DATA not contains :NODE_ID");
        route.setRouterId("route1");
        DataMetaData dataMetaData = new DataMetaData(data, table, route, nodeChannel);
        
        Set<String> result = router.routeToNodes(routingContext, dataMetaData, nodes, false, false, null);
        assertEquals(2, result.size());
        assertEquals(true, result.contains("100"));
        assertEquals(true, result.contains("300"));
    }
}
