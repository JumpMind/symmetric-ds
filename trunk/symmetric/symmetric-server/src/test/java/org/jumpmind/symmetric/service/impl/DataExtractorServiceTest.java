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

import java.io.BufferedWriter;
import java.util.StringTokenizer;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.jumpmind.symmetric.transport.mock.MockOutgoingTransport;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 
 */
public class DataExtractorServiceTest extends AbstractDatabaseTest {

    protected DataExtractorService dataExtractorService;
    
    protected IOutgoingBatchService outgoingBatchService;

    protected IDataService dataService;

    private TriggerHistory triggerHistory;

    protected Node node;

    public DataExtractorServiceTest() throws Exception {
        super();
    }

    @Before
    public void setUp() {
        dataExtractorService = (DataExtractorService) find(Constants.DATAEXTRACTOR_SERVICE);
        outgoingBatchService = (IOutgoingBatchService)find(Constants.OUTGOING_BATCH_SERVICE);
        dataService = (IDataService) find(Constants.DATA_SERVICE);
        node = new Node();
        node.setNodeId(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        triggerHistory = getTriggerRouterService().getHistoryRecords().values().iterator().next();
    }

    @Test
    public void testInitialLoadExtract() throws Exception {
        getTriggerRouterService().syncTriggers();
        MockOutgoingTransport mockTransport = new MockOutgoingTransport();
        BufferedWriter writer = mockTransport.open();
        JdbcTemplate template = getJdbcTemplate();
        template.update("delete from " + TriggerRouterServiceTest.TEST_TRIGGERS_TABLE);
        TriggerRouter trigger = getTriggerRouterService().getTriggerRouterForTableForCurrentNode(null, null, TriggerRouterServiceTest.TEST_TRIGGERS_TABLE, false).iterator().next();
        OutgoingBatch batch = new OutgoingBatch(node.getNodeId(), trigger.getTrigger().getChannelId(), Status.NE);
        outgoingBatchService.insertOutgoingBatch(batch);
        DataExtractorContext ctx = new DataExtractorContext();
        ctx.setBatch(batch);
        
        dataExtractorService.writeInitialLoad(node, trigger, null, writer, ctx);
        String loadResults = mockTransport.toString();
        assertEquals(countLines(loadResults), 5, "Unexpected number of lines in the csv result: " + loadResults);
        assertTrue(loadResults.startsWith("nodeid, 00000"), "Unexpected line at the start of the feed.");
        
        TriggerRouterServiceTest.insert(TriggerRouterServiceTest.INSERT1_VALUES, template, getDbDialect());
        TriggerRouterServiceTest.insert(TriggerRouterServiceTest.INSERT2_VALUES, template, getDbDialect());
        
        batch = new OutgoingBatch(node.getNodeId(), trigger.getTrigger().getChannelId(), Status.NE);
        outgoingBatchService.insertOutgoingBatch(batch);
        ctx = new DataExtractorContext();
        ctx.setBatch(batch);
        dataExtractorService.writeInitialLoad(node, trigger, null, writer, ctx);
        loadResults = mockTransport.toString();
        assertEquals(countLines(loadResults), 17, "Unexpected number of lines in the csv result: " + loadResults);
        
        
    }

    @Test
    public void testExtract() throws Exception {
        cleanSlate("sym_data_event", "sym_data",
                "sym_outgoing_batch");
        createDataEvent(triggerHistory, TestConstants.TEST_CHANNEL_ID, DataEventType.INSERT, node.getNodeId());

        MockOutgoingTransport mockTransport = new MockOutgoingTransport();
        mockTransport.open();
        dataExtractorService.extract(node, mockTransport);
        String loadResults = mockTransport.toString();

        assertEquals(countLines(loadResults), 11, "Unexpected number of lines in the transport result: "
                + loadResults);
    }

    private int countLines(String results) {
        return new StringTokenizer(results, "\n").countTokens();
    }

    private void createDataEvent(TriggerHistory hist, String channelId, DataEventType type, String nodeId) {
        Data data = new Data(hist.getSourceTableName(), type, "r.o.w., dat-a", "p-k d.a.t.a", hist, TestConstants.TEST_CHANNEL_ID, null, null);
        dataService.insertDataAndDataEventAndOutgoingBatch(data, nodeId, Constants.UNKNOWN_ROUTER_ID, false);
    }
}