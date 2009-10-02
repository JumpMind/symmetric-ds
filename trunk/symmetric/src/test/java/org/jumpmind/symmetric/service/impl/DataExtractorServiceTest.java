/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
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

import java.io.BufferedWriter;
import java.util.StringTokenizer;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.jumpmind.symmetric.transport.mock.MockOutgoingTransport;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class DataExtractorServiceTest extends AbstractDatabaseTest {

    protected IDataExtractorService dataExtractorService;

    protected IDataService dataService;

    private TriggerHistory triggerHistory;

    protected Node node;

    public DataExtractorServiceTest() throws Exception {
        super();
    }

    public DataExtractorServiceTest(String dbName) {
        super(dbName);
    }

    @Before
    public void setUp() {
        dataExtractorService = (IDataExtractorService) find(Constants.DATAEXTRACTOR_SERVICE);
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
        TriggerRouter trigger = getTriggerRouterService().findTriggerRouter(TriggerRouterServiceTest.TEST_TRIGGERS_TABLE, TestConstants.TEST_CONTINUOUS_NODE_GROUP);
        dataExtractorService.extractInitialLoadFor(node, trigger, writer);
        String loadResults = mockTransport.toString();
        assertEquals(countLines(loadResults), 5, "Unexpected number of lines in the csv result: " + loadResults);
        assertTrue(loadResults.startsWith("nodeid, 00000"), "Unexpected line at the start of the feed.");
        
        TriggerRouterServiceTest.insert(TriggerRouterServiceTest.INSERT1_VALUES, template, getDbDialect());
        TriggerRouterServiceTest.insert(TriggerRouterServiceTest.INSERT2_VALUES, template, getDbDialect());
        
        dataExtractorService.extractInitialLoadFor(node, trigger, writer);
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
        dataService.insertDataAndDataEvent(data, nodeId, Constants.UNKNOWN_ROUTER_ID);
    }
}
