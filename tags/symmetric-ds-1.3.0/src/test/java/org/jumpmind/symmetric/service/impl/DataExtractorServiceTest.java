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

import java.util.Set;
import java.util.StringTokenizer;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.transport.mock.MockOutgoingTransport;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class DataExtractorServiceTest extends AbstractDatabaseTest {

    protected IDataExtractorService dataExtractorService;

    protected IConfigurationService configurationService;

    protected IDataService dataService;

    private int triggerHistId;

    protected Node node;

    @BeforeTest(groups = "continuous")
    protected void setUp() {
        dataExtractorService = (IDataExtractorService) getBeanFactory().getBean(
                Constants.DATAEXTRACTOR_SERVICE);
        configurationService = (IConfigurationService) getBeanFactory().getBean(Constants.CONFIG_SERVICE);
        dataService = (IDataService) getBeanFactory().getBean(Constants.DATA_SERVICE);
        node = new Node();
        node.setNodeId(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        node.setNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        Set<Long> histKeys = configurationService.getHistoryRecords().keySet();
        Assert.assertFalse(histKeys.isEmpty());
        triggerHistId = histKeys.iterator().next().intValue();
    }

    @Test(groups = "continuous")
    public void testInitialLoadExtract() throws Exception {
        ((IBootstrapService) getBeanFactory().getBean(Constants.BOOTSTRAP_SERVICE)).syncTriggers();
        MockOutgoingTransport mockTransport = new MockOutgoingTransport();
        dataExtractorService.extractInitialLoadFor(node, configurationService.getTriggerFor(
                TestConstants.TEST_PREFIX + "node_group", TestConstants.TEST_CONTINUOUS_NODE_GROUP),
                mockTransport);
        String loadResults = mockTransport.toString();
        Assert.assertEquals(9, countLines(loadResults), "Unexpected number of lines in the csv result: "
                + loadResults);
        Assert.assertTrue(loadResults.contains("insert, \"test-root-group\",\"a test config\""),
                "Did not find expected insert for CORP");
        Assert.assertTrue(loadResults.startsWith("nodeid, 00000"),
                "Unexpected line at the start of the feed.");
    }

    @Test(groups = "continuous")
    public void testExtract() throws Exception {
        cleanSlate(TestConstants.TEST_PREFIX + "data_event", TestConstants.TEST_PREFIX + "data",
                TestConstants.TEST_PREFIX + "outgoing_batch");
        createDataEvent("Foo", triggerHistId, TestConstants.TEST_CHANNEL_ID,
                DataEventType.INSERT, node.getNodeId());

        MockOutgoingTransport mockTransport = new MockOutgoingTransport();
        mockTransport.open();
        dataExtractorService.extract(node, mockTransport);
        String loadResults = mockTransport.toString();

        Assert.assertEquals(countLines(loadResults), 8,
                "Unexpected number of lines in the transport result: " + loadResults);
    }

    private int countLines(String results) {
        return new StringTokenizer(results, "\n").countTokens();
    }

    private void createDataEvent(String tableName, int auditId, String channelId, DataEventType type,
            String nodeId) {
        TriggerHistory audit = new TriggerHistory();
        audit.setTriggerHistoryId(auditId);
        Data data = new Data(tableName, type, "r.o.w., dat-a", "p-k d.a.t.a", audit);
        dataService.insertDataEvent(data, channelId, nodeId);
    }
}
