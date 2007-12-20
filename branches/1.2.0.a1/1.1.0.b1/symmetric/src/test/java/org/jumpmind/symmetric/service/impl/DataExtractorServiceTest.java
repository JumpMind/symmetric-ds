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

import java.util.StringTokenizer;

import org.jumpmind.symmetric.AbstractTest;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.transport.mock.MockOutgoingTransport;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class DataExtractorServiceTest extends AbstractTest {

    protected IDataExtractorService dataExtractorService;
    
    protected IConfigurationService configurationService;
    
    protected Node node;
    
    @BeforeTest(groups="continuous")
    protected void setUp() {
        dataExtractorService = (IDataExtractorService) getBeanFactory().getBean(Constants.DATAEXTRACTOR_SERVICE);
        configurationService = (IConfigurationService) getBeanFactory().getBean(Constants.CONFIG_SERVICE);
        node = new Node();
        node.setNodeId(TestConstants.TEST_CLIENT_EXTERNAL_ID);
    }

    @Test(groups="continuous")
    public void testInitialLoadExtract() throws Exception {
        ((IBootstrapService)getBeanFactory().getBean(Constants.BOOTSTRAP_SERVICE)).syncTriggers();
        MockOutgoingTransport mockTransport = new MockOutgoingTransport();
        dataExtractorService.extractInitialLoadFor(node, configurationService.getTriggerFor(TestConstants.TEST_PREFIX + "node_group", TestConstants.TEST_CONTINUOUS_NODE_GROUP), mockTransport);
        String loadResults = mockTransport.toString();
        Assert.assertEquals(11, countLines(loadResults), "Unexpected number of lines in the csv result: " + loadResults);
        Assert.assertTrue(loadResults.contains("insert, \"CORP\",\"Central Office\""), "Did not find expected insert for CORP");
        Assert.assertTrue(loadResults.startsWith("nodeid, 00001"), "Unexpected line at the start of the feed.");
    }
    
    private int countLines(String results) {
        return new StringTokenizer(results, "\n").countTokens();
    }

    @Test(groups="continuous")
    public void testExtract() throws Exception {
        cleanSlate(TestConstants.TEST_PREFIX + "data_event", TestConstants.TEST_PREFIX + "data", TestConstants.TEST_PREFIX + "outgoing_batch");
        OutgoingBatchServiceTest obst = new OutgoingBatchServiceTest();
        obst.init();
        int dataId = obst.createData("Foo", TestConstants.TEST_AUDIT_ID, TestConstants.TEST_CHANNEL_ID, DataEventType.INSERT); 
        obst.createEvent(dataId, node.getNodeId());
        
        MockOutgoingTransport mockTransport = new MockOutgoingTransport();
        mockTransport.open();
        dataExtractorService.extract(node, mockTransport);
        String loadResults = mockTransport.toString();
        
        Assert.assertEquals(countLines(loadResults), 7, "Unexpected number of lines in the transport result: " + loadResults);
    }

}
