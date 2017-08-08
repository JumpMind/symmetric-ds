package org.jumpmind.symmetric.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.TestConstants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.junit.Test;

public class BatchStatisticsTest extends AbstractIntegrationTest {

    static {
        System.setProperty("h2.baseDir", "./");
    }

    @Test(timeout = 240000)
    public void testCreateServer() {
        ISymmetricEngine server = getServer();
        assertNotNull(server);
        server.getParameterService().saveParameter(ParameterConstants.FILE_SYNC_ENABLE, false, "unit_test");
        checkForFailedTriggers(true, false);

    }

    @Test(timeout = 240000)
    public void testRegisterClientWithRoot() {
        logTestRunning();
        ISymmetricEngine rootEngine = getServer();
        INodeService rootNodeService = rootEngine.getNodeService();
        rootEngine.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertTrue("The registration for the client should be opened now",
                rootNodeService.findNodeSecurity(TestConstants.TEST_CLIENT_EXTERNAL_ID).isRegistrationEnabled());
        getClient().start();
        getClient().getParameterService().saveParameter(ParameterConstants.FILE_SYNC_ENABLE, false, "unit_test");
        clientPull();
        assertTrue("The client did not register", getClient().isRegistered());
        assertFalse("The registration for the client should be closed now",
                rootNodeService.findNodeSecurity(TestConstants.TEST_CLIENT_EXTERNAL_ID).isRegistrationEnabled());
        IStatisticManager statMgr = getClient().getStatisticManager();
        statMgr.flush();

        checkForFailedTriggers(true, true);
    }

    @Test(timeout = 120000)
    public void testSyncStatistics() throws Exception {
        logTestRunning();

        String channelId = "testchannel";

        ISymmetricEngine client = getClient();
        ISymmetricEngine server = getServer();

        Date date = DateUtils.parseDate("2007-01-03", new String[] { "yyyy-MM-dd" });
        Order order = new Order("101", 100, null, date);
        order.getOrderDetails().add(new OrderDetail("101", 1, "STK", "110000065", 3, new BigDecimal("3.33")));

        assertNull(serverTestService.getOrder(order.getOrderId()));

        clientTestService.insertOrder(order);

        List<OutgoingBatch> outgoingBatches = client.getOutgoingBatchService().getOutgoingBatches(server.getNodeId(), channelId, true)
                .getBatches();
        System.out.println("Ougoing Batches Size = " + outgoingBatches.size());

        boolean pushedData = clientPush();

        assertTrue("Client data was not batched and pushed", pushedData);

        assertNotNull(serverTestService.getOrder(order.getOrderId()));

        IIncomingBatchService serverIncomingBatchService = server.getIncomingBatchService();
        for (OutgoingBatch outgoingBatch : outgoingBatches) {
            IncomingBatch incomingBatch = serverIncomingBatchService.findIncomingBatch(outgoingBatch.getBatchId(), client.getNodeId());
            assertOutgoingStatisticsEqual(outgoingBatch, incomingBatch);
        }

        List<OutgoingBatch> ackedOutgoingBatches = getClient().getOutgoingBatchService()
                .getOutgoingBatches(server.getNodeId(), channelId, true).getBatches();
        System.out.println("Ougoing Batches Size = " + ackedOutgoingBatches.size());
        
        for (OutgoingBatch ackedOutgoingBatch : ackedOutgoingBatches) {
            IncomingBatch incomingBatch = serverIncomingBatchService.findIncomingBatch(ackedOutgoingBatch.getBatchId(), client.getNodeId());
            assertIncomingStatisticsEqual(ackedOutgoingBatch, incomingBatch);
        }

    }

    private void assertOutgoingStatisticsEqual(OutgoingBatch outgoingBatch, IncomingBatch incomingBatch) {
        System.out.println("Is Load Flag = " + outgoingBatch.isLoadFlag());
        assertEquals(outgoingBatch.isLoadFlag(), incomingBatch.isLoadFlag());
        assertEquals(outgoingBatch.getExtractCount(), incomingBatch.getExtractCount());
        assertEquals(outgoingBatch.getSentCount(), incomingBatch.getSentCount());
        assertEquals(outgoingBatch.getLoadCount(), incomingBatch.getLoadCount());
        assertEquals(outgoingBatch.getLoadId(), incomingBatch.getLoadId());
        assertEquals(outgoingBatch.isCommonFlag(), incomingBatch.isCommonFlag());
        assertEquals(outgoingBatch.getRouterMillis(), incomingBatch.getRouterMillis());
        assertEquals(outgoingBatch.getExtractMillis(), incomingBatch.getExtractMillis());
        assertEquals(outgoingBatch.getTransformExtractMillis(), incomingBatch.getTransformExtractMillis());
        assertEquals(outgoingBatch.getTransformLoadMillis(), incomingBatch.getTransformLoadMillis());
        assertEquals(outgoingBatch.getReloadRowCount(), incomingBatch.getReloadRowCount());
        assertEquals(outgoingBatch.getOtherRowCount(), incomingBatch.getOtherRowCount());
        assertEquals(outgoingBatch.getDataRowCount(), incomingBatch.getDataRowCount());
        assertEquals(outgoingBatch.getDataInsertRowCount(), incomingBatch.getDataInsertRowCount());
        assertEquals(outgoingBatch.getDataUpdateRowCount(), incomingBatch.getDataUpdateRowCount());
        assertEquals(outgoingBatch.getDataDeleteRowCount(), incomingBatch.getDataDeleteRowCount());
        assertEquals(outgoingBatch.getExtractRowCount(), incomingBatch.getExtractRowCount());
        assertEquals(outgoingBatch.getExtractInsertRowCount(), incomingBatch.getExtractInsertRowCount());
        assertEquals(outgoingBatch.getExtractUpdateRowCount(), incomingBatch.getExtractUpdateRowCount());
        assertEquals(outgoingBatch.getExtractDeleteRowCount(), incomingBatch.getExtractDeleteRowCount());
        assertEquals(outgoingBatch.getFailedDataId(), incomingBatch.getFailedDataId());
    }

    private void assertIncomingStatisticsEqual(OutgoingBatch outgoingBatch, IncomingBatch incomingBatch) {
        System.out.println("Load Row Count = " + outgoingBatch.getLoadRowCount());
        assertEquals(outgoingBatch.getLoadRowCount(), incomingBatch.getLoadRowCount());
        assertEquals(outgoingBatch.getLoadInsertRowCount(), incomingBatch.getLoadInsertRowCount());
        assertEquals(outgoingBatch.getLoadUpdateRowCount(), incomingBatch.getLoadUpdateRowCount());
        assertEquals(outgoingBatch.getLoadDeleteRowCount(), incomingBatch.getLoadDeleteRowCount());
        assertEquals(outgoingBatch.getFallbackInsertCount(), incomingBatch.getFallbackInsertCount());
        assertEquals(outgoingBatch.getFallbackUpdateCount(), incomingBatch.getFallbackUpdateCount());
        assertEquals(outgoingBatch.getIgnoreRowCount(), incomingBatch.getIgnoreRowCount());
        assertEquals(outgoingBatch.getMissingDeleteCount(), incomingBatch.getMissingDeleteCount());
        assertEquals(outgoingBatch.getSkipCount(), incomingBatch.getSkipCount());
    }

}
