/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *               Eric Long <erilong@users.sourceforge.net>
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.BatchType;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;

public class OutgoingBatchServiceTest extends AbstractDatabaseTest {

    private IOutgoingBatchService batchService;

    private IDataService dataService;

    private IAcknowledgeService ackService;

    private IConfigurationService configService;

    private int triggerHistId;

    public OutgoingBatchServiceTest() throws Exception {
        super();
    }

    public OutgoingBatchServiceTest(String dbName) {
        super(dbName);
    }

    @Before
    public void setUp() {
        ackService = (IAcknowledgeService) find(Constants.ACKNOWLEDGE_SERVICE);
        batchService = (IOutgoingBatchService) find(Constants.OUTGOING_BATCH_SERVICE);
        dataService = (IDataService) find(Constants.DATA_SERVICE);
        configService = (IConfigurationService) find(Constants.CONFIG_SERVICE);
        Set<Long> histKeys = configService.getHistoryRecords().keySet();
        assertFalse(histKeys.isEmpty());
        triggerHistId = histKeys.iterator().next().intValue();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void test() {
        List<NodeChannel> channels = configService.getChannels();
        cleanSlate(TestConstants.TEST_PREFIX + "data_event", TestConstants.TEST_PREFIX + "data",
                TestConstants.TEST_PREFIX + "outgoing_batch");
        // create a batch
        createDataEvent("Foo", triggerHistId, TestConstants.TEST_CHANNEL_ID, DataEventType.INSERT,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        batchService.buildOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID, channels);
        List<OutgoingBatch> list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertTrue(list != null);
        assertEquals(list.size(), 1);
        assertTrue(list.get(0).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));

        // create another batch
        createDataEvent("Foo", triggerHistId, TestConstants.TEST_CHANNEL_ID, DataEventType.INSERT,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        createDataEvent("Foo", triggerHistId, TestConstants.TEST_CHANNEL_ID, DataEventType.INSERT,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        batchService.buildOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID, channels);
        list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertTrue(list != null);
        assertTrue(list.size() == 2);
        assertTrue(list.get(0).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));
        assertTrue(list.get(1).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));

        // mark the first batch as sent (should still be eligible to be resent)
        batchService.markOutgoingBatchSent(list.get(0));
        list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertTrue(list.size() == 2);
        assertTrue(list.get(0).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));
        assertTrue(list.get(1).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));

        // set the second batch status to ok
        batchService.setBatchStatus(list.get(0).getBatchId(), Status.OK);
        list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertTrue(list.size() == 1);
        assertTrue(list.get(0).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));

        // test for initial load (batch type == IL)
        OutgoingBatch ilBatch = new OutgoingBatch();
        ilBatch.setNodeId(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        ilBatch.setChannelId(TestConstants.TEST_CHANNEL_ID);
        ilBatch.setBatchType(BatchType.INITIAL_LOAD);
        batchService.insertOutgoingBatch(ilBatch);
        list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertTrue(list.size() == 2);
        assertTrue(list.get(0).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));
        assertTrue(list.get(1).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));

        // now mark the IL batch as complete
        batchService.setBatchStatus(ilBatch.getBatchId(), Status.OK);
        list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertTrue(list.size() == 1);
        assertTrue(list.get(0).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testBatchBoundary() {
        List<NodeChannel> channels = configService.getChannels();

        cleanSlate(TestConstants.TEST_PREFIX + "data_event", TestConstants.TEST_PREFIX + "data",
                TestConstants.TEST_PREFIX + "outgoing_batch");
        int size = 50;
        int count = 3; // must be <= size
        assertTrue(count <= size);

        for (int i = 0; i < size * count; i++) {
            createDataEvent("Foo", triggerHistId, TestConstants.TEST_CHANNEL_ID, DataEventType.INSERT,
                    TestConstants.TEST_CLIENT_EXTERNAL_ID);
        }

        for (int i = 0; i < count; i++) {
            batchService.buildOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID, channels);
        }

        List<OutgoingBatch> list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertNotNull(list);
        assertEquals(list.size(), count);

        for (int i = 0; i < count; i++) {
            assertTrue(getBatchSize(list.get(i).getBatchId()) <= size + 1);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testMultipleChannels() {
        List<NodeChannel> channels = configService.getChannels();

        cleanSlate(TestConstants.TEST_PREFIX + "data_event", TestConstants.TEST_PREFIX + "data",
                TestConstants.TEST_PREFIX + "outgoing_batch");
        createDataEvent("Foo", triggerHistId, "testchannel", DataEventType.INSERT,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        createDataEvent("Foo", triggerHistId, "config", DataEventType.INSERT, TestConstants.TEST_CLIENT_EXTERNAL_ID);

        batchService.buildOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID, channels);

        List<OutgoingBatch> list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertNotNull(list);
        assertEquals(list.size(), 2);
    }

    @Test
    public void testDisabledChannel() {
        cleanSlate(TestConstants.TEST_PREFIX + "data_event", TestConstants.TEST_PREFIX + "data",
                TestConstants.TEST_PREFIX + "outgoing_batch");
        int size = 50; // magic number
        int count = 3; // must be <= size
        assertTrue(count <= size);

        for (int i = 0; i < size * count; i++) {
            createDataEvent("Foo", triggerHistId, TestConstants.TEST_CHANNEL_ID, DataEventType.INSERT,
                    TestConstants.TEST_CLIENT_EXTERNAL_ID);
        }

        List<OutgoingBatch> list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertNotNull(list);
        assertEquals(list.size(), 0);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testErrorChannel() {
        IConfigurationService configService = (IConfigurationService) find(Constants.CONFIG_SERVICE);
        List<NodeChannel> channels = configService.getChannels();

        cleanSlate(TestConstants.TEST_PREFIX + "data_event", TestConstants.TEST_PREFIX + "data",
                TestConstants.TEST_PREFIX + "outgoing_batch");
        // Create data events for two different channels
        createDataEvent("TestTable1", triggerHistId, "testchannel", DataEventType.INSERT,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);

        // Build the batch, make sure this event gets its own batch
        batchService.buildOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID, channels);

        createDataEvent("TestTable1", triggerHistId, "testchannel", DataEventType.INSERT,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        createDataEvent("TestTable2", triggerHistId, "config", DataEventType.INSERT,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);

        // Build the batches, which should be one for each channel
        batchService.buildOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID, channels);

        // Make sure we got three batches
        List<OutgoingBatch> batches = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertNotNull(batches);
        assertEquals(batches.size(), 3);
        long firstBatchId = batches.get(0).getBatchId();
        long secondBatchId = batches.get(1).getBatchId();
        long thirdBatchId = batches.get(2).getBatchId();

        // Ack the first batch as an error, leaving the others as new
        ackService.ack(new BatchInfo(firstBatchId, 1));

        // Get the batches again. The error channel batches should be last
        batches = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertNotNull(batches);
        assertEquals(batches.size(), 3);
        assertEquals(batches.get(0).getBatchId(), secondBatchId,
                "Channel in error should have batches last - missing new batch");
        assertEquals(batches.get(1).getBatchId(), thirdBatchId,
                "Channel in error should have batches last - missing error batch");
        assertEquals(batches.get(2).getBatchId(), firstBatchId,
                "Channel in error should have batches last - missing new batch");

    }

    protected void createDataEvent(String tableName, int auditId, String channelId, DataEventType type, String nodeId) {
        TriggerHistory audit = new TriggerHistory();
        audit.setTriggerHistoryId(auditId);
        Data data = new Data(tableName, type, "r.o.w., dat-a", "p-k d.a.t.a", audit);
        dataService.insertDataEvent(data, channelId, nodeId);
    }

    protected int getBatchSize(final long batchId) {
        return (Integer) getJdbcTemplate().execute(new ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                PreparedStatement s = conn.prepareStatement("select count(*) " + "from " + TestConstants.TEST_PREFIX
                        + "data_event where batch_id = ?");
                s.setLong(1, batchId);
                ResultSet rs = s.executeQuery();
                rs.next();
                return rs.getInt(1);
            }
        });
    }

}
