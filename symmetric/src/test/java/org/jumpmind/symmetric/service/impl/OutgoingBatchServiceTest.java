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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;

import junit.framework.Assert;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.TriggerHistory;
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

    private int triggerHistId;

    public OutgoingBatchServiceTest() throws Exception {
        super();
    }

    public OutgoingBatchServiceTest(String dbName) {
        super(dbName);
    }

    @Before
    public void setUp() {
        batchService = (IOutgoingBatchService) find(Constants.OUTGOING_BATCH_SERVICE);
        dataService = (IDataService) find(Constants.DATA_SERVICE);
        Set<Long> histKeys = getTriggerRouterService().getHistoryRecords().keySet();
        assertFalse(histKeys.isEmpty());
        triggerHistId = histKeys.iterator().next().intValue();
    }

    @Test
    public void testDisabledChannel() {
        NodeChannel nodeChannel = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID);
        nodeChannel.setEnabled(false);
        getConfigurationService().saveChannel(nodeChannel.getChannel(), true);

        cleanSlate("sym_data_event", "sym_data", "sym_outgoing_batch");
        int size = 50; // magic number
        int count = 3; // must be <= size
        assertTrue(count <= size);

        for (int i = 0; i < size * count; i++) {
            createDataEvent("Foo", triggerHistId, TestConstants.TEST_CHANNEL_ID, DataEventType.INSERT,
                    TestConstants.TEST_CLIENT_EXTERNAL_ID);
        }

        OutgoingBatches list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertNotNull(list);
        assertNotNull(list.getBatches());
        assertEquals(list.getBatches().size(), 0);

        nodeChannel.setEnabled(true);
        getConfigurationService().saveChannel(nodeChannel, true);
    }

    // Tests to make sure the cache, even when cached, goes ahead and pulls the
    // last_extracted_time from the database.

    @Test
    public void testChannelCachingLastExtracted() {
        final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        NodeChannel nodeChannel = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Calendar currentTime = Calendar.getInstance();
        Calendar hourAgo = (Calendar) currentTime.clone();
        hourAgo.add(Calendar.HOUR_OF_DAY, -1);
        Calendar halfHourAgo = (Calendar) currentTime.clone();
        halfHourAgo.add(Calendar.MINUTE, -30);
        nodeChannel.setLastExtractedTime(hourAgo.getTime());

        getConfigurationService().saveNodeChannel(nodeChannel, true);
        nodeChannel = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertEquals(formatter.format(hourAgo.getTime()), formatter.format(nodeChannel.getLastExtractedTime()));

        int updateCount = updateNodeChannelLastExtractTimeManually(halfHourAgo.getTime(), nodeChannel.getNodeId(),
                TestConstants.TEST_CHANNEL_ID);

        Assert.assertEquals(1, updateCount);

        nodeChannel = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);

        Assert.assertEquals(formatter.format(halfHourAgo.getTime()), formatter.format(nodeChannel
                .getLastExtractedTime()));

        getConfigurationService().reloadChannels();
        nodeChannel = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertEquals(formatter.format(halfHourAgo.getTime()), formatter.format(nodeChannel
                .getLastExtractedTime()));

    }

    @Test
    public void testChannelRemovalOfBatchesNotTimeYet() {
        final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        long channelOtherExtractPeriod = 60 * 60 * 1000;
        long channelTestExtractPeriod = 30 * 60 * 1000;

        Calendar currentTime = Calendar.getInstance();
        Calendar hourAgo = (Calendar) currentTime.clone();
        hourAgo.add(Calendar.HOUR_OF_DAY, -1);
        Calendar halfHourAgo = (Calendar) currentTime.clone();
        halfHourAgo.add(Calendar.MINUTE, -30);

        cleanSlate("sym_data_event", "sym_data", "sym_outgoing_batch");
        
        NodeChannel nodeChannelTest = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        nodeChannelTest.setExtractPeriodMillis(channelTestExtractPeriod);
        getConfigurationService().saveChannel(nodeChannelTest, false);

        NodeChannel nodeChannelOther = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID_OTHER,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        nodeChannelOther.setExtractPeriodMillis(channelOtherExtractPeriod);
        getConfigurationService().saveChannel(nodeChannelOther, true);

        nodeChannelTest = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);

        nodeChannelOther = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID_OTHER,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);

        Assert.assertEquals(channelTestExtractPeriod, nodeChannelTest.getExtractPeriodMillis());
        Assert.assertEquals(channelOtherExtractPeriod, nodeChannelOther.getExtractPeriodMillis());

        // test channel is every 30 minutes, "other" is every 60.
        // go through series of setting the last extract on both channels to
        // validate the correct amounts come back each time. 5 data events
        // each....

        for (int i = 0; i < 5; i++) {
            createDataEvent("Foo", triggerHistId, TestConstants.TEST_CHANNEL_ID, DataEventType.INSERT,
                    TestConstants.TEST_CLIENT_EXTERNAL_ID);
        }

        for (int i = 0; i < 5; i++) {
            createDataEvent("Foo", triggerHistId, TestConstants.TEST_CHANNEL_ID_OTHER, DataEventType.INSERT,
                    TestConstants.TEST_CLIENT_EXTERNAL_ID);
        }

        OutgoingBatches list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);

    //    Assert.assertEquals(4, list.getActiveChannels().size(), 4);
     //   Assert.assertEquals(10, list.getBatches().size());

    }

    private int updateNodeChannelLastExtractTimeManually(final Date newTime, final String nodeId, final String channelId) {
        return (Integer) getJdbcTemplate().execute(new ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                PreparedStatement s = conn
                        .prepareStatement("update sym_node_channel_ctl set last_extract_time=? where node_id= ? and channel_id= ?");
                s.setTimestamp(1, new java.sql.Timestamp(newTime.getTime()));
                s.setString(2, nodeId);
                s.setString(3, channelId);
                int result = s.executeUpdate();

                return result;
            }
        });
    }

    protected void createDataEvent(String tableName, int triggerHistoryId, String channelId, DataEventType type,
            String nodeId) {
        TriggerHistory history = new TriggerHistory();
        history.setTriggerHistoryId(triggerHistoryId);
        Data data = new Data(tableName, type, "r.o.w., dat-a", "p-k d.a.t.a", history, channelId, null, null);
        dataService.insertDataEvent(data, nodeId, channelId);
    }

    protected int getBatchSize(final long batchId) {
        return (Integer) getJdbcTemplate().execute(new ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                PreparedStatement s = conn.prepareStatement("select count(*) from sym_data_event where batch_id = ?");
                s.setLong(1, batchId);
                ResultSet rs = s.executeQuery();
                rs.next();
                return rs.getInt(1);
            }
        });
    }

}
