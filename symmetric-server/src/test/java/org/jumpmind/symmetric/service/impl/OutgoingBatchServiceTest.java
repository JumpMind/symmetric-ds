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
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
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

/**
 * 
 */
public class OutgoingBatchServiceTest extends AbstractDatabaseTest {

    private IOutgoingBatchService batchService;

    private IDataService dataService;

    private int triggerHistId;

    public OutgoingBatchServiceTest() throws Exception {
        super();
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
        NodeChannel nodeChannel = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID, false);
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

        OutgoingBatches list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_NODE, false);
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
        cleanSlate("sym_data_event", "sym_data", "sym_outgoing_batch", "sym_node_channel_ctl");

        final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        NodeChannel nodeChannel = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID, true);
        Calendar currentTime = Calendar.getInstance();
        Calendar hourAgo = (Calendar) currentTime.clone();
        hourAgo.add(Calendar.HOUR_OF_DAY, -1);
        Calendar halfHourAgo = (Calendar) currentTime.clone();
        halfHourAgo.add(Calendar.MINUTE, -30);
        nodeChannel.setLastExtractedTime(hourAgo.getTime());

        getConfigurationService().saveNodeChannel(nodeChannel, true);
        nodeChannel = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID, true);
        Assert.assertEquals(formatter.format(hourAgo.getTime()), formatter.format(nodeChannel.getLastExtractedTime()));

        int updateCount = updateNodeChannelLastExtractTimeManually(halfHourAgo.getTime(), nodeChannel.getNodeId(),
                TestConstants.TEST_CHANNEL_ID);

        Assert.assertEquals(1, updateCount);

        nodeChannel = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID, true);

        Assert.assertEquals(formatter.format(halfHourAgo.getTime()), formatter.format(nodeChannel
                .getLastExtractedTime()));

        getConfigurationService().reloadChannels();
        nodeChannel = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID, true);
        Assert.assertEquals(formatter.format(halfHourAgo.getTime()), formatter.format(nodeChannel
                .getLastExtractedTime()));

    }
    
    @Test
    public void testInsertOutgoingBatchMaxSize() {        
        OutgoingBatch batch = new OutgoingBatch();
        batch.setStatus(Status.NE);
        batch.setNodeId("XXXXX");
        batch.setChannelId(TestConstants.TEST_CHANNEL_ID);
        batch.setByteCount(Long.MAX_VALUE);
        batch.setDataEventCount(Long.MAX_VALUE);
        batch.setDeleteEventCount(Long.MAX_VALUE);
        batch.setFailedDataId(Long.MAX_VALUE);
        batch.setExtractCount(Long.MAX_VALUE);
        batch.setExtractMillis(Long.MAX_VALUE);
        batch.setFilterMillis(Long.MAX_VALUE);
        batch.setInsertEventCount(Long.MAX_VALUE);
        batch.setLoadCount(Long.MAX_VALUE);
        batch.setLoadMillis(Long.MAX_VALUE);
        batch.setNetworkMillis(Long.MAX_VALUE);
        batch.setOtherEventCount(Long.MAX_VALUE);
        batch.setReloadEventCount(Long.MAX_VALUE);
        batch.setRouterMillis(Long.MAX_VALUE);
        batch.setUpdateEventCount(Long.MAX_VALUE);
        batch.setSentCount(Long.MAX_VALUE);
        getOutgoingBatchService().insertOutgoingBatch(batch);
        OutgoingBatches batches = getOutgoingBatchService().getOutgoingBatches(new Node("XXXXX", TestConstants.TEST_ROOT_NODE_GROUP), false);
        Assert.assertEquals(1, batches.getBatches().size());
        batch.setBatchId(batches.getBatches().get(0).getBatchId());
        batch.setStatus(Status.OK);
        getOutgoingBatchService().updateOutgoingBatch(batch);
    }

    @Test
    public void testChannelRemovalOfBatchesNotTimeYet() {

        long channelOtherExtractPeriod = 59 * 60 * 1000;
        long channelTestExtractPeriod = 29 * 60 * 1000;

        Calendar startOfTestTime = Calendar.getInstance();
        Calendar hourAgo = (Calendar) startOfTestTime.clone();
        hourAgo.add(Calendar.HOUR_OF_DAY, -1);
        Calendar halfHourAgo = (Calendar) startOfTestTime.clone();
        halfHourAgo.add(Calendar.MINUTE, -30);

        cleanSlate("sym_data_event", "sym_data", "sym_outgoing_batch", "sym_node_channel_ctl");

        NodeChannel nodeChannelTest = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
        nodeChannelTest.setExtractPeriodMillis(channelTestExtractPeriod);

        getConfigurationService().saveChannel(nodeChannelTest, false);

        NodeChannel nodeChannelOther = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID_OTHER,
                TestConstants.TEST_CLIENT_EXTERNAL_ID, false);
        nodeChannelOther.setExtractPeriodMillis(channelOtherExtractPeriod);
        getConfigurationService().saveChannel(nodeChannelOther, false);

        nodeChannelTest = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID, false);

        nodeChannelOther = getConfigurationService().getNodeChannel(TestConstants.TEST_CHANNEL_ID_OTHER,
                TestConstants.TEST_CLIENT_EXTERNAL_ID, false);

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

        // reload, config, testchannel and other

        OutgoingBatches list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_NODE, false);

        Assert.assertEquals(4, list.getActiveChannels().size(), 4);
        Assert.assertEquals(10, list.getBatches().size());

        Assert.assertEquals(5, list.getBatchesForChannel(TestConstants.TEST_CHANNEL_ID).size());
        Assert.assertEquals(5, list.getBatchesForChannel(TestConstants.TEST_CHANNEL_ID_OTHER).size());

        nodeChannelTest.setLastExtractedTime(halfHourAgo.getTime());
        getConfigurationService().saveNodeChannel(nodeChannelTest, false);

        nodeChannelOther.setLastExtractedTime(halfHourAgo.getTime());
        getConfigurationService().saveNodeChannel(nodeChannelOther, false);

        list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_NODE, false);

        Assert.assertEquals(4, list.getActiveChannels().size(), 4);
        Assert.assertEquals(5, list.getBatches().size());

        for (OutgoingBatch batch : list.getBatches()) {
            Assert.assertEquals(TestConstants.TEST_CHANNEL_ID, batch.getChannelId());
        }

        nodeChannelOther.setLastExtractedTime(hourAgo.getTime());
        getConfigurationService().saveNodeChannel(nodeChannelOther, false);

        list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_NODE, false);

        Assert.assertEquals(4, list.getActiveChannels().size(), 4);
        Assert.assertEquals(10, list.getBatches().size());

        nodeChannelTest.setLastExtractedTime(halfHourAgo.getTime());
        getConfigurationService().saveNodeChannel(nodeChannelTest, false);

        nodeChannelOther.setLastExtractedTime(halfHourAgo.getTime());
        getConfigurationService().saveNodeChannel(nodeChannelOther, false);

        nodeChannelTest.setExtractPeriodMillis(0);
        getConfigurationService().saveChannel(nodeChannelTest, false);

        nodeChannelOther.setExtractPeriodMillis(0);
        getConfigurationService().saveChannel(nodeChannelOther, false);

        list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_NODE, false);

        // would be 5, but we reset the extract period to 0, meaning every time

        Assert.assertEquals(4, list.getActiveChannels().size(), 4);
        Assert.assertEquals(10, list.getBatches().size());
    }

    private int updateNodeChannelLastExtractTimeManually(final Date newTime, final String nodeId, final String channelId) {
        return (Integer) getJdbcTemplate().execute(new ConnectionCallback<Object>() {
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
        dataService.insertDataAndDataEventAndOutgoingBatch(data, nodeId, "", false);
    }        

    protected int getBatchSize(final long batchId) {
        return (Integer) getJdbcTemplate().execute(new ConnectionCallback<Object>() {
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