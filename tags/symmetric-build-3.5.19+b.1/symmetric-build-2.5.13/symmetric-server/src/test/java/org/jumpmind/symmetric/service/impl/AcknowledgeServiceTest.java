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

import java.util.Date;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.jumpmind.symmetric.transport.mock.MockAcknowledgeEventListener;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 */
public class AcknowledgeServiceTest extends AbstractDatabaseTest {

    protected IAcknowledgeService ackService;

    protected IOutgoingBatchService outgoingBatchService;

    protected IDataService dataService;

    public AcknowledgeServiceTest() throws Exception {
        super();
    }

    @Before
    public void setUp() {
        ackService = (IAcknowledgeService) find(Constants.ACKNOWLEDGE_SERVICE);
        outgoingBatchService = (IOutgoingBatchService) find(Constants.OUTGOING_BATCH_SERVICE);
        dataService = (IDataService) find(Constants.DATA_SERVICE);
    }

    @Test
    public void okTest() {        
        cleanSlate();

        OutgoingBatch batch = createOutgoingBatch();
        
        ackService.addAcknowledgeEventListener(new MockAcknowledgeEventListener());

        ackService.ack(batch.getBatchInfo());

        batch = outgoingBatchService.findOutgoingBatch(batch.getBatchId());
        Assert.assertEquals(batch.getStatus(), OutgoingBatch.Status.OK);
    }

    private void cleanSlate() {
        cleanSlate("sym_data_event", "sym_data",
                "sym_outgoing_batch");
    }

    @Test
    public void unspecifiedErrorTest() {
        cleanSlate();
        OutgoingBatch batch = createOutgoingBatch();
        createDataEvents(batch, 5);
        errorTestCore(batch.getBatchId(), -1, -1);
    }

    @Test
    public void errorTest() {
        cleanSlate();
        OutgoingBatch batch = createOutgoingBatch();
        long dataId[] = createDataEvents(batch, 5);
        errorTestCore(batch.getBatchId(), 3, dataId[2]);
    }

    @Test
    public void errorTestBoundary1() {
        cleanSlate();
        OutgoingBatch batch = createOutgoingBatch();
        long dataId[] = createDataEvents(batch, 5);
        errorTestCore(batch.getBatchId(), 1, dataId[0]);
    }

    @Test
    public void errorTestBoundary2() {
        cleanSlate();
        OutgoingBatch batch = createOutgoingBatch();
        long dataId[] = createDataEvents(batch, 5);
        errorTestCore(batch.getBatchId(), 5, dataId[dataId.length - 1]);
    }

    @Test
    public void errorErrorTest() {
        cleanSlate();
        OutgoingBatch batch = createOutgoingBatch();
        createDataEvents(batch, 5);
        errorTestCore(batch.getBatchId(), 7, -1);
    }

    protected void errorTestCore(long batchId, int errorLine, long expectedResults) {
        ackService.ack(new BatchInfo(batchId, errorLine));
        OutgoingBatch batch = outgoingBatchService.findOutgoingBatch(batchId);
        Assert.assertEquals(batch.getBatchId(), batchId);
        Assert.assertEquals(batch.getStatus(), OutgoingBatch.Status.ER);
        Assert.assertEquals(batch.getFailedDataId(), expectedResults);
    }

    protected OutgoingBatch createOutgoingBatch() {
        OutgoingBatch batch = new OutgoingBatch();
        batch.setNodeId(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        batch.setChannelId(TestConstants.TEST_CHANNEL_ID);
        batch.setStatus("SE");
        batch.setCreateTime(new Date());
        outgoingBatchService.insertOutgoingBatch(batch);
        return batch;
    }

    protected long[] createDataEvents(OutgoingBatch batch, int size) {
        TriggerHistory history = new TriggerHistory();
        history.setTriggerHistoryId(TestConstants.TEST_TRIGGER_HISTORY_ID);
        final long[] id = new long[size];
        for (int i = 0; i < size; i++) {
            Data data = new Data("table1", DataEventType.INSERT, "some data", "some data", history,
                    TestConstants.TEST_CHANNEL_ID, null, null);
            id[i] = dataService.insertData(data);
            dataService.insertDataEvent(getJdbcTemplate(), id[i], batch.getBatchId(), Constants.UNKNOWN_ROUTER_ID);
        }
        return id;
    }

}