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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEvent;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.RowMapper;


public class AcknowledgeServiceTest extends AbstractDatabaseTest {

    protected IAcknowledgeService ackService;

    protected IOutgoingBatchService outgoingBatchService;

    protected IDataService dataService;

    public AcknowledgeServiceTest() throws Exception {
        super();
    }

    public AcknowledgeServiceTest(String dbName) {
        super(dbName);
    }

    @Before
    public void setUp() {
        ackService = (IAcknowledgeService)find(Constants.ACKNOWLEDGE_SERVICE);
        outgoingBatchService = (IOutgoingBatchService)find(Constants.OUTGOING_BATCH_SERVICE);
        dataService = (IDataService)find(Constants.DATA_SERVICE);
    }

    @Test
    public void okTest() {
        cleanSlate();
        ackService.ack(new BatchInfo(1));

        List<OutgoingBatchHistory> history = getOutgoingBatchHistory(1);
        Assert.assertEquals(history.size(), 1);
        OutgoingBatchHistory hist = history.get(0);
        Assert.assertEquals(hist.getBatchId(), 1);
        Assert.assertEquals(hist.getStatus(), OutgoingBatchHistory.Status.OK);
    }

    private void cleanSlate() {
        cleanSlate(TestConstants.TEST_PREFIX + "data_event", TestConstants.TEST_PREFIX + "data",
                TestConstants.TEST_PREFIX + "outgoing_batch_hist", TestConstants.TEST_PREFIX + "outgoing_batch");
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
        List<OutgoingBatchHistory> history = getOutgoingBatchHistory(batchId);
        Assert.assertEquals(history.size(), 1);
        OutgoingBatchHistory hist = history.get(0);
        Assert.assertEquals(hist.getBatchId(), batchId);
        Assert.assertEquals(hist.getStatus(), OutgoingBatchHistory.Status.ER);
        Assert.assertEquals(hist.getFailedDataId(), expectedResults);
    }

    @SuppressWarnings("unchecked")
    protected List<OutgoingBatchHistory> getOutgoingBatchHistory(long batchId) {
        final String sql = "select batch_id, status, data_event_count, start_time, " + "failed_data_id from "
                + TestConstants.TEST_PREFIX + "outgoing_batch_hist where batch_id = ?";
        final List<OutgoingBatchHistory> list = new ArrayList<OutgoingBatchHistory>();
        getJdbcTemplate().query(sql, new Object[] { batchId }, new RowMapper() {
            public Object[] mapRow(ResultSet rs, int row) throws SQLException {
                OutgoingBatchHistory item = new OutgoingBatchHistory();
                item.setBatchId(rs.getLong(1));
                item.setStatus(OutgoingBatchHistory.Status.valueOf(rs.getString(2)));
                item.setDataEventCount(rs.getLong(3));
                item.setStartTime(rs.getTimestamp(4));
                item.setFailedDataId(rs.getLong(5));
                list.add(item);
                return null;
            }
        });
        return list;
    }

    protected OutgoingBatch createOutgoingBatch() {
        OutgoingBatch batch = new OutgoingBatch();
        batch.setNodeId(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        batch.setChannelId(TestConstants.TEST_CHANNEL_ID);
        batch.setBatchType("EV");
        batch.setStatus("SE");
        batch.setCreateTime(new Date());
        outgoingBatchService.insertOutgoingBatch(batch);
        return batch;
    }

    protected long[] createDataEvents(OutgoingBatch batch, int size) {
        TriggerHistory audit = new TriggerHistory();
        audit.setTriggerHistoryId(TestConstants.TEST_AUDIT_ID);
        final long[] id = new long[size];
        for (int i = 0; i < size; i++) {
            Data data = new Data("table1", DataEventType.INSERT, "some data", "some data", audit);
            id[i] = dataService.insertData(data);
            DataEvent dataEvent = new DataEvent(id[i], TestConstants.TEST_CLIENT_EXTERNAL_ID,
                    TestConstants.TEST_CHANNEL_ID);
            dataEvent.setBatchId(Long.valueOf(batch.getBatchId()));
            dataEvent.setBatched(true);
            dataService.insertDataEvent(dataEvent);
        }
        return id;
    }

}
