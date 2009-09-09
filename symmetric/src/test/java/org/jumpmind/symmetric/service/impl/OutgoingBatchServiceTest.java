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
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
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

        cleanSlate("sym_data_event", "sym_data",
                "sym_outgoing_batch");
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

        nodeChannel.setEnabled(true);
        getConfigurationService().saveChannel(nodeChannel);
    }

    protected void createDataEvent(String tableName, int triggerHistoryId, String channelId, DataEventType type,
            String nodeId) {
        TriggerHistory history = new TriggerHistory();
        history.setTriggerHistoryId(triggerHistoryId);
        Data data = new Data(tableName, type, "r.o.w., dat-a", "p-k d.a.t.a", history, channelId, null, null);
        dataService.insertDataEvent(data, channelId, nodeId);
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
