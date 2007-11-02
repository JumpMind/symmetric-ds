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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.jumpmind.symmetric.AbstractTest;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AcknowledgeServiceTest extends AbstractTest {
    @Test(groups = "continuous")
    public void okTest() {
        cleanSlate();
        IAcknowledgeService service = (IAcknowledgeService) getBeanFactory()
                .getBean(Constants.ACKNOWLEDGE_SERVICE);
        ArrayList<BatchInfo> list = new ArrayList<BatchInfo>();
        list.add(new BatchInfo("1"));
        service.ack(list);

        List<OutgoingBatchHistory> history = getOutgoingBatchHistory(1);
        Assert.assertEquals(history.size(), 1);
        OutgoingBatchHistory hist = history.get(0);
        Assert.assertEquals(hist.getBatchId(), 1);
        Assert.assertEquals(hist.getStatus(), OutgoingBatchHistory.Status.OK);
    }

    private void cleanSlate() {
        cleanSlate(TestConstants.TEST_PREFIX + "data_event", TestConstants.TEST_PREFIX + "data", TestConstants.TEST_PREFIX + "outgoing_batch_hist",
                TestConstants.TEST_PREFIX + "outgoing_batch");
    }

    @Test(groups = "continuous")
    public void unspecifiedErrorTest() {
        cleanSlate();
        makeFakeBatch(1, 5);
        errorTestCore(1, -1, -1);
    }

    @Test(groups = "continuous")
    public void errorTest() {
        cleanSlate();
        long[] id = makeFakeBatch(1, 5);
        errorTestCore(1, 3, id[2]);
    }

    @Test(groups = "continuous")
    public void errorTestBoundary1() {
        cleanSlate();
        long[] id = makeFakeBatch(1, 5);
        errorTestCore(1, 1, id[0]);
    }

    @Test(groups = "continuous")
    public void errorTestBoundary2() {
        cleanSlate();
        long[] id = makeFakeBatch(1, 5);
        errorTestCore(1, 5, id[id.length - 1]);
    }

    @Test(groups = "continuous")
    public void errorErrorTest() {
        cleanSlate();
        makeFakeBatch(1, 5);
        errorTestCore(1, 7, -1);
    }

    protected void errorTestCore(int batchId, int errorLine,
            long expectedResults) {
        IAcknowledgeService service = (IAcknowledgeService) getBeanFactory()
                .getBean(Constants.ACKNOWLEDGE_SERVICE);
        ArrayList<BatchInfo> list = new ArrayList<BatchInfo>();
        list.add(new BatchInfo(Integer.toString(batchId), errorLine));
        service.ack(list);

        List<OutgoingBatchHistory> history = getOutgoingBatchHistory(batchId);
        Assert.assertEquals(history.size(), 1);
        OutgoingBatchHistory hist = history.get(0);
        Assert.assertEquals(hist.getBatchId(), batchId);
        Assert.assertEquals(hist.getStatus(), OutgoingBatchHistory.Status.ER);
        Assert.assertEquals(hist.getFailedDataId(), expectedResults);
    }

    @SuppressWarnings("unchecked")
    protected List<OutgoingBatchHistory> getOutgoingBatchHistory(int batchId) {
        final String sql = "select batch_id, status, data_event_count, event_time, "
                + "failed_data_id from " + TestConstants.TEST_PREFIX + "outgoing_batch_hist where batch_id = ?";
        final List<OutgoingBatchHistory> list = new ArrayList<OutgoingBatchHistory>();
        getJdbcTemplate().query(sql, new Object[] { batchId }, new RowMapper() {
            public Object[] mapRow(ResultSet rs, int row) throws SQLException {
                OutgoingBatchHistory item = new OutgoingBatchHistory();
                item.setBatchId(rs.getInt(1));
                item.setStatus(OutgoingBatchHistory.Status.valueOf(rs
                        .getString(2)));
                item.setDataEventCount(rs.getLong(3));
                item.setEventTime(rs.getTimestamp(4));
                item.setFailedDataId(rs.getLong(5));
                list.add(item);
                return null;
            }
        });
        return list;
    }

    protected void executeSql(String file) {
        new SqlScript(getClass().getResource(file), getDataSource()).execute();
    }

    protected long[] makeFakeBatch(final int batchId, int size) {
        final long[] id = new long[size];
        this
                .getJdbcTemplate()
                .update(
                        "insert into " + TestConstants.TEST_PREFIX + "outgoing_batch (batch_id,node_id,channel_id,batch_type,status,create_time) values("
                                + batchId
                                + ",'"
                                + TestConstants.TEST_CLIENT_EXTERNAL_ID
                                + "','"
                                + TestConstants.TEST_CHANNEL_ID
                                + "','EV','SE',current_timestamp)");
        for (int i = 0; i < size; i++) {
            final long dataId = (Long) this.getJdbcTemplate().execute(
                    new ConnectionCallback() {

                        public Object doInConnection(Connection connection)
                                throws SQLException, DataAccessException {
                            Statement s = connection.createStatement();
                            s
                                    .executeUpdate("insert into " + TestConstants.TEST_PREFIX + "data (table_name, trigger_hist_id) values ('table1', "
                                            + TestConstants.TEST_AUDIT_ID
                                            + ");");
                            ResultSet rs = s.getGeneratedKeys();
                            rs.next();
                            return rs.getLong(1);
                        }

                    });
            id[i] = dataId;
            this.getJdbcTemplate().execute(new ConnectionCallback() {

                public Object doInConnection(Connection connection)
                        throws SQLException, DataAccessException {
                    PreparedStatement s = connection
                            .prepareStatement("insert into " + TestConstants.TEST_PREFIX + "data_event (data_id, node_id, batch_id) values (?, '"
                                    + TestConstants.TEST_CLIENT_EXTERNAL_ID + "', ?)");
                    s.setLong(1, dataId);
                    s.setInt(2, batchId);
                    s.executeUpdate();
                    return null;
                }

            });
        }
        return id;
    }

    protected DataSource getDataSource() {
        return (DataSource) getBeanFactory().getBean("dataSource");
    }

    protected JdbcTemplate getJdbcTemplate() {
        return new JdbcTemplate(getDataSource());
    }

}
