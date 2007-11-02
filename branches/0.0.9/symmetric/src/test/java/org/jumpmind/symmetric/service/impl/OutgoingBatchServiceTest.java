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
import java.util.List;

import org.jumpmind.symmetric.AbstractTest;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.model.BatchType;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class OutgoingBatchServiceTest extends AbstractTest
{
    private IOutgoingBatchService batchService;

    @BeforeTest(groups="continuous")
    protected void setUp() {
        batchService = (IOutgoingBatchService) getBeanFactory().getBean(Constants.OUTGOING_BATCH_SERVICE);
    }
    
    @Test(groups="continuous")
    public void test()
    {
        cleanSlate(TestConstants.TEST_PREFIX + "data_event", TestConstants.TEST_PREFIX + "data", TestConstants.TEST_PREFIX + "outgoing_batch");
        // create a batch
        int dataId = createData("Foo", 1, TestConstants.TEST_CHANNEL_ID, DataEventType.INSERT);
        createEvent(dataId, TestConstants.TEST_CLIENT_EXTERNAL_ID);
        batchService.buildOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        List<OutgoingBatch> list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertTrue(list != null);
        Assert.assertEquals(list.size(), 1);
        Assert.assertTrue(list.get(0).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));
        
        // create another batch
        dataId = createData("Foo", 1, TestConstants.TEST_CHANNEL_ID, DataEventType.INSERT);
        createEvent(dataId, TestConstants.TEST_CLIENT_EXTERNAL_ID);
        dataId = createData("Foo", 1, TestConstants.TEST_CHANNEL_ID, DataEventType.INSERT);
        createEvent(dataId, TestConstants.TEST_CLIENT_EXTERNAL_ID);
        batchService.buildOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertTrue(list != null);
        Assert.assertTrue(list.size() == 2);
        Assert.assertTrue(list.get(0).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));
        Assert.assertTrue(list.get(1).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));
        
        // mark the first batch as sent (should still be eligible to be resent)
        batchService.markOutgoingBatchSent(list.get(0));
        list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertTrue(list.size() == 2);
        Assert.assertTrue(list.get(0).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));
        Assert.assertTrue(list.get(1).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));
        
        // set the second batch status to ok
        batchService.setBatchStatus(list.get(0).getBatchId(), Status.OK);
        list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertTrue(list.size() == 1);
        Assert.assertTrue(list.get(0).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));
        
        // test for initial load (batch type == IL)
        OutgoingBatch ilBatch = new OutgoingBatch();
        ilBatch.setNodeId(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        ilBatch.setChannelId(TestConstants.TEST_CHANNEL_ID);
        ilBatch.setBatchType(BatchType.INITIAL_LOAD);
        batchService.insertOutgoingBatch(ilBatch);
        list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertTrue(list.size() == 2);
        Assert.assertTrue(list.get(0).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));
        Assert.assertTrue(list.get(1).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));
        
        // now mark the IL batch as complete
        batchService.setBatchStatus(ilBatch.getBatchId(), Status.OK);
        list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertTrue(list.size() == 1);
        Assert.assertTrue(list.get(0).getChannelId().equals(TestConstants.TEST_CHANNEL_ID));
    }
    
    @Test(groups="continuous")
    public void testBatchBoundary()
    {
        cleanSlate(TestConstants.TEST_PREFIX + "data_event", TestConstants.TEST_PREFIX + "data", TestConstants.TEST_PREFIX + "outgoing_batch");
        int size = 50;
        int count = 3; // must be <= size
        Assert.assertTrue(count <= size);
        
        for (int i=0; i<size*count; i++)
        {
            createEvent(createData("Foo", TestConstants.TEST_AUDIT_ID, 
                TestConstants.TEST_CHANNEL_ID, DataEventType.INSERT), TestConstants.TEST_CLIENT_EXTERNAL_ID);
        }
        
        for (int i=0; i<count; i++)
        {
            batchService.buildOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        }
        
        List<OutgoingBatch> list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertNotNull(list);
        Assert.assertEquals(list.size(), count);
        
        for (int i=0; i<count; i++)
        {
            Assert.assertTrue(getBatchSize(list.get(i).getBatchId()) <= size +1);
        }
    }
    
    @Test(groups="continuous")
    public void testMultipleChannels()
    {
        cleanSlate(TestConstants.TEST_PREFIX + "data_event", TestConstants.TEST_PREFIX + "data", TestConstants.TEST_PREFIX + "outgoing_batch");
        createEvent(createData("Foo", TestConstants.TEST_AUDIT_ID, "testchannel", DataEventType.INSERT), TestConstants.TEST_CLIENT_EXTERNAL_ID);
        createEvent(createData("Foo", TestConstants.TEST_AUDIT_ID, "config", DataEventType.INSERT), TestConstants.TEST_CLIENT_EXTERNAL_ID);
           
        batchService.buildOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
           
        List<OutgoingBatch> list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertNotNull(list);
        Assert.assertEquals(list.size(), 2);
   }
   
    @Test(groups="continuous")
    public void testDisabledChannel()
    {
        cleanSlate(TestConstants.TEST_PREFIX + "data_event", TestConstants.TEST_PREFIX + "data", TestConstants.TEST_PREFIX + "outgoing_batch");
        int size = 50; // magic number
        int count = 3; // must be <= size
        Assert.assertTrue(count <= size);
       
        for (int i=0; i<size*count; i++)
        {
            createEvent(createData("Foo", 1, TestConstants.TEST_CHANNEL_ID, DataEventType.INSERT), TestConstants.TEST_CLIENT_EXTERNAL_ID);
        }

        List<OutgoingBatch> list = batchService.getOutgoingBatches(TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertNotNull(list);
        Assert.assertEquals(list.size(), 0);
   }

   protected int createData(final String tableName, final int auditId, final String channelId, final DataEventType type) 
    {
        return (Integer)getJdbcTemplate().execute(new ConnectionCallback() 
        {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException
            {
                PreparedStatement statement = conn.prepareStatement("insert into " + TestConstants.TEST_PREFIX + "data " +
                        "(table_name, trigger_hist_id, channel_id, row_data, pk_data, event_type) " +
                        "values (?, ?, ?, ?, ?, ?)");
                statement.setString(1, tableName);
                statement.setInt(2, auditId);
                statement.setString(3, channelId);
                statement.setString(4, "r.o.w., dat-a");
                statement.setString(5, "p-k d.a.t.a");
                statement.setString(6, type.getCode());
                statement.executeUpdate();
                ResultSet results = statement.getGeneratedKeys();
                results.next();
                return results.getInt(1);
            }
        });
    }
    
    protected void createEvent(final int dataId, final String nodeId)
    {
        getJdbcTemplate().execute(new ConnectionCallback()
        {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException
            {
                PreparedStatement s = conn.prepareStatement("insert into " + TestConstants.TEST_PREFIX + "data_event " +
                        "(data_id, node_id) values (?, ?)");
                s.setInt(1, dataId);
                s.setString(2, nodeId);
                s.executeUpdate();
                return null;
            }
            
        });
    }
    
    protected int getBatchSize(final String batchId)
    {
        return (Integer) getJdbcTemplate().execute(new ConnectionCallback()
        {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException
            {
                PreparedStatement s = conn.prepareStatement("select count(*) " +
                        "from " + TestConstants.TEST_PREFIX + "data_event where batch_id = ?");
                s.setString(1, batchId);
                ResultSet rs = s.executeQuery();
                rs.next();
                return rs.getInt(1);
            }            
        });
    }
    
    protected void executeSql(String file)
    {
        new SqlScript(getClass().getResource(file), getDataSource(), false).execute();
    }
}
