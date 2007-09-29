package org.jumpmind.symmetric.service.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.jumpmind.symmetric.AbstractTest;
import org.jumpmind.symmetric.service.IOutgoingBatchHistoryService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class OutgoingBatchHistoryServiceTest extends AbstractTest
{
    private IOutgoingBatchHistoryService historyService;

    @BeforeTest(groups="continuous")
    protected void setUp() {
        historyService = (IOutgoingBatchHistoryService) getBeanFactory().getBean("outgoingBatchHistoryService");
        cleanSlate("sym_outgoing_batch_hist");
    }
    
    @Test(groups="continuous")
    public void test()
    {
        historyService.created(1, 5);
        historyService.sent(1);
        historyService.ok(1);
        historyService.error(1, 550);

        int count = (Integer)
        getJdbcTemplate().execute(new ConnectionCallback()
        {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException
            {
                Statement s = conn.createStatement();
                ResultSet rs = s.executeQuery("select count(*) from sym_outgoing_batch_hist ");
                rs.next();
                return rs.getInt(1);
            }            
        });
        
        Assert.assertEquals(count, 4);
    }
}
