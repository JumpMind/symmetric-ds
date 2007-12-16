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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.service.IOutgoingBatchHistoryService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class OutgoingBatchHistoryServiceTest extends AbstractDatabaseTest
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
