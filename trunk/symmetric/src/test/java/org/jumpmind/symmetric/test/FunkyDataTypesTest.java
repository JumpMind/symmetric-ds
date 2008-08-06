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
package org.jumpmind.symmetric.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.test.ParameterizedSuite.ParameterMatcher;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class FunkyDataTypesTest extends AbstractDatabaseTest {

    static final Log logger = LogFactory.getLog(FunkyDataTypesTest.class);
    static final String TABLE_NAME = "test_oracle_dates";

    public FunkyDataTypesTest(String dbName) {
        super(dbName);
    }
    
    public FunkyDataTypesTest() throws Exception {
        super();
    }

    @Test
    @ParameterMatcher("oracle")
    public void testOraclePrecisionTimestamp() {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        try {
            jdbcTemplate.update("drop table " + TABLE_NAME + "");
        } catch (Exception ex) {
            logger.info("The table did not exist.");
        }
        jdbcTemplate.update("create table " + TABLE_NAME + " (id char(5) not null, ts timestamp(6), ts2 timestamp(9))");
        IConfigurationService configService = find(Constants.CONFIG_SERVICE);
        Trigger trigger = new Trigger();
        trigger.setChannelId("other");
        trigger.setSourceGroupId(TestConstants.TEST_CONTINUOUS_NODE_GROUP);
        trigger.setTargetGroupId(TestConstants.TEST_CONTINUOUS_NODE_GROUP);
        trigger.setSourceTableName(TABLE_NAME);
        trigger.setSyncOnInsert(true);
        trigger.setSyncOnUpdate(true);
        trigger.setSyncOnDelete(true);
        configService.insert(trigger);
        getSymmetricEngine().syncTriggers();
        jdbcTemplate.update("insert into " + TABLE_NAME
                + " values('00000',timestamp'2008-01-01 00:00:00.000',timestamp'2008-01-01 00:00:00.000')");
        Assert.assertEquals("The data event from the other database's other_table was not captured.", jdbcTemplate.queryForInt("select count(*) from sym_data_event where channel_id='other'"),
                1);
    }
}
