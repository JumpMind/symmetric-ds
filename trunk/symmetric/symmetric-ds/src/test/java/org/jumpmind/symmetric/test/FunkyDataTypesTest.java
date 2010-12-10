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

package org.jumpmind.symmetric.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.test.ParameterizedSuite.ParameterMatcher;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;

/**
 * 
 */
public class FunkyDataTypesTest extends AbstractDatabaseTest {

    static final Log logger = LogFactory.getLog(FunkyDataTypesTest.class);
    static final String TABLE_NAME = "TEST_ORACLE_DATES";

    public FunkyDataTypesTest() throws Exception {
        super();
    }

    @Test
    @ParameterMatcher({"oracle","h2"})
    public void testOraclePrecisionTimestamp() {
        SimpleJdbcTemplate jdbcTemplate = getSimpleJdbcTemplate();
        try {
            jdbcTemplate.update("drop table " + TABLE_NAME + "");
        } catch (Exception ex) {
            logger.info("The table did not exist.");
        }
        jdbcTemplate.update("create table " + TABLE_NAME + " (id char(5) not null, ts timestamp(6), ts2 timestamp(9))");
        TriggerRouter trigger = new TriggerRouter();
        trigger.getTrigger().setChannelId(TestConstants.TEST_CHANNEL_ID_OTHER);
        trigger.getRouter().getNodeGroupLink().setSourceNodeGroupId(TestConstants.TEST_CONTINUOUS_NODE_GROUP);
        trigger.getRouter().getNodeGroupLink().setTargetNodeGroupId(TestConstants.TEST_CLIENT_NODE_GROUP);
        trigger.getTrigger().setSourceTableName(TABLE_NAME);
        trigger.getTrigger().setSyncOnInsert(true);
        trigger.getTrigger().setSyncOnUpdate(true);
        trigger.getTrigger().setSyncOnDelete(true);
        getTriggerRouterService().saveTriggerRouter(trigger);        
        getSymmetricEngine().syncTriggers();
        final String VERIFICATION_SQL = "select count(*) from sym_data where table_name=? and data_id=(select max(data_id) from sym_data)";
        Assert.assertEquals("There should not be any data captured at this point.", 0, jdbcTemplate.queryForInt(
                VERIFICATION_SQL, TABLE_NAME));
        jdbcTemplate.update("insert into " + TABLE_NAME
                + " values('00000',timestamp'2008-01-01 00:00:00.000',timestamp'2008-01-01 00:00:00.000')");
        Assert.assertEquals("The data event from the other database's other_table was not captured.", 1, jdbcTemplate
                .queryForInt(VERIFICATION_SQL, TABLE_NAME));
    }
}