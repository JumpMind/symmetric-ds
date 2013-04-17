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

import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.test.ParameterizedSuite.ParameterMatcher;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 
 */
public class CrossCatalogSyncTest extends AbstractDatabaseTest {

    public CrossCatalogSyncTest() throws Exception {
        super();
    }

    @Ignore
    @Test
    @ParameterMatcher("mysql")
    public void testCrossCatalogSyncOnMySQL() {
        testCrossCatalogSyncOnMySQL(false, true);
    }

    @Ignore
    @Test
    @ParameterMatcher("mysql")
    public void testCrossSchemaSyncOnMySQL() {
        // there really is no such thing as a schema on mysql.  this should behave just like setting the catalog element.
        testCrossCatalogSyncOnMySQL(true, false);
    }

    @Ignore
    @Test
    @ParameterMatcher("mysql")
    public void testCrossSchemaCatalogSyncOnMySQL() {
        testCrossCatalogSyncOnMySQL(true, true);
    }

    protected void testCrossCatalogSyncOnMySQL(boolean schema, boolean catalog) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.update("drop database if exists other ");
        jdbcTemplate.update("create database other");
        String db = (String) jdbcTemplate.queryForObject("select database()", String.class);
        jdbcTemplate.update("use other");
        jdbcTemplate.update("create table other_table (id char(5) not null, name varchar(40), primary key(id))");
        jdbcTemplate.update("use " + db);
        TriggerRouter triggerRouter = new TriggerRouter();
        triggerRouter.getTrigger().setChannelId("other");
        triggerRouter.getRouter().getNodeGroupLink().setSourceNodeGroupId(TestConstants.TEST_CONTINUOUS_NODE_GROUP);
        triggerRouter.getRouter().getNodeGroupLink().setTargetNodeGroupId(TestConstants.TEST_CONTINUOUS_NODE_GROUP);
        if (catalog) {
            triggerRouter.getTrigger().setSourceCatalogName("other");
        }
        if (schema) {
            triggerRouter.getTrigger().setSourceSchemaName("other");
        }
        triggerRouter.getTrigger().setSourceTableName("other_table");
        triggerRouter.getTrigger().setSyncOnInsert(true);
        triggerRouter.getTrigger().setSyncOnUpdate(true);
        triggerRouter.getTrigger().setSyncOnDelete(true);
        getTriggerRouterService().saveTriggerRouter(triggerRouter);
        getSymmetricEngine().syncTriggers();
        jdbcTemplate.update("insert into other.other_table values('00000','first row')");
        getRouterService().routeData();
        Assert.assertEquals("The data event from the other database's other_table was not captured.", 1, jdbcTemplate
                .queryForInt("select count(*) from sym_data_event where data_id in (select data_id from sym_data where channel_id='other')"));
    }

    @Ignore
    @Test
    @ParameterMatcher("mssql")
    public void testCrossCatalogSyncOnMsSql() {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        try {
            jdbcTemplate.update("drop database other");
        } catch (Exception e) { }
        jdbcTemplate.update("create database other");
        String db = (String) jdbcTemplate.queryForObject("select db_name()", String.class);
        jdbcTemplate.update("use other");
        jdbcTemplate.update("create table other_table (id char(5) not null, name varchar(40), primary key(id))");
        jdbcTemplate.update("use " + db);
        TriggerRouter trigger = new TriggerRouter();
        trigger.getTrigger().setChannelId("other");
        trigger.getRouter().getNodeGroupLink().setSourceNodeGroupId(TestConstants.TEST_CONTINUOUS_NODE_GROUP);
        trigger.getRouter().getNodeGroupLink().setTargetNodeGroupId(TestConstants.TEST_CONTINUOUS_NODE_GROUP);
        trigger.getTrigger().setSourceCatalogName("other");
        trigger.getTrigger().setSourceSchemaName("dbo");
        trigger.getTrigger().setSourceTableName("other_table");
        trigger.getTrigger().setSyncOnInsert(true);
        trigger.getTrigger().setSyncOnUpdate(true);
        trigger.getTrigger().setSyncOnDelete(true);
        getTriggerRouterService().saveTriggerRouter(trigger);
        getSymmetricEngine().syncTriggers();
        jdbcTemplate.update("insert into other.dbo.other_table values('00000','first row')");
        Assert.assertEquals("The data event from the other database's other_table was not captured.", 1, jdbcTemplate
                .queryForInt("select count(*) from sym_data_event where channel_id='other'"));
    }

    @Ignore
    @Test
    @ParameterMatcher("mssql")
    public void testCrossSchemaSyncOnMsSql() {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        try {            
            jdbcTemplate.update("drop table other.other_table2");
        } catch (Exception e) { }
        try {            
            jdbcTemplate.update("drop schema other");
        } catch (Exception e) { }
        jdbcTemplate.update("create schema other");
        jdbcTemplate.update("create table other.other_table2 (id char(5) not null, name varchar(40), primary key(id))");
        TriggerRouter trigger = new TriggerRouter();
        trigger.getTrigger().setChannelId("other");
        trigger.getRouter().getNodeGroupLink().setSourceNodeGroupId(TestConstants.TEST_CONTINUOUS_NODE_GROUP);
        trigger.getRouter().getNodeGroupLink().setTargetNodeGroupId(TestConstants.TEST_CONTINUOUS_NODE_GROUP);
        trigger.getTrigger().setSourceSchemaName("other");
        trigger.getTrigger().setSourceTableName("other_table2");
        trigger.getTrigger().setSyncOnInsert(true);
        trigger.getTrigger().setSyncOnUpdate(true);
        trigger.getTrigger().setSyncOnDelete(true);
        getTriggerRouterService().saveTriggerRouter(trigger);
        getSymmetricEngine().syncTriggers();
        jdbcTemplate.update("insert into other.other_table2 values('00000','first row')");
        Assert.assertEquals("The data event from the other database's other_table was not captured.", 1, jdbcTemplate
                .queryForInt("select count(*) from sym_data where table_name='other_table2'"));
    }

}