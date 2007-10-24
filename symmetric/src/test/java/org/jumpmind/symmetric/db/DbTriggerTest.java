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

package org.jumpmind.symmetric.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.jumpmind.symmetric.SymmetricEngine;
import org.jumpmind.symmetric.SymmetricEngineTestFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class DbTriggerTest {

    private static final String TEST_TRIGGERS_TABLE = "test_triggers_table";

    final static String INSERT1 = "insert into "
            + TEST_TRIGGERS_TABLE
            + " (string_One_Value,string_Two_Value,long_String_Value,time_Value,date_Value,boolean_Value,bigInt_Value,decimal_Value) "
            + "values(?,?,?,?,?,?,?,?)"; //'\\\\','\"','\"1\"',null,null,1,1,1)";
    
    final static Object[] INSERT1_VALUES = new Object[] {"\\\\","\"","\"1\"",null,null,1,1,1};

    final static String INSERT2 = "insert into "
            + TEST_TRIGGERS_TABLE
            + " (string_One_Value,string_Two_Value,long_String_Value,time_Value,date_Value,boolean_Value,bigInt_Value,decimal_Value) "
            + "values('here','here',1,null,null,1,1,1)";
    
    final static String EXPECTED_INSERT1_CSV = "1,\"\\\\\\\\\",\"\\\"\",\"\\\"1\\\"\",,,1,1,1";

    final static String EXPECTED_INSERT2_CSV = "3,\"here\",\"here\",\"1\",,,1,1";

    final static String TEST_TRIGGER_WHERE_CLAUSE = "where source_table_name='"
            + TEST_TRIGGERS_TABLE + "' and source_node_group_id='"
            + TestConstants.TEST_ROOT_NODE_GROUP
            + "' and target_node_group_id='"
            + TestConstants.TEST_ROOT_NODE_GROUP + "' and channel_id='"
            + TestConstants.TEST_CHANNEL_ID + "'";

    @BeforeTest(groups = "integration")
    public void init() {
        SymmetricEngineTestFactory.resetSchemasAndEngines();
    }

    @Test(groups = "continuous")
    public void testBootstrapSchemaSync() throws Exception {
        SymmetricEngine[] engines2test = SymmetricEngineTestFactory
                .getUnitTestableEngines();
        for (SymmetricEngine engine : engines2test) {
            testBootstrapSchemaSync(engine);
        }
    }

    private void testBootstrapSchemaSync(SymmetricEngine engine)
            throws Exception {
        IBootstrapService service = (IBootstrapService) engine
                .getApplicationContext().getBean("bootstrapService");

        // baseline
        service.syncTriggers();

        // get the current number of hist rows
        int count = getTriggerHistTableRowCount(engine);

        Thread.sleep(1000);

        // force the triggers to rebuild
        count = count
                + getJdbcTemplate(engine)
                        .update(
                                "update "
                                        + TestConstants.TEST_PREFIX
                                        + "trigger set last_updated_time=current_timestamp where inactive_time is null and source_node_group_id='"
                                        + TestConstants.TEST_ROOT_NODE_GROUP
                                        + "' and (sync_on_update = '1' or sync_on_insert = '1' or sync_on_delete = '1')");

        service.syncTriggers();

        // check to see that we recorded the rebuilds
        Assert.assertEquals(getTriggerHistTableRowCount(engine), count,
                "Wrong trigger_hist row count. engine="
                        + getDbDialect(engine).getPlatform().getName());
    }

    private JdbcTemplate getJdbcTemplate(SymmetricEngine engine) {
        return (JdbcTemplate) engine.getApplicationContext().getBean(
                Constants.JDBC);
    }

    private IDbDialect getDbDialect(SymmetricEngine engine) {
        return (IDbDialect) engine.getApplicationContext().getBean(
                Constants.DB_DIALECT);
    }

    private int getTriggerHistTableRowCount(SymmetricEngine engine) {
        return getJdbcTemplate(engine).queryForInt(
                "select count(*) from " + TestConstants.TEST_PREFIX
                        + "trigger_hist");
    }

    @Test(groups = "continuous", dependsOnMethods = "testBootstrapSchemaSync")
    public void validateTestTableTriggers() throws Exception {
        SymmetricEngine[] engines2test = SymmetricEngineTestFactory
                .getUnitTestableEngines();
        for (SymmetricEngine engine : engines2test) {
            validateTestTableTriggers(engine);
        }
    }

    private void validateTestTableTriggers(SymmetricEngine engine)
            throws Exception {
        JdbcTemplate jdbcTemplate = getJdbcTemplate(engine);

        int count = jdbcTemplate.update(INSERT1, INSERT1_VALUES);

        assert count == 1;
        String csvString = getNextDataRow(engine);
        boolean match = false;
        match = csvString.equals(EXPECTED_INSERT1_CSV);
        assert match : "Received " + csvString + ", Expected "
                + EXPECTED_INSERT1_CSV;
    }

    @Test(groups = "continuous", dependsOnMethods = "validateTestTableTriggers")
    public void testInitialLoadSql() throws Exception {
        SymmetricEngine[] engines2test = SymmetricEngineTestFactory
                .getUnitTestableEngines();
        for (SymmetricEngine engine : engines2test) {
            testInitialLoadSql(engine);
        }
    }

    private void testInitialLoadSql(SymmetricEngine engine) throws Exception {
        IConfigurationService service = (IConfigurationService) engine
                .getApplicationContext().getBean("configurationService");
        service.getTriggerFor(TEST_TRIGGERS_TABLE,
                TestConstants.TEST_ROOT_NODE_GROUP);
        String sql = getDbDialect(engine).createInitalLoadSqlFor(
                new Node("1", null, "1.0"),
                service.getTriggerFor(TEST_TRIGGERS_TABLE,
                        TestConstants.TEST_ROOT_NODE_GROUP));
        String csvString = (String) getJdbcTemplate(engine).queryForObject(sql,
                String.class);
        boolean match = false;
        match = csvString.equals(EXPECTED_INSERT1_CSV);
        assert match : "Received " + csvString + ", Expected "
                + EXPECTED_INSERT1_CSV;
    }

    @Test(groups = "continuous", dependsOnMethods = "testInitialLoadSql")
    public void validateTransactionFunctionailty() throws Exception {
        SymmetricEngine[] engines2test = SymmetricEngineTestFactory
                .getUnitTestableEngines();
        for (SymmetricEngine engine : engines2test) {
            validateTransactionFunctionailty(engine);
        }
    }

    void validateTransactionFunctionailty(SymmetricEngine engine)
            throws Exception {
        JdbcTemplate jdbcTemplate = getJdbcTemplate(engine);
        jdbcTemplate.execute(new ConnectionCallback() {
            public Object doInConnection(Connection c) throws SQLException,
                    DataAccessException {
                boolean origValue = c.getAutoCommit();
                c.setAutoCommit(false);
                Statement stmt = c.createStatement();
                stmt.executeUpdate("update " + TEST_TRIGGERS_TABLE
                        + " set time_value=current_timestamp");
                stmt.executeUpdate(INSERT2);
                c.commit();
                c.setAutoCommit(origValue);
                ResultSet rs = stmt
                        .executeQuery("select transaction_id from "
                                + TestConstants.TEST_PREFIX
                                + "data where transaction_id is not null group by transaction_id having count(*)>1");
                rs.next();
                String batchId = rs.getString(1);
                assert (batchId != null);
                stmt.close();
                return null;
            }
        });
    }

    @Test(groups = "continuous", dependsOnMethods = "validateTransactionFunctionailty")
    public void testExcludedColumnsFunctionality() throws Exception {
        SymmetricEngine[] engines2test = SymmetricEngineTestFactory
                .getUnitTestableEngines();
        for (SymmetricEngine engine : engines2test) {
            testExcludedColumnsFunctionality(engine);
        }
    }

    private void testExcludedColumnsFunctionality(SymmetricEngine engine)
            throws Exception {
        IBootstrapService service = (IBootstrapService) engine
                .getApplicationContext().getBean(Constants.BOOTSTRAP_SERVICE);
        // need to wait for a second to make sure enough time has passed so the update of the config
        // table will have a greater timestamp than the audit table.
        Thread.sleep(1000);
        JdbcTemplate jdbcTemplate = getJdbcTemplate(engine);
        Assert
                .assertEquals(
                        1,
                        jdbcTemplate
                                .update("update "
                                        + TestConstants.TEST_PREFIX
                                        + "trigger set excluded_column_names='BOOLEAN_VALUE', last_updated_time=current_timestamp "
                                        + TEST_TRIGGER_WHERE_CLAUSE));

        service.syncTriggers();

        IConfigurationService configService = (IConfigurationService) engine
                .getApplicationContext().getBean(Constants.CONFIG_SERVICE);
        Trigger trigger = configService.getTriggerFor(TEST_TRIGGERS_TABLE,
                TestConstants.TEST_ROOT_NODE_GROUP);
        Assert.assertEquals(jdbcTemplate.queryForInt("select count(*) from "
                + TestConstants.TEST_PREFIX + "trigger_hist where trigger_id="
                + trigger.getTriggerId() + " and inactive_time is null"), 1,
                "We expected only one active record in the trigger_hist table for "
                        + TEST_TRIGGERS_TABLE);

        Assert.assertEquals(1, jdbcTemplate.update(INSERT2));

        String csvString = getNextDataRow(engine);
        Assert.assertEquals(csvString, EXPECTED_INSERT2_CSV, "Received "
                + csvString + ", Expected " + EXPECTED_INSERT2_CSV);
    }

    @Test(groups = "continuous", dependsOnMethods = "testExcludedColumnsFunctionality")
    public void testDisableTriggers() throws Exception {
        SymmetricEngine[] engines2test = SymmetricEngineTestFactory
                .getUnitTestableEngines();
        for (SymmetricEngine engine : engines2test) {
            testDisableTriggers(engine);
        }
    }

    private void testDisableTriggers(SymmetricEngine engine) throws Exception {
        JdbcTemplate jdbcTemplate = getJdbcTemplate(engine);
        getDbDialect(engine).disableSyncTriggers();
        int count = jdbcTemplate.update(INSERT1, INSERT1_VALUES);
        getDbDialect(engine).enableSyncTriggers();
        assert count == 1;
        String csvString = getNextDataRow(engine);
        boolean match = false;
        match = csvString.equals(EXPECTED_INSERT2_CSV);
        assert match : "Received " + csvString + ", Expected "
                + EXPECTED_INSERT2_CSV;
    }

    @Test(groups = "continuous", dependsOnMethods = "testDisableTriggers")
    public void testTargetTableNameFunctionality() throws Exception {
        SymmetricEngine[] engines2test = SymmetricEngineTestFactory
                .getUnitTestableEngines();
        for (SymmetricEngine engine : engines2test) {
            testTargetTableNameFunctionality(engine);
        }
    }

    private void testTargetTableNameFunctionality(SymmetricEngine engine)
            throws Exception {
        
        final String TARGET_TABLE_NAME = "SOME_OTHER_TABLE_NAME";
        IBootstrapService service = (IBootstrapService) engine
                .getApplicationContext().getBean(Constants.BOOTSTRAP_SERVICE);
        // need to wait for a second to make sure enough time has passed so the update of the trigger
        // table will have a greater timestamp than the audit table.
        Thread.sleep(1000);
        JdbcTemplate jdbcTemplate = getJdbcTemplate(engine);
        Assert
                .assertEquals(
                        1,
                        jdbcTemplate
                                .update("update "
                                        + TestConstants.TEST_PREFIX
                                        + "trigger set target_table_name='"+TARGET_TABLE_NAME+"', last_updated_time=current_timestamp "
                                        + TEST_TRIGGER_WHERE_CLAUSE));

        service.syncTriggers();

        IConfigurationService configService = (IConfigurationService) engine
                .getApplicationContext().getBean(Constants.CONFIG_SERVICE);
        Trigger trigger = configService.getTriggerFor(TEST_TRIGGERS_TABLE,
                TestConstants.TEST_ROOT_NODE_GROUP);
        Assert.assertEquals(jdbcTemplate.queryForInt("select count(*) from "
                + TestConstants.TEST_PREFIX + "trigger_hist where trigger_id="
                + trigger.getTriggerId() + " and inactive_time is null"), 1,
                "We expected only one active record in the trigger_hist table for "
                        + TEST_TRIGGERS_TABLE);

        Assert.assertEquals(1, jdbcTemplate.update(INSERT2));

        String tableName = getNextDataRowTableName(engine);
        Assert.assertEquals(tableName, TARGET_TABLE_NAME, "Received "
                + tableName + ", Expected " + TARGET_TABLE_NAME);
    }

    private String getNextDataRow(SymmetricEngine engine) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate(engine);
        return (String) jdbcTemplate.queryForObject("select row_data from "
                + TestConstants.TEST_PREFIX
                + "data where data_id = (select max(data_id) from "
                + TestConstants.TEST_PREFIX + "data)", String.class);

    }
    
    private String getNextDataRowTableName(SymmetricEngine engine) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate(engine);
        return (String) jdbcTemplate.queryForObject("select table_name from "
                + TestConstants.TEST_PREFIX
                + "data where data_id = (select max(data_id) from "
                + TestConstants.TEST_PREFIX + "data)", String.class);
    }    

    @AfterClass(groups = "integration")
    public void tearDown() {
        SymmetricEngineTestFactory.resetSchemasAndEngines();
    }

}
