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

import java.sql.Types;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.SymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.oracle.OracleDbDialect;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.jumpmind.symmetric.test.ParameterizedSuite.ParameterExcluder;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

public class DbTriggerTest extends AbstractDatabaseTest {

    static final Log logger = LogFactory.getLog(DbTriggerTest.class);

    private static final String TEST_TRIGGERS_TABLE = "test_triggers_table";

    final static String INSERT = "insert into "
            + TEST_TRIGGERS_TABLE
            + " (string_One_Value,string_Two_Value,long_String_Value,time_Value,date_Value,boolean_Value,bigInt_Value,decimal_Value) "
            + "values(?,?,?,?,?,?,?,?)"; // '\\\\','\"','\"1\"',null,null,1,1,1)";

    final static int[] INSERT_TYPES = new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP,
            Types.DATE, Types.BOOLEAN, Types.INTEGER, Types.DECIMAL };

    final static Object[] INSERT1_VALUES = new Object[] { "\\\\", "\"", "\"1\"", null, null, Boolean.TRUE, 1, 1 };

    final static Object[] INSERT2_VALUES = new Object[] { "here", "here", "1", null, null, Boolean.TRUE, 1, 1 };
    
    final static Object[] INSERT3_VALUES = new Object[] { "inactive", "inactive", "0", null, null, Boolean.TRUE, 1, 1 };

    final static String EXPECTED_INSERT1_CSV_ENDSWITH = "\"\\\\\\\\\",\"\\\"\",\"\\\"1\\\"\",,,\"1\",\"1\",\"1\"";

    final static String EXPECTED_INSERT2_CSV_ENDSWITH = "\"here\",\"here\",\"1\",,,\"1\",\"1\"";
    
    final static String UNEXPECTED_INSERT3_CSV_ENDSWITH = "\"inactive\",\"inactive\",\"0\",,,\"1\",\"1\"";

    final static String TEST_TRIGGER_WHERE_CLAUSE = "where source_table_name='" + TEST_TRIGGERS_TABLE
            + "' and source_node_group_id='" + TestConstants.TEST_ROOT_NODE_GROUP + "' and target_node_group_id='"
            + TestConstants.TEST_ROOT_NODE_GROUP + "' and channel_id='" + TestConstants.TEST_CHANNEL_ID + "'";

    public DbTriggerTest() throws Exception {
        super();
    }

    public DbTriggerTest(String dbName) {
        super(dbName);
    }

    @Test
    public void testBootstrapSchemaSync() throws Exception {
        IBootstrapService service = (IBootstrapService) getSymmetricEngine().getApplicationContext().getBean(
                "bootstrapService");

        // baseline
        service.syncTriggers();

        // get the current number of hist rows
        int count = getTriggerHistTableRowCount(getSymmetricEngine());

        Thread.sleep(1000);

        // force the triggers to rebuild
        count = count
                + getJdbcTemplate()
                        .update(
                                "update "
                                        + TestConstants.TEST_PREFIX
                                        + "trigger set last_updated_time=current_timestamp where inactive_time is null and source_node_group_id='"
                                        + TestConstants.TEST_ROOT_NODE_GROUP
                                        + "' and (sync_on_update = 1 or sync_on_insert = 1 or sync_on_delete = 1)");

        service.syncTriggers();

        // check to see that we recorded the rebuilds
        assertEquals(getTriggerHistTableRowCount(getSymmetricEngine()), count, "Wrong trigger_hist row count. engine="
                + getDbDialect().getPlatform().getName());
    }

    private int getTriggerHistTableRowCount(SymmetricEngine engine) {
        return getJdbcTemplate()
                .queryForInt("select count(*) from " + TestConstants.TEST_PREFIX + "trigger_hist");
    }

    @Test
    public void validateTestTableTriggers() throws Exception {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();

        int count = jdbcTemplate.update(INSERT, filterValues(INSERT1_VALUES), filterTypes(INSERT_TYPES));

        assert count == 1;
        String csvString = getNextDataRow(getSymmetricEngine());
        boolean match = false;
        match = csvString.endsWith(EXPECTED_INSERT1_CSV_ENDSWITH);
        assert match : "Received " + csvString + ", Expected the string to end with " + EXPECTED_INSERT1_CSV_ENDSWITH;
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testInitialLoadSql() throws Exception {
        IConfigurationService service = (IConfigurationService) getSymmetricEngine().getApplicationContext().getBean(
                "configurationService");
        service.getTriggerFor(TEST_TRIGGERS_TABLE, TestConstants.TEST_ROOT_NODE_GROUP);
        String sql = getDbDialect().createInitalLoadSqlFor(new Node("1", null, "1.0"),
                service.getTriggerFor(TEST_TRIGGERS_TABLE, TestConstants.TEST_ROOT_NODE_GROUP));
        List<String> csvStrings = getJdbcTemplate().queryForList(sql, String.class);
        assertTrue(csvStrings.size() > 0);
        String csvString = csvStrings.get(0);
        assertTrue(csvString.endsWith(EXPECTED_INSERT1_CSV_ENDSWITH), "Received " + csvString
                + ", Expected the string to end with " + EXPECTED_INSERT1_CSV_ENDSWITH);
    }

    @Test
    @ParameterExcluder("postgres")
    @SuppressWarnings("unchecked")
    public void validateTransactionFunctionailty() throws Exception {
        final JdbcTemplate jdbcTemplate = getJdbcTemplate();
        TransactionTemplate transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(
                getDataSource()));
        transactionTemplate.execute(new TransactionCallback() {
            public Object doInTransaction(TransactionStatus status) {
                jdbcTemplate.update("update " + TEST_TRIGGERS_TABLE + " set time_value=current_timestamp");
                jdbcTemplate.update(INSERT, filterValues(INSERT2_VALUES), filterTypes(INSERT_TYPES));
                return null;
            }
        });
        String sql = "select transaction_id from " + TestConstants.TEST_PREFIX
                + "data_event where transaction_id is not null group by transaction_id having count(*)>1";
        List<String> batchIdList = (List<String>) jdbcTemplate.queryForList(sql, String.class);

        IDbDialect dbDialect = getDbDialect();
        if (dbDialect.supportsTransactionId()) {
            assertTrue(batchIdList != null && batchIdList.size() == 1);
            assertNotNull(batchIdList.get(0));
        }
    }

    @Test
    public void testExcludedColumnsFunctionality() throws Exception {
        IBootstrapService service = (IBootstrapService) getSymmetricEngine().getApplicationContext().getBean(
                Constants.BOOTSTRAP_SERVICE);
        // need to wait for a second to make sure enough time has passed so the
        // update of the config
        // table will have a greater timestamp than the audit table.
        Thread.sleep(1000);
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        assertEquals(1, jdbcTemplate.update("update " + TestConstants.TEST_PREFIX
                + "trigger set excluded_column_names='BOOLEAN_VALUE', last_updated_time=current_timestamp "
                + TEST_TRIGGER_WHERE_CLAUSE));

        service.syncTriggers();

        IConfigurationService configService = (IConfigurationService) getSymmetricEngine().getApplicationContext()
                .getBean(Constants.CONFIG_SERVICE);
        Trigger trigger = configService.getTriggerFor(TEST_TRIGGERS_TABLE, TestConstants.TEST_ROOT_NODE_GROUP);
        assertEquals(jdbcTemplate.queryForInt("select count(*) from " + TestConstants.TEST_PREFIX
                + "trigger_hist where trigger_id=" + trigger.getTriggerId() + " and inactive_time is null"), 1,
                "We expected only one active record in the trigger_hist table for " + TEST_TRIGGERS_TABLE);

        assertEquals(1, jdbcTemplate.update(INSERT, filterValues(INSERT2_VALUES), filterTypes(INSERT_TYPES)));

        String csvString = getNextDataRow(getSymmetricEngine());
        boolean match = csvString.endsWith(EXPECTED_INSERT2_CSV_ENDSWITH);
        assert match : "Received " + csvString + ", Expected the string to end with " + EXPECTED_INSERT2_CSV_ENDSWITH;
    }

    @Test
    public void testDisableTriggers() throws Exception {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        getDbDialect().disableSyncTriggers();
        int count = jdbcTemplate.update(INSERT, filterValues(INSERT1_VALUES), filterTypes(INSERT_TYPES));
        getDbDialect().enableSyncTriggers();
        assert count == 1;
        String csvString = getNextDataRow(getSymmetricEngine());
        boolean match = csvString.endsWith(EXPECTED_INSERT2_CSV_ENDSWITH);
        assert match : "Received " + csvString + ", Expected the string to end with " + EXPECTED_INSERT2_CSV_ENDSWITH;
    }

    @Test
    public void testTargetTableNameFunctionality() throws Exception {

        final String TARGET_TABLE_NAME = "SOME_OTHER_TABLE_NAME";
        IBootstrapService service = (IBootstrapService) getSymmetricEngine().getApplicationContext().getBean(
                Constants.BOOTSTRAP_SERVICE);
        // need to wait for a second to make sure enough time has passed so the
        // update of the trigger
        // table will have a greater timestamp than the audit table.
        Thread.sleep(1000);
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        assertEquals(1, jdbcTemplate.update("update " + TestConstants.TEST_PREFIX + "trigger set target_table_name='"
                + TARGET_TABLE_NAME + "', last_updated_time=current_timestamp " + TEST_TRIGGER_WHERE_CLAUSE));

        service.syncTriggers();

        IConfigurationService configService = (IConfigurationService) getSymmetricEngine().getApplicationContext()
                .getBean(Constants.CONFIG_SERVICE);
        Trigger trigger = configService.getTriggerFor(TEST_TRIGGERS_TABLE, TestConstants.TEST_ROOT_NODE_GROUP);
        assertEquals(jdbcTemplate.queryForInt("select count(*) from " + TestConstants.TEST_PREFIX
                + "trigger_hist where trigger_id=" + trigger.getTriggerId() + " and inactive_time is null"), 1,
                "We expected only one active record in the trigger_hist table for " + TEST_TRIGGERS_TABLE);

        assertEquals(1, jdbcTemplate.update(INSERT, filterValues(INSERT2_VALUES), filterTypes(INSERT_TYPES)));

        String tableName = getNextDataRowTableName(getSymmetricEngine());
        assertEquals(tableName, TARGET_TABLE_NAME, "Received " + tableName + ", Expected " + TARGET_TABLE_NAME);
    }
    
    @Test
    public void inactivateTriggersTest() throws Exception {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.update("update " + TestConstants.TEST_PREFIX + "trigger set inactive_time=current_timestamp where source_table_name='"+TEST_TRIGGERS_TABLE+"'");
        getSymmetricEngine().syncTriggers();
        Assert.assertEquals(1, jdbcTemplate.update(INSERT, filterValues(INSERT3_VALUES), filterTypes(INSERT_TYPES)));
        String csvString = getNextDataRow(getSymmetricEngine());
        Assert.assertNotSame(UNEXPECTED_INSERT3_CSV_ENDSWITH, csvString, "Data was captured when it should not have been");
        
    }    

    private int[] filterTypes(int[] types) {
        boolean isBooleanSupported = !(getDbDialect() instanceof OracleDbDialect);
        int[] filteredTypes = new int[types.length];
        for (int i = 0; i < types.length; i++) {
            if (types[i] == Types.BOOLEAN && !isBooleanSupported) {
                filteredTypes[i] = Types.INTEGER;
            } else {
                filteredTypes[i] = types[i];
            }
        }
        return filteredTypes;
    }

    private Object[] filterValues(Object[] values) {
        boolean isBooleanSupported = !(getDbDialect() instanceof OracleDbDialect);
        Object[] filteredValues = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] instanceof Boolean && !isBooleanSupported) {
                filteredValues[i] = ((Boolean) values[i]) ? new Integer(1) : new Integer(0);
            } else {
                filteredValues[i] = values[i];
            }
        }
        return filteredValues;
    }

    private String getNextDataRow(SymmetricEngine engine) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        return (String) jdbcTemplate
                .queryForObject("select row_data from " + TestConstants.TEST_PREFIX
                        + "data where data_id = (select max(data_id) from " + TestConstants.TEST_PREFIX + "data)",
                        String.class);

    }

    private String getNextDataRowTableName(SymmetricEngine engine) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        return (String) jdbcTemplate
                .queryForObject("select table_name from " + TestConstants.TEST_PREFIX
                        + "data where data_id = (select max(data_id) from " + TestConstants.TEST_PREFIX + "data)",
                        String.class);
    }

}
