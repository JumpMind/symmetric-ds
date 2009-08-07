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
import org.jumpmind.symmetric.db.db2.Db2DbDialect;
import org.jumpmind.symmetric.db.oracle.OracleDbDialect;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class DbTriggerTest extends AbstractDatabaseTest {

    static final Log logger = LogFactory.getLog(DbTriggerTest.class);

    public static final String TEST_TRIGGERS_TABLE = "test_triggers_table";

    public final static String CREATE_ORACLE_BINARY_TYPE = "create table test_oracle_binary_types (id varchar(4), num_one binary_float, num_two binary_double)";
    public final static String INSERT_ORACLE_BINARY_TYPE_TRIGGER = "insert into "
            + TestConstants.TEST_PREFIX
            + "trigger (source_table_name,source_node_group_id,target_node_group_id,channel_id,sync_on_update,sync_on_insert,sync_on_delete,initial_load_order,last_updated_by,last_updated_time,create_time) values('test_oracle_binary_types','test-root-group','test-root-group','testchannel', 1, 1, 1, 1, 'chenson', current_timestamp,current_timestamp)";
    public final static String INSERT_ORACLE_BINARY_TYPE_1 = "insert into test_oracle_binary_types values('1', 2.04299998, 5.2212)";
    public final static String EXPECTED_INSERT_ORALCE_BINARY_TYPE_1 = "\"1\",\"2.04299998\",\"5.2212\"";

    public final static String INSERT = "insert into "
            + TEST_TRIGGERS_TABLE
            + " (string_One_Value,string_Two_Value,long_String_Value,time_Value,date_Value,boolean_Value,bigInt_Value,decimal_Value) "
            + "values(?,?,?,?,?,?,?,?)"; // '\\\\','\"','\"1\"',null,null,1,1,1)";

    public final static int[] INSERT_TYPES = new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP,
            Types.DATE, Types.BOOLEAN, Types.INTEGER, Types.DECIMAL };

    public final static Object[] INSERT1_VALUES = new Object[] { "\\\\", "\"", "\"1\"", null, null, Boolean.TRUE, 1, 1 };

    public final static Object[] INSERT2_VALUES = new Object[] { "here", "here", "1", null, null, Boolean.TRUE, 1, 1 };

    public final static Object[] INSERT3_VALUES = new Object[] { "inactive", "inactive", "0", null, null, Boolean.TRUE,
            1, 1 };

    public final static String EXPECTED_INSERT1_CSV_ENDSWITH = "\"\\\\\\\\\",\"\\\"\",\"\\\"1\\\"\",,,\"1\",\"1\",\"1\"";

    public final static String EXPECTED_INSERT2_CSV_ENDSWITH = "\"here\",\"here\",\"1\",,,\"1\",\"1\"";

    public final static String UNEXPECTED_INSERT3_CSV_ENDSWITH = "\"inactive\",\"inactive\",\"0\",,,\"1\",\"1\"";

    public final static String TEST_TRIGGER_WHERE_CLAUSE = "where source_table_name='" + TEST_TRIGGERS_TABLE
            + "' and source_node_group_id='" + TestConstants.TEST_ROOT_NODE_GROUP + "' and target_node_group_id='"
            + TestConstants.TEST_ROOT_NODE_GROUP + "' and channel_id='" + TestConstants.TEST_CHANNEL_ID + "'";

    public static final String insertSyncIncomingBatchSql = "insert into test_sync_incoming_batch (id, data) values (?, ?)";

    public DbTriggerTest() throws Exception {
        super();
    }

    public DbTriggerTest(String dbName) {
        super(dbName);
    }

    @Test
    public void testBootstrapSchemaSync() throws Exception {
        IConfigurationService service = (IConfigurationService) getSymmetricEngine().getApplicationContext().getBean(
                Constants.CONFIG_SERVICE);

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
        return getJdbcTemplate().queryForInt(
                "select count(*) from " + TestConstants.TEST_PREFIX + "trigger_hist where trigger_id < 100");
    }

    @Test
    public void validateTestTableTriggers() throws Exception {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        int count = insert(INSERT1_VALUES, jdbcTemplate, getDbDialect());
        assertTrue(count == 1);
        String csvString = getNextDataRow(getSymmetricEngine());
        // DB2 captures decimal differently
        csvString = csvString.replaceFirst("\"00001\\.\"", "\"1\"");
        boolean match = csvString.endsWith(EXPECTED_INSERT1_CSV_ENDSWITH);
        assertTrue(match, "The full string we pulled from the database was " + csvString
                + " however, we expected the string to end with " + EXPECTED_INSERT1_CSV_ENDSWITH);
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
        // DB2 captures decimal differently
        csvString = csvString.replaceFirst("\"00001\\.\"", "\"1\"");
        assertTrue(csvString.endsWith(EXPECTED_INSERT1_CSV_ENDSWITH), "Received " + csvString
                + ", Expected the string to end with " + EXPECTED_INSERT1_CSV_ENDSWITH);
    }

    @Test
    public void testExcludedColumnsFunctionality() throws Exception {
        IConfigurationService configService = (IConfigurationService) getSymmetricEngine().getApplicationContext()
        .getBean(Constants.CONFIG_SERVICE);
        
        // need to wait for a second to make sure enough time has passed so the
        // update of the config table will have a greater timestamp than the audit table.
        Thread.sleep(1000);
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        assertEquals(1, jdbcTemplate.update("update " + TestConstants.TEST_PREFIX
                + "trigger set excluded_column_names='BOOLEAN_VALUE', last_updated_time=current_timestamp "
                + TEST_TRIGGER_WHERE_CLAUSE));

        configService.syncTriggers();


        Trigger trigger = configService.getTriggerFor(TEST_TRIGGERS_TABLE, TestConstants.TEST_ROOT_NODE_GROUP);
        assertEquals(jdbcTemplate.queryForInt("select count(*) from " + TestConstants.TEST_PREFIX
                + "trigger_hist where trigger_id=" + trigger.getTriggerId() + " and inactive_time is null"), 1,
                "We expected only one active record in the trigger_hist table for " + TEST_TRIGGERS_TABLE);

        assertEquals(1, insert(INSERT2_VALUES, jdbcTemplate, getDbDialect()));

        String csvString = getNextDataRow(getSymmetricEngine());
        // DB2 captures decimal differently
        csvString = csvString.replaceFirst("\"00001\\.\"", "\"1\"");
        boolean match = csvString.endsWith(EXPECTED_INSERT2_CSV_ENDSWITH);
        assertTrue(match, "Received " + csvString + ", Expected the string to end with "
                + EXPECTED_INSERT2_CSV_ENDSWITH);
    }

    @Test
    public void testDisableTriggers() throws Exception {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        getDbDialect().disableSyncTriggers();
        int count = insert(INSERT1_VALUES, jdbcTemplate, getDbDialect());
        getDbDialect().enableSyncTriggers();
        assertTrue(count == 1);
        String csvString = getNextDataRow(getSymmetricEngine());
        // DB2 captures decimal differently
        csvString = csvString.replaceFirst("\"00001\\.\"", "\"1\"");
        boolean match = csvString.endsWith(EXPECTED_INSERT2_CSV_ENDSWITH);
        assertTrue(match, "Received " + csvString + ", Expected the string to end with "
                + EXPECTED_INSERT2_CSV_ENDSWITH);
    }

    @Test
    public void inactivateTriggersTest() throws Exception {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.update("update " + TestConstants.TEST_PREFIX
                + "trigger set inactive_time=current_timestamp where source_table_name='" + TEST_TRIGGERS_TABLE + "'");
        getSymmetricEngine().syncTriggers();

        Assert.assertEquals(1, insert(INSERT3_VALUES, jdbcTemplate, getDbDialect()));
        String csvString = getNextDataRow(getSymmetricEngine());
        Assert.assertNotSame(UNEXPECTED_INSERT3_CSV_ENDSWITH, csvString,
                "Data was captured when it should not have been");

    }

    @Test
    public void testBinaryColumnTypesForOracle() {
        IDbDialect dialect = getDbDialect();
        if (dialect instanceof OracleDbDialect) {
            getJdbcTemplate().update(CREATE_ORACLE_BINARY_TYPE);
            getJdbcTemplate().update(INSERT_ORACLE_BINARY_TYPE_TRIGGER);
            IConfigurationService configService = AppUtils.find(Constants.CONFIG_SERVICE, getSymmetricEngine());
            configService.syncTriggers();
            Assert.assertEquals("Some triggers must have failed to build.", 0, configService.getFailedTriggers()
                    .size());
            getJdbcTemplate().update(INSERT_ORACLE_BINARY_TYPE_1);
            String csvString = getNextDataRow(getSymmetricEngine());
            Assert.assertEquals(EXPECTED_INSERT_ORALCE_BINARY_TYPE_1, csvString);
        }
    }

    protected static int[] filterTypes(int[] types, IDbDialect dbDialect) {
        boolean isBooleanSupported = !((dbDialect instanceof OracleDbDialect) || (dbDialect instanceof Db2DbDialect));
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

    public static int insert(Object[] values, JdbcTemplate jdbcTemplate, IDbDialect dbDialect) {
        return jdbcTemplate.update(INSERT, filterValues(values, dbDialect), filterTypes(INSERT_TYPES, dbDialect));
    }

    protected static Object[] filterValues(Object[] values, IDbDialect dbDialect) {
        boolean isBooleanSupported = !((dbDialect instanceof OracleDbDialect) || (dbDialect instanceof Db2DbDialect));
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

}
