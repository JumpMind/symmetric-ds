package org.jumpmind.symmetric;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.oracle.OracleDbDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FunkyDataTypesTest extends AbstractDatabaseTest {

    static final Log logger = LogFactory.getLog(FunkyDataTypesTest.class);
    static final String TABLE_NAME = "test_oracle_dates";

    public FunkyDataTypesTest() {
    }

    public FunkyDataTypesTest(String db) {
        super(db);
    }

    @Test(groups = "continuous")
    public void testOraclePrecisionTimestamp() {
        IDbDialect dbDialect = getDbDialect();
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        if (dbDialect instanceof OracleDbDialect) {
            try {
                jdbcTemplate.update("drop table " + TABLE_NAME + "");
            } catch (Exception ex) {
                logger.info("The table did not exist.");
            }
            jdbcTemplate.update("create table " + TABLE_NAME
                    + " (id char(5) not null, ts timestamp(6), ts2 timestamp(9))");
            IConfigurationService configService = (IConfigurationService) getBeanFactory().getBean(
                    Constants.CONFIG_SERVICE);
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
            Assert.assertEquals(jdbcTemplate
                    .queryForInt("select count(*) from sym_data_event where channel_id='other'"), 1,
                    "The data event from the other database's other_table was not captured.");
        }
    }
}
