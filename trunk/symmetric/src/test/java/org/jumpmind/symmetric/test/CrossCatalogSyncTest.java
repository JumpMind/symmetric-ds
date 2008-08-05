package org.jumpmind.symmetric.test;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.test.ParameterizedSuite.ParameterMatcher;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class CrossCatalogSyncTest extends AbstractDatabaseTest {

    public CrossCatalogSyncTest(String db) {
        super(db);
    }

    public CrossCatalogSyncTest() throws Exception {
        super();
    }

    @Test
    @ParameterMatcher("mysql")
    public void testCrossCatalogSyncOnMySQL() {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.update("drop database if exists other ");
        jdbcTemplate.update("create database other");
        String db = (String) jdbcTemplate.queryForObject("select database()", String.class);
        jdbcTemplate.update("use other");
        jdbcTemplate.update("create table other_table (id char(5) not null, name varchar(40), primary key(id))");
        jdbcTemplate.update("use " + db);
        IConfigurationService configService = find(Constants.CONFIG_SERVICE);
        Trigger trigger = new Trigger();
        trigger.setChannelId("other");
        trigger.setSourceGroupId(TestConstants.TEST_CONTINUOUS_NODE_GROUP);
        trigger.setTargetGroupId(TestConstants.TEST_CONTINUOUS_NODE_GROUP);
        trigger.setSourceCatalogName("other");
        trigger.setSourceTableName("other_table");
        trigger.setSyncOnInsert(true);
        trigger.setSyncOnUpdate(true);
        trigger.setSyncOnDelete(true);
        configService.insert(trigger);
        getSymmetricEngine().syncTriggers();
        jdbcTemplate.update("insert into other.other_table values('00000','first row')");
        Assert.assertEquals("The data event from the other database's other_table was not captured.", jdbcTemplate
                .queryForInt("select count(*) from sym_data_event where channel_id='other'"), 1);
    }
}
