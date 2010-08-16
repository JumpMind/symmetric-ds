package org.jumpmind.symmetric.service.impl;

import java.util.Calendar;
import java.util.Date;

import junit.framework.Assert;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.service.IStatisticService;
import org.jumpmind.symmetric.statistic.ChannelStats;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.Before;
import org.junit.Test;

public class StatisticServiceTest extends AbstractDatabaseTest {

    static final String TEST_STAT_NAME = "test";

    IStatisticService statisticService;

    public StatisticServiceTest() throws Exception {
        super();
    }

    @Before
    public void setUp() {
        statisticService = AppUtils.find(Constants.STATISTIC_SERVICE, getSymmetricEngine());
    }

    @Test
    public void testSaveChannelStats() throws Exception {
        Calendar now = Calendar.getInstance();
        Date endTime = now.getTime();
        now.add(Calendar.HOUR, -1);
        ChannelStats stats = new ChannelStats("11111", "me.com", now.getTime(), endTime,
                "testChannel");
        stats.incrementDataEventInserted(1);
        stats.incrementDataBytesExtracted(1000);

        int before = getJdbcTemplate().queryForInt(
                "select count(*) from sym_node_host_channel_stats");
        statisticService.save(stats);
        int after = getJdbcTemplate().queryForInt(
                "select count(*) from sym_node_host_channel_stats");
        Assert.assertEquals(before + 1, after);
    }
}
