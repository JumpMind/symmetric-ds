package org.jumpmind.symmetric.service.impl;

import java.math.BigDecimal;
import java.util.List;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.StatisticAlertThresholds;
import org.jumpmind.symmetric.service.IStatisticService;
import org.jumpmind.symmetric.util.AppUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class StatisticServiceTest extends AbstractDatabaseTest {

    static final String TEST_STAT_NAME = "test";

    IStatisticService statisticService;

    @BeforeTest(groups = "continuous")
    protected void setUp() {
        statisticService = AppUtils.find(Constants.STATISTIC_SERVICE, getSymmetricEngine());
    }

    @Test(groups = "continuous")
    public void testSaveAlerts() throws Exception {
        statisticService.removeStatisticAlertThresholds(TEST_STAT_NAME);

        StatisticAlertThresholds thresholds = new StatisticAlertThresholds(TEST_STAT_NAME, new BigDecimal(10),
                new Long(2), new BigDecimal(0), new Long(0));

        List<StatisticAlertThresholds> list = statisticService.getAlertThresholds();
        Assert.assertFalse(list.contains(thresholds));

        statisticService.saveStatisticAlertThresholds(thresholds);

        list = statisticService.getAlertThresholds();
        Assert.assertTrue(list.contains(thresholds));

        thresholds.setThreshholdCountMin(-100l);
        Assert.assertFalse(list.contains(thresholds));

        statisticService.saveStatisticAlertThresholds(thresholds);

        list = statisticService.getAlertThresholds();
        Assert.assertTrue(list.contains(thresholds));

        statisticService.removeStatisticAlertThresholds(TEST_STAT_NAME);
    }
}
