package org.jumpmind.symmetric.service.impl;

import java.math.BigDecimal;
import java.util.List;

import javax.management.Notification;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.StatisticAlertThresholds;
import org.jumpmind.symmetric.service.IStatisticService;
import org.jumpmind.symmetric.statistic.Statistic;
import org.jumpmind.symmetric.statistic.StatisticName;
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

    public StatisticServiceTest(String dbName) {
        super(dbName);
    }

    @Before
    public void setUp() {
        statisticService = AppUtils.find(Constants.STATISTIC_SERVICE, getSymmetricEngine());
    }

    @Test
    public void testSaveAlerts() throws Exception {
        statisticService.removeStatisticAlertThresholds(TEST_STAT_NAME);

        StatisticAlertThresholds thresholds = new StatisticAlertThresholds(TEST_STAT_NAME, new BigDecimal(10),
                new Long(2), new BigDecimal(0), new Long(0), new BigDecimal(0), new BigDecimal(0));

        List<StatisticAlertThresholds> list = statisticService.getAlertThresholds();
        assertFalse(list.contains(thresholds));

        statisticService.saveStatisticAlertThresholds(thresholds);

        list = statisticService.getAlertThresholds();
        assertTrue(list.contains(thresholds));

        thresholds.setThresholdCountMin(-100l);
        assertFalse(list.contains(thresholds));

        statisticService.saveStatisticAlertThresholds(thresholds);

        list = statisticService.getAlertThresholds();
        assertTrue(list.contains(thresholds));

        statisticService.removeStatisticAlertThresholds(TEST_STAT_NAME);
    }

    @Test
    public void testSaveAlertsWithNulls() throws Exception {
        statisticService.removeStatisticAlertThresholds(TEST_STAT_NAME);

        StatisticAlertThresholds thresholds = new StatisticAlertThresholds(TEST_STAT_NAME, new BigDecimal(10),
                new Long(2), null, new Long(0), null, null);

        List<StatisticAlertThresholds> list = statisticService.getAlertThresholds();
        assertFalse(list.contains(thresholds));

        statisticService.saveStatisticAlertThresholds(thresholds);

        list = statisticService.getAlertThresholds();
        assertTrue(list.contains(thresholds));

        thresholds.setThresholdCountMin(-100l);
        assertFalse(list.contains(thresholds));

        statisticService.saveStatisticAlertThresholds(thresholds);

        list = statisticService.getAlertThresholds();
        assertTrue(list.contains(thresholds));

        statisticService.removeStatisticAlertThresholds(TEST_STAT_NAME);
    }

    @Test
    public void testNotificationCreationForTotals() throws Exception {
        Statistic stat = new Statistic(StatisticName.INCOMING_BATCH_COUNT, "010101");
        StatisticAlertThresholds thresholds = new StatisticAlertThresholds(StatisticName.INCOMING_BATCH_COUNT.name(),
                new BigDecimal(10), null, new BigDecimal(1), null, null, null);
        assertNotNull(thresholds.outsideOfBoundsNotification(stat));
        stat.add(new BigDecimal(1));
        assertNull(thresholds.outsideOfBoundsNotification(stat));
        stat.add(new BigDecimal(9));
        assertNull(thresholds.outsideOfBoundsNotification(stat));stat.add(new BigDecimal(1));
        Notification event = thresholds.outsideOfBoundsNotification(stat);
        assertNotNull(event);
        String expectedMsg = stat.getName().name() + ":total=11";
        assertEquals(event.getMessage(), expectedMsg);      
    }
}
