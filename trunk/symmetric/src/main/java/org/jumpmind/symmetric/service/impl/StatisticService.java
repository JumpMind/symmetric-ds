package org.jumpmind.symmetric.service.impl;

import java.util.Collection;
import java.util.Date;

import org.jumpmind.symmetric.service.IStatisticService;
import org.jumpmind.symmetric.statistic.Statistic;
import org.jumpmind.symmetric.util.AppUtils;

public class StatisticService extends AbstractService implements IStatisticService {

    public void save(Collection<Statistic> stats, Date captureEndTime) {
        if (stats != null) {
            for (Statistic statistic : stats) {
                jdbcTemplate.update(getSql("insertStatisticSql"), new Object[] { statistic.getNodeId(),
                        AppUtils.getServerId(), statistic.getName().name(), statistic.getCaptureStartTimeMs(),
                        captureEndTime, statistic.getTotal(), statistic.getCount() });
            }
        }
    }

}
